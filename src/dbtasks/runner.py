import concurrent.futures
import functools
import importlib
import logging
import platform
import threading
import time

from django.core.exceptions import ImproperlyConfigured
from django.db import connection, transaction
from django.db.models import Q
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.module_loading import import_string

from .backend import DatabaseBackend
from .defaults import DEFAULT_RUNNER_LOOP_DELAY
from .models import ScheduledTask
from .periodic import Periodic

logger = logging.getLogger(__name__)


def run_task(task: ScheduledTask) -> TaskResultStatus:
    """
    Fetches, runs, and updates a `ScheduledTask`. Runs in a worker thread.
    """
    try:
        logger.info(f"Running {task}")
        return task.run_and_update()
    finally:
        # Thread pools don't have shutdown hooks for individual worker threads, so we
        # close the connection after each run to make sure none are left open when
        # shutting down.
        #
        # Seems like this might be a welcome addition:
        # https://discuss.python.org/t/adding-finalizer-to-the-threading-library/54186
        connection.close()


class Runner:
    backend: DatabaseBackend

    def __init__(
        self,
        workers: int = 4,
        worker_id: str | None = None,
        backend: str = DEFAULT_TASK_BACKEND_ALIAS,
        loop_delay: float = DEFAULT_RUNNER_LOOP_DELAY,
        init_periodic: bool = True,
    ):
        self.workers = workers
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        # In-process tasks.
        self.tasks: dict[str, concurrent.futures.Future] = {}
        # Keep track of any seen task module, for reloading.
        self.seen_modules: set[str] = set()
        # Track the number of tasks we've executed.
        self.processed = 0
        self.worker_id = worker_id or platform.node() or type(self).__name__.lower()
        # How long to wait between scheduling polls.
        self.loop_delay = loop_delay
        self.backend = task_backends[backend]
        if not isinstance(self.backend, DatabaseBackend):
            raise ImproperlyConfigured("Backend must be a `DatabaseBackend`")
        # Signaled when the runner is ready and processing tasks.
        self.ready = threading.Event()
        # Signaled when the runner should stop.
        self.stopsign = threading.Event()
        # Signaled when the runner is finished stopping.
        self.finished = threading.Event()
        # Signaled each time the queue is empty (no READY tasks).
        self.empty = threading.Event()
        # Covers `self.tasks`, `self.seen_modules`, and `self.processed` access.
        self.lock = threading.Lock()
        # Allows callers to block on a single task being completed.
        self.waiting: dict[str, threading.Event] = {}
        self.periodic: dict[str, Periodic] = {}
        self.should_init_periodic = init_periodic
        self.should_delete_tasks = True
        for task_path, schedule in self.backend.options.get("periodic", {}).items():
            self.periodic[task_path] = (
                schedule if isinstance(schedule, Periodic) else Periodic(schedule)
            )

    def get_tasks(self, number: int) -> list[ScheduledTask]:
        """
        Returns up to `number` ready tasks, atomically changing their status to running,
        marking their start dates, and adding our `worker_id`.
        """
        if number <= 0:
            return []
        with transaction.atomic(durable=True):
            now = timezone.now()
            tasks = list(
                ScheduledTask.objects.filter(
                    Q(run_after__isnull=True) | Q(run_after__lte=now),
                    status=TaskResultStatus.READY,
                    # TODO: allow runner to specify which backend/queues to process
                    backend=self.backend.alias,
                    queue__in=self.backend.queues,
                )
                .order_by("-priority", "enqueued_at")[:number]
                .select_for_update()
            )
            for t in tasks:
                t.status = TaskResultStatus.RUNNING
                t.started_at = now
                # TODO: can't figure out how to do this in a .update call.
                t.worker_ids.append(self.worker_id)
                t.save(update_fields=["status", "started_at", "worker_ids"])
        return tasks

    def task_done(
        self,
        task: ScheduledTask,
        fut: concurrent.futures.Future,
    ):
        """
        Called when a task is finished. Removes the task from `self.tasks` and logs the
        completion. If the task was a periodic task, schedules the next run. Note that
        there are no guarantees about which thread this method is called from.
        """
        with self.lock:
            self.processed += 1
            del self.tasks[task.task_id]

        try:
            status = fut.result()
            logger.info(f"Task {task} finished with status {status}")
        except Exception as ex:
            logger.info(f"Task {task} raised {ex}")

        if task.periodic and (schedule := self.periodic.get(task.task_path)):
            after = timezone.make_aware(schedule.next())
            # Since this can run in the task's thread, we need to clean up the
            # connection afterwards since it may not be closed at the end of `run`.
            with connection.temporary_connection():
                t = ScheduledTask.objects.create(
                    task_path=task.task_path,
                    args=schedule.args,
                    kwargs=schedule.kwargs,
                    backend=self.backend.alias,
                    run_after=after,
                    periodic=True,
                )
                logger.info(f"Re-scheduled {t} for {after}")

        if self.backend.send_signals:
            task_finished.send(type(self.backend), task_result=task.result)

        # If anyone is waiting on this task, wake them up.
        if event := self.waiting.get(task.task_id):
            event.set()

    def submit_task(self, task: ScheduledTask, start: bool = True) -> TaskResult:
        """
        Submits a `ScheduledTask` for execution, marking it as RUNNING and setting its
        `started_at` timestamp if `start=True`.

        Note that `task` is passed directly to a separate thread, so callers should not
        modify it until after the task is complete.
        """
        if start:
            task.status = TaskResultStatus.RUNNING
            task.started_at = timezone.now()
            task.worker_ids.append(self.worker_id)
            task.save(update_fields=["status", "started_at", "worker_ids"])
        logger.debug(f"Submitting {task} for execution")
        if self.backend.send_signals:
            task_started.send(type(self.backend), task_result=task.result)
        f = self.executor.submit(run_task, task)
        with self.lock:
            # Keep track of task modules we've seen, so we can reload them.
            self.seen_modules.add(task.task_path.rsplit(".", 1)[0])
            self.tasks[task.task_id] = f
        f.add_done_callback(functools.partial(self.task_done, task))
        return task.result

    def schedule_tasks(self) -> float:
        """
        Fetches a number of tasks and submits them for execution. Returns how long to
        delay before the next call to `schedule_tasks`.
        """
        available = max(0, self.workers - len(self.tasks))
        if available <= 0:
            # No available worker threads, do nothing.
            return self.loop_delay

        tasks = self.get_tasks(available)
        if not tasks:
            # If we ask for tasks and get none back, AND there are no outstanding task
            # callbacks, signal that the queue is empty.
            if not self.tasks:
                self.empty.set()
            return self.loop_delay

        # We have tasks to process, clear the empty flag.
        self.empty.clear()

        for t in tasks:
            # get_tasks starts all of the returned tasks atomically, no need to here.
            self.submit_task(t, start=False)

        if len(tasks) >= available:
            # We got a full batch, try again immediately.
            return 0

        return self.loop_delay

    def delete_tasks(self):
        """
        Deletes any finished tasks scheduled for deletion before now.
        """
        deleted = ScheduledTask.objects.filter(
            delete_after__lt=timezone.now(),
            status__in=[TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED],
            # TODO: allow runner to specify which backend/queues to process
            backend=self.backend.alias,
            queue__in=self.backend.queues,
        ).delete()[0]
        if deleted:
            logger.debug(f"Removed {deleted} completed task(s)")

    def init_periodic(self):
        """
        Removes any outstanding scheduled periodic tasks, and schedules the next runs
        for each.
        """
        ScheduledTask.objects.filter(
            status=TaskResultStatus.READY,
            periodic=True,
        ).delete()
        # Schedule the next run of each periodic task. Subsequent runs will be scheduled
        # on completion.
        for task_path, schedule in self.periodic.items():
            after = timezone.make_aware(schedule.next())
            t = ScheduledTask.objects.create(
                task_path=task_path,
                args=schedule.args,
                kwargs=schedule.kwargs,
                backend=self.backend.alias,
                run_after=after,
                periodic=True,
            )
            logger.info(f"Scheduled {t} for {after}")

    def run(self):
        """
        Schedules and executes tasks until `stop()` is called.
        """
        logger.info(f"Starting task runner with {self.workers} workers")
        self.processed = 0
        self.stopsign.clear()
        if self.should_init_periodic:
            with transaction.atomic(durable=True):
                self.init_periodic()
                transaction.on_commit(self.ready.set)
        else:
            self.ready.set()
        try:
            while not self.stopsign.is_set():
                if delay := self.schedule_tasks():
                    time.sleep(delay)
                    # Only process deletes when not running through full batches (and
                    # when the flag is set - mostly for testing).
                    if self.should_delete_tasks:
                        self.delete_tasks()
        except KeyboardInterrupt:
            pass
        finally:
            self.executor.shutdown()
            connection.close()
        self.ready.clear()
        self.finished.set()

    def wait_for(self, result: TaskResult, timeout: float | None = None) -> bool:
        """
        Waits for the specified `TaskResult` to complete (or fail).
        """
        if result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return True
        logger.debug(f"Waiting for {result.id}...")
        event = threading.Event()
        self.waiting[result.id] = event
        success = event.wait(timeout)
        del self.waiting[result.id]
        result.refresh()
        return success

    def wait(self, timeout: float | None = None) -> bool:
        """
        Waits for the next time there are no tasks to run (i.e. the queue is empty).
        """
        self.empty.clear()
        return self.empty.wait(timeout)

    def stop(self):
        """
        Signals the runner to stop.
        """
        logger.info("Shutting down task runner")
        self.stopsign.set()

    def reload(self):
        """
        Reloads all known task modules.
        """
        with self.lock:
            for mod_path in list(self.seen_modules):
                try:
                    mod = import_string(mod_path)
                    importlib.reload(mod)
                    logger.debug(f"Reloaded module {mod_path}")
                except ImportError:
                    logger.debug(f"Error reloading {mod_path}")
                    self.seen_modules.discard(mod_path)
