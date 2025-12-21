import concurrent.futures
import datetime
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
    task,
    task_backends,
)
from django.utils import timezone
from django.utils.module_loading import import_string

from .backend import DatabaseBackend
from .models import ScheduledTask
from .periodic import Periodic

logger = logging.getLogger(__name__)


def run_task(pk: str) -> TaskResultStatus:
    """
    Fetches, runs, and updates a `ScheduledTask`. Runs in a worker thread.
    """
    try:
        task = ScheduledTask.objects.get(pk=pk)
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
        pass


@task
def cleanup(retention: int):
    before = timezone.now() - datetime.timedelta(seconds=retention)
    logger.info(f"Cleaning up scheduled tasks before {before}")
    return ScheduledTask.objects.filter(
        status__in=[TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED],
        finished_at__lt=before,
    ).delete()


class Runner:
    def __init__(
        self,
        workers: int = 4,
        worker_id: str | None = None,
        backend: str = DEFAULT_TASK_BACKEND_ALIAS,
        loop_delay: float = 0.5,
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
        # Signaled when the runner should stop.
        self.stopsign = threading.Event()
        # Signaled when the queue is empty (no READY tasks).
        self.empty = threading.Event()
        # Covers `self.tasks`, `self.seen_modules`, and `self.processed` access.
        self.lock = threading.Lock()
        # Allows callers to block on a single task being completed.
        self.waiting: dict[str, threading.Event] = {}
        self.periodic: dict[str, Periodic] = {}
        if retain := self.backend.options.get("retain"):
            # If the task backend specifies a retention period, schedule a periodic task
            # to delete finished tasks older than that period.
            retain_secs = 0
            if isinstance(retain, int):
                retain_secs = retain
            elif isinstance(retain, datetime.timedelta):
                retain_secs = int(retain.total_seconds())
            else:
                raise ImproperlyConfigured(
                    "Backend `retain` option should be an `int` or `timedelta`"
                )
            self.periodic[f"{__name__}.cleanup"] = Periodic(
                "~ * * * *", args=[retain_secs]
            )
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
        pk: str,
        task_path: str,
        was_periodic: bool,
        fut: concurrent.futures.Future,
    ):
        """
        Called when a task is finished. Removes the task from `self.tasks` and logs the
        completion. If the task was a periodic task, schedules the next run. Note that
        there are no guarantees about which thread this method is called from.
        """
        with self.lock:
            self.processed += 1
            del self.tasks[pk]

        try:
            status = fut.result()
            logger.info(f"Task {task_path} ({pk}) finished with status {status}")
        except Exception as ex:
            logger.info(f"Task {task_path} ({pk}) raised {ex}")

        if event := self.waiting.get(pk):
            event.set()

        if was_periodic and (schedule := self.periodic.get(task_path)):
            after = timezone.make_aware(schedule.next())
            t = ScheduledTask.objects.create(
                task_path=task_path,
                args=schedule.args,
                kwargs=schedule.kwargs,
                backend=self.backend.alias,
                run_after=after,
                periodic=True,
            )
            logger.info(f"Re-scheduled {t} for {after}")

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
            logger.debug(f"Submitting {t} for execution")
            f = self.executor.submit(run_task, t.task_id)
            with self.lock:
                # Keep track of task modules we've seen, so we can reload them.
                self.seen_modules.add(t.task_path.rsplit(".", 1)[0])
                self.tasks[t.task_id] = f
            f.add_done_callback(
                functools.partial(
                    self.task_done,
                    t.task_id,
                    t.task_path,
                    t.periodic,
                ),
            )

        if len(tasks) >= available:
            # We got a full batch, try again immediately.
            return 0

        return self.loop_delay

    def init_periodic(self):
        """
        Removes any outstanding scheduled periodic tasks, and schedules the next runs
        for each.
        """
        # First delete any un-started periodic tasks.
        ScheduledTask.objects.filter(
            status=TaskResultStatus.READY,
            periodic=True,
        ).delete()
        # Then schedule the next run of each periodic task. Subsequent runs will be
        # scheduled on completion.
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
        self.init_periodic()
        try:
            while not self.stopsign.is_set():
                delay = self.schedule_tasks()
                time.sleep(delay)
        except KeyboardInterrupt:
            pass
        finally:
            self.executor.shutdown()
            connection.close()

    def wait_for(self, result: TaskResult, timeout: float | None = None) -> bool:
        """
        Waits for the specified `TaskResult` to complete (or fail).
        """
        if result.status in (TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED):
            return True
        logger.info(f"Waiting for {result.id}...")
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
