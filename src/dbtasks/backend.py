import logging
from collections.abc import Mapping
from typing import Any

from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

from .models import ScheduledTask
from .periodic import Periodic
from .schedule import Duration

logger = logging.getLogger(__name__)


class DatabaseBackend(BaseTaskBackend):
    supports_defer = True
    supports_get_result = True
    supports_priority = True

    @property
    def immediate(self):
        """
        Whether tasks should be executed immediately by the backend. Useful when testing
        without having to run a worker.
        """
        return bool(self.options.get("immediate", False))

    @property
    def send_signals(self):
        """
        Whether the `task_enqueued`, `task_started`, and `task_finished` should be sent
        when executing tasks with this backend. Defaults to `True`.
        """
        return bool(self.options.get("signals", True))

    @property
    def worker_id(self):
        """
        The `worker_id` to record when tasks are run by the backend itself (i.e. when
        `immediate=True`).
        """
        return f"{type(self).__module__}.{type(self).__qualname__}"

    def get_retention(self, task_name: str) -> Duration | None:
        """
        Returns the retention period for the specified `task_name`, or `None` if there
        is none defined (in which case it should be retained indefinitely).
        """
        # Special case for periodic tasks with a specific retention.
        if periodic := self.options.get("periodic"):
            if spec := periodic.get(task_name):
                if isinstance(spec, Periodic) and spec.retain is not None:
                    return spec.retain
        retain = self.options.get("retain")
        if retain is None:
            return None
        elif isinstance(retain, Mapping):
            value = retain.get(task_name)
            return None if value is None else Duration(value)
        return Duration(retain)

    def validate_task(self, task):
        super().validate_task(task)
        if self.immediate and task.run_after is not None:
            raise InvalidTask("Backend does not support run_after in immediate mode.")

    def enqueue(self, task: Task, args: list[Any], kwargs: dict[str, Any]):
        self.validate_task(task)

        scheduled = ScheduledTask.objects.create(
            task_path=task.module_path,
            priority=task.priority,
            queue=task.queue_name,
            backend=task.backend,
            run_after=task.run_after,
            args=normalize_json(args),
            kwargs=normalize_json(kwargs),
        )

        logger.debug(f"Enqueued {scheduled}")
        if self.send_signals:
            task_enqueued.send(type(self), task_result=scheduled.result)

        if self.immediate:
            logger.info(f"Running {scheduled} IMMEDIATELY")

            scheduled.status = TaskResultStatus.RUNNING
            scheduled.started_at = timezone.now()
            scheduled.worker_ids.append(self.worker_id)
            scheduled.save(update_fields=["status", "started_at", "worker_ids"])

            if self.send_signals:
                task_started.send(type(self), task_result=scheduled.result)

            scheduled.run_and_update()

            if self.send_signals:
                task_finished.send(type(self), task_result=scheduled.result)

        return scheduled.result

    def get_result(self, result_id) -> TaskResult:
        try:
            return ScheduledTask.objects.get(pk=result_id).result
        except ScheduledTask.DoesNotExist:
            raise TaskResultDoesNotExist(result_id)
