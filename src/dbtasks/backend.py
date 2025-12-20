import logging
from typing import Any

from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

from .models import ScheduledTask

logger = logging.getLogger(__name__)


class DatabaseBackend(BaseTaskBackend):
    supports_defer = True
    supports_get_result = True
    supports_priority = True

    @property
    def immediate(self):
        return bool(self.options.get("immediate", False))

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
        task_enqueued.send(type(self), task_result=scheduled.result)

        if self.immediate:
            logger.info(f"Running {scheduled} IMMEDIATELY")

            scheduled.status = TaskResultStatus.RUNNING
            scheduled.started_at = timezone.now()
            scheduled.save(update_fields=["status", "started_at"])
            task_started.send(type(self), task_result=scheduled.result)

            scheduled.run_and_update()
            task_finished.send(type(self), task_result=scheduled.result)

        return scheduled.result

    def get_result(self, result_id) -> TaskResult:
        try:
            return ScheduledTask.objects.get(pk=result_id).result
        except ScheduledTask.DoesNotExist:
            raise TaskResultDoesNotExist(result_id)
