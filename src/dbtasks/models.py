import logging
import traceback
import uuid
from typing import TYPE_CHECKING

from django.core.exceptions import SuspiciousOperation
from django.db import models
from django.tasks import (
    DEFAULT_TASK_QUEUE_NAME,
    Task,
    TaskContext,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
from django.tasks.base import TaskError
from django.utils import timezone
from django.utils.module_loading import import_string

if TYPE_CHECKING:
    from .backend import DatabaseBackend

logger = logging.getLogger(__name__)


def new_task_id():
    try:
        return uuid.uuid7()
    except AttributeError:
        return uuid.uuid4()


class ScheduledTask(models.Model):
    id = models.UUIDField(primary_key=True, default=new_task_id, editable=False)

    status = models.CharField(
        choices=TaskResultStatus.choices,
        default=TaskResultStatus.READY,
        max_length=max(len(value) for value in TaskResultStatus.values),
    )

    enqueued_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)

    task_path = models.TextField()
    priority = models.IntegerField(default=0)
    queue = models.CharField(max_length=32, default=DEFAULT_TASK_QUEUE_NAME)
    backend = models.CharField(max_length=32)
    run_after = models.DateTimeField(null=True, blank=True)
    delete_after = models.DateTimeField(null=True, blank=True)

    periodic = models.BooleanField(default=False)

    worker_ids = models.JSONField(default=list, blank=True)

    return_value = models.JSONField(default=None, null=True)

    exception_path = models.TextField(blank=True)
    traceback = models.TextField(blank=True)

    class Meta:
        indexes = [
            models.Index(
                "status",
                "backend",
                "queue",
                "run_after",
                name="idx_dbtasks_scheduledtask",
            ),
        ]

    def __str__(self):
        return f"{self.task_path} ({self.task_id})"

    @property
    def task_id(self) -> str:
        return str(self.pk)

    @property
    def task_backend(self) -> "DatabaseBackend":
        return task_backends[self.backend]

    @property
    def task(self) -> Task:
        task = import_string(self.task_path)

        if not isinstance(task, Task):
            raise SuspiciousOperation(
                f"Task {self.id} does not point to a Task ({self.task_path})"
            )

        return task.using(
            priority=self.priority,
            queue_name=self.queue,
            run_after=self.run_after,
            backend=self.backend,
        )

    @property
    def result(self) -> TaskResult:
        errors = []
        if self.exception_path and self.status == TaskResultStatus.FAILED:
            errors.append(
                TaskError(
                    exception_class_path=self.exception_path,
                    traceback=self.traceback,
                )
            )
        r = TaskResult(
            task=self.task,
            id=self.task_id,
            status=TaskResultStatus(self.status),
            enqueued_at=self.enqueued_at,
            started_at=self.started_at,
            last_attempted_at=self.started_at,
            finished_at=self.finished_at,
            args=self.args,
            kwargs=self.kwargs,
            backend=self.backend,
            errors=errors,
            worker_ids=self.worker_ids,
        )
        object.__setattr__(r, "_return_value", self.return_value)
        return r

    def run(self):
        """
        Runs the task, passing `args`, `kwargs`, and a `TaskContext` if requested.
        """
        if self.task.takes_context:
            return self.task.call(
                TaskContext(task_result=self.result),
                *self.args,
                **self.kwargs,
            )
        else:
            return self.task.call(*self.args, **self.kwargs)

    def run_and_update(self) -> TaskResultStatus:
        """
        Executes the task (on the current thread) and sets the status and related fields
        accordingly. The `status` and `finished_at` fields are always updated.
        `return_value` is set upon success, while `exception_path` and `traceback` are
        set upon failure. `delete_after` is set if a retention period is set for the
        task (which is added to `finished_at`).
        """
        fields = ["finished_at", "status"]

        try:
            self.return_value = self.run()
            self.status = TaskResultStatus.SUCCESSFUL
            fields.append("return_value")
        except Exception as ex:
            self.exception_path = (
                f"{ex.__class__.__module__}.{ex.__class__.__qualname__}"
            )
            self.traceback = "".join(traceback.format_exception(ex))
            self.status = TaskResultStatus.FAILED
            fields.extend(["exception_path", "traceback"])

        self.finished_at = timezone.now()

        try:
            retain = self.task_backend.get_retention(self.task_path)
            if retain is not None:
                self.delete_after = self.finished_at + retain
                fields.append("delete_after")
        except Exception:
            # Log this loudly, but don't fail.
            logger.exception(f"Could not set `delete_after` for {self}")

        self.save(update_fields=fields)
        return self.status
