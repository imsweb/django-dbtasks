from django.contrib import admin
from django.tasks import TaskResultStatus

from .models import ScheduledTask


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "task_path",
        "status",
        "priority",
        "enqueued_at",
        "run_after",
        "finished_at",
    ]
    list_filter = [
        "task_path",
        "status",
        "periodic",
        "queue",
        "backend",
    ]
    actions = ["run_task"]

    @admin.action(description="Mark ready to run now")
    def run_task(self, request, queryset):
        queryset.update(status=TaskResultStatus.READY, run_after=None)
