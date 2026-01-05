from django.contrib import admin
from django.tasks import TaskResultStatus
from django.utils import timezone

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
    actions = ["mark_ready", "mark_deletion"]
    ordering = ["-enqueued_at"]

    @admin.action(description="Mark ready to run now")
    def mark_ready(self, request, queryset):
        queryset.update(status=TaskResultStatus.READY, run_after=None)

    @admin.action(description="Mark ready for deletion")
    def mark_deletion(self, request, queryset):
        queryset.update(delete_after=timezone.now())
