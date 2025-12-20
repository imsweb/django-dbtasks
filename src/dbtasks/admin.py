from django.contrib import admin

from .models import ScheduledTask


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "task_path",
        "status",
        "priority",
        "enqueued_at",
        "finished_at",
    ]
    list_filter = [
        "task_path",
        "status",
        "periodic",
        "queue",
        "backend",
    ]
