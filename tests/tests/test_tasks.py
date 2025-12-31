import copy

from django.conf import settings
from django.tasks import TaskResult, TaskResultStatus, default_task_backend
from django.test import TestCase, override_settings

from dbtasks.models import ScheduledTask
from dbtasks.periodic import Periodic
from dbtasks.schedule import Duration

from ..tasks import kaboom, maintenance, send_mail
from ..utils import LoggedRunnerTestCase


class ScheduledTaskTests(LoggedRunnerTestCase):
    def test_single_task(self):
        result: TaskResult = send_mail.enqueue("user@example.com", "hello world!")
        self.assertTrue(self.runner.wait_for(result))
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(result.return_value, {"sent": True})
        # The send_mail task retention is set to 0, so it should be deleted immediately.
        t = ScheduledTask.objects.get(pk=result.id)
        self.assertEqual(t.finished_at, t.delete_after)
        self.assertEqual(
            self.task_logs,
            {"INFO": ["Sending mail to user@example.com: hello world!"]},
        )
        self.runner.delete_tasks()
        with self.assertRaises(ScheduledTask.DoesNotExist):
            t.refresh_from_db()

    def test_lots_of_tasks(self):
        expected = set()
        for k in range(100):
            send_mail.enqueue(f"user-{k}@example.com", "hello!")
            expected.add(f"Sending mail to user-{k}@example.com: hello!")
        self.runner.wait()
        self.assertEqual(set(self.task_logs["INFO"]), expected)

    def test_periodic(self):
        self.assertIn(maintenance.module_path, self.runner.periodic)
        # No retention period is set by default.
        self.assertIsNone(self.runner.backend.get_retention(maintenance.module_path))
        self.runner.init_periodic()
        first = ScheduledTask.objects.get(
            task_path=maintenance.module_path,
            status=TaskResultStatus.READY,
            periodic=True,
        )
        # Run the initial scheduled task manually.
        result = self.runner.submit_task(first)
        self.assertTrue(self.runner.wait_for(result))
        first.refresh_from_db()
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(first.status, TaskResultStatus.SUCCESSFUL)
        # Make sure a second periodic task got scheduled when the first completed.
        second = ScheduledTask.objects.get(
            task_path=maintenance.module_path,
            status=TaskResultStatus.READY,
            periodic=True,
        )
        self.assertNotEqual(first.id, second.id)
        self.assertGreater(second.enqueued_at, first.enqueued_at)
        # Now run the maintenance task manually, not periodically.
        result: TaskResult = maintenance.enqueue()
        self.assertTrue(self.runner.wait_for(result))
        manual = ScheduledTask.objects.get(pk=result.id)
        # Make sure the manual run was not marked periodic, and that no new tasks were
        # automatically scheduled afterwards.
        self.assertFalse(manual.periodic)
        self.assertEqual(
            ScheduledTask.objects.filter(
                task_path=maintenance.module_path, status=TaskResultStatus.READY
            ).count(),
            1,
        )
        second.refresh_from_db()
        self.assertEqual(second.status, TaskResultStatus.READY)

    def test_failed_task(self):
        result: TaskResult = kaboom.enqueue("Boom goes the dynamite!")
        self.assertTrue(self.runner.wait_for(result))
        self.assertEqual(result.status, TaskResultStatus.FAILED)
        with self.assertRaises(ValueError):
            result.return_value
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0].exception_class_path, "builtins.ValueError")


def opts(**options):
    tasks = copy.deepcopy(settings.TASKS)
    tasks["default"]["OPTIONS"].update(options)
    return tasks


class DefaultRetainTest(TestCase):
    @override_settings(TASKS=opts(retain="14d"))
    def test_default_retain(self):
        self.assertEqual(
            default_task_backend.get_retention("tests.tasks.maintenance"),
            Duration("14d"),
        )

    @override_settings(TASKS=opts(retain={"tests.tasks.maintenance": "30d"}))
    def test_custom_retain(self):
        self.assertEqual(
            default_task_backend.get_retention("tests.tasks.maintenance"),
            Duration("30d"),
        )

    @override_settings(
        TASKS=opts(
            retain="30d",
            periodic={
                "tests.tasks.maintenance": Periodic("24h", retain="7d"),
            },
        )
    )
    def test_custom_periodic(self):
        self.assertEqual(
            default_task_backend.get_retention("tests.tasks.maintenance"),
            Duration("7d"),
        )
