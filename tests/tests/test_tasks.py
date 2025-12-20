from django.tasks import TaskResult, TaskResultStatus

from ..tasks import send_mail
from ..utils import LoggedRunnerTestCase


class ScheduledTaskTests(LoggedRunnerTestCase):
    def test_single_task(self):
        result: TaskResult = send_mail.enqueue("user@example.com", "hello world!")
        self.assertTrue(self.runner.wait_for(result))
        self.assertEqual(result.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(result.return_value, {"sent": True})
        self.assertEqual(
            self.task_logs,
            {"INFO": ["Sending mail to user@example.com: hello world!"]},
        )

    def test_lots_of_tasks(self):
        expected = set()
        for k in range(100):
            send_mail.enqueue(f"user-{k}@example.com", "hello!")
            expected.add(f"Sending mail to user-{k}@example.com: hello!")
        self.runner.wait()
        self.assertEqual(self.runner.processed, 100)
        self.assertEqual(set(self.task_logs["INFO"]), expected)
