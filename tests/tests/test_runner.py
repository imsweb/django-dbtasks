from unittest.mock import call, patch

from django.db import connection
from django.test import TransactionTestCase

from dbtasks.runner import Runner


class RunnerTests(TransactionTestCase):
    def test_reconnect_after_scheduling_error(self):
        runner = Runner(init_periodic=False, loop_delay=0.25)
        runner.should_delete_tasks = False
        self.addCleanup(runner.executor.shutdown)

        connection.ensure_connection()
        original_connection = connection.connection
        schedule_tasks = runner.schedule_tasks
        attempts = 0
        recovered_connections = []

        def schedule():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                # Simulate a disconnect without clearing Django's connection wrapper.
                original_connection.close()
            try:
                delay = schedule_tasks()
                recovered_connections.append(connection.connection)
                return delay
            finally:
                # Bound the test even if the second attempt also fails.
                if attempts >= 2:
                    runner.stop()

        with (
            patch.object(runner, "schedule_tasks", side_effect=schedule),
            patch("dbtasks.runner.time.sleep") as sleep,
        ):
            runner.run()

        self.assertEqual(attempts, 2)
        self.assertEqual(len(recovered_connections), 1)
        self.assertIsNot(recovered_connections[0], original_connection)
        self.assertEqual(sleep.call_args_list, [call(1.0), call(0.25)])
        self.assertTrue(runner.empty.is_set())
        self.assertTrue(runner.finished.is_set())
