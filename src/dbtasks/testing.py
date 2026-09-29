import threading

from django.test import TransactionTestCase

from .runner import Runner


class RunnerTestCase(TransactionTestCase):
    runner: Runner

    def setUp(self):
        super().setUp()
        # Run with a very short loop delay to speed up tests. None of our test tasks
        # take very long, so there's not much point in waiting aside from not flooding
        # the database with queries for new tasks. Also we don't initialize periodic
        # tasks - individual tests can call `self.runner.init_periodic()`.
        self.runner = Runner(
            worker_id=self.id(),
            loop_delay=0.01,
            init_periodic=False,
        )
        # Don't deleted completed tasks - tests can call `self.runner.delete_tasks()`.
        self.runner.should_delete_tasks = False
        threading.Thread(target=self.runner.run).start()
        self.runner.ready.wait()

    def tearDown(self):
        self.runner.stop()
        self.runner.finished.wait()
        super().tearDown()
