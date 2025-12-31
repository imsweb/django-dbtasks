import os
import threading
from typing import ClassVar

from django.test import TransactionTestCase

from .runner import Runner


class RunnerTestCase(TransactionTestCase):
    runner: ClassVar[Runner]

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Run with a very short loop delay to speed up tests. None of our test tasks
        # take very long, so there's not much point in waiting aside from not flooding
        # the database with queries for new tasks. Also we don't initialize periodic
        # tasks - individual tests can call `self.runner.init_periodic()`.
        cls.runner = Runner(
            workers=max(1, (os.cpu_count() or 4) - 1),
            loop_delay=0.01,
            init_periodic=False,
        )
        # Don't deleted completed tasks - tests can call `self.runner.delete_tasks()`.
        cls.runner.should_delete_tasks = False
        threading.Thread(target=cls.runner.run).start()
        cls.runner.ready.wait()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.runner.stop()
        cls.runner.finished.wait()
