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
        # the database with queries for new tasks.
        cls.runner = Runner(workers=os.cpu_count() - 1, loop_delay=0.01)
        cls.runner_thread = threading.Thread(target=cls.runner.run)
        cls.runner_thread.start()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.runner.stop()
        cls.runner_thread.join()
