from dbtasks.testing import RunnerTestCase

from .loggers import stash, stashlock


class LoggedRunnerTestCase(RunnerTestCase):
    def setUp(self):
        with stashlock:
            stash.clear()
        super().setUp()

    def tearDown(self):
        super().tearDown()

    @property
    def logs(self) -> dict:
        with stashlock:
            return stash.copy()

    @property
    def task_logs(self) -> dict:
        with stashlock:
            return stash.get("tests.tasks", {}).copy()
