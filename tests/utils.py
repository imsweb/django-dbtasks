from dbtasks.testing import RunnerTestCase

from .loggers import stash, stashlock


class LoggedRunnerTestCase(RunnerTestCase):
    def setUp(self):
        with stashlock:
            stash.clear()

    def tearDown(self):
        pass

    @property
    def logs(self) -> dict:
        with stashlock:
            return stash.copy()

    @property
    def task_logs(self) -> dict:
        with stashlock:
            return stash.get("tests.tasks", {}).copy()
