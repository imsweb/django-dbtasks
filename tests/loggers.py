import logging
import threading

stash = {}
stashlock = threading.Lock()


class TestLogHandler(logging.Handler):
    """
    Logging handler that stashes all logged messages by logger name and level.
    """

    def emit(self, record: logging.LogRecord):
        with stashlock:
            levels = stash.setdefault(record.name, {})
            levels.setdefault(record.levelname, []).append(record.getMessage())
