import importlib.metadata

from .periodic import Periodic
from .schedule import Crontab, Duration, Every, Schedule, ScheduleExhausted

__version__ = importlib.metadata.version("django-dbtasks")
__version_info__ = tuple(
    int(num) if num.isdigit() else num for num in __version__.split(".")
)

__all__ = [
    "Crontab",
    "Duration",
    "Every",
    "Periodic",
    "Schedule",
    "ScheduleExhausted",
    "__version__",
    "__version_info__",
]
