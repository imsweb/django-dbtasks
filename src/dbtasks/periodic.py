from datetime import datetime, timedelta
from typing import Callable

from .schedule import Crontab, Every, Schedule


class Periodic:
    schedule: Schedule

    def __init__(
        self,
        spec: Schedule | str | int | timedelta,
        args: list | tuple | Callable[[], list | tuple] | None = None,
        kwargs: dict | Callable[[], dict] | None = None,
    ):
        if isinstance(spec, Schedule):
            self.schedule = spec
        elif isinstance(spec, str) and len(spec.split()) == 5:
            self.schedule = Crontab(spec)
        else:
            self.schedule = Every(spec)
        self._args = args
        self._kwargs = kwargs

    @property
    def args(self) -> list:
        if self._args is None:
            return []
        elif isinstance(self._args, (list, tuple)):
            return list(self._args)
        else:
            return list(self._args())

    @property
    def kwargs(self) -> dict:
        if self._kwargs is None:
            return {}
        elif isinstance(self._kwargs, dict):
            return self._kwargs
        else:
            return self._kwargs()

    def next(
        self,
        after: datetime | None = None,
        until: datetime | None = None,
    ) -> datetime:
        return self.schedule.next(after=after, until=until)
