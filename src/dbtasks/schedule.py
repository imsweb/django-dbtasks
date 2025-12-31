import random
import re
from datetime import datetime, timedelta
from typing import Iterator


class ScheduleExhausted(Exception):
    pass


class Schedule:
    def match(self, dt: datetime) -> bool:
        """
        Returns whether the specified `datetime` matches the schedule.
        """
        raise NotImplementedError()

    def first(self, after: datetime) -> datetime | None:
        """
        Returns the first scheduled `datetime` after the specified `datetime`, or `None`
        if one cannot be found. May also raise `ScheduleExhausted`.
        """
        raise NotImplementedError()

    def next(
        self,
        after: datetime | None = None,
        until: datetime | None = None,
    ) -> datetime:
        """
        Returns the next matching `datetime` after the one specified (or after the
        current date if not specified), and before the specified `until` (if specified).
        Raises `ScheduleExhausted` if unable to find a matching date.
        """
        if after is None:
            after = datetime.now()
        if d := self.first(after):
            if until is None or d < until:
                return d
        raise ScheduleExhausted("Could not find next matching date")

    def dates(
        self,
        after: datetime | None = None,
        until: datetime | None = None,
    ) -> Iterator[datetime]:
        """
        Yields each scheduled `datetime` between `after` and `until`.
        """
        d = after
        while True:
            try:
                d = self.next(after=d, until=until)
                yield d
            except ScheduleExhausted:
                break


# Python's `isoweekday` uses Sunday=7
WEEKDAYS = {
    "mon": 1,
    "tue": 2,
    "wed": 3,
    "thu": 4,
    "fri": 5,
    "sat": 6,
    "sun": 7,
}

MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


class CrontabParseError(Exception):
    pass


class CrontabParser:
    def __init__(
        self,
        min_value: int,
        max_value: int,
        names: dict[str, int] | None = None,
        replace: dict[int, int] | None = None,
    ):
        self.min_value = min_value
        self.max_value = max_value
        self.names = {
            key[:3].lower(): self._range_check(value)
            for key, value in (names or {}).items()
        }
        self.replace = replace or {}

    def _range_check(self, num: int) -> int:
        if num < self.min_value or num > self.max_value:
            raise CrontabParseError(
                f"{num} is not in the range {self.min_value}-{self.max_value}"
            )
        return num

    def _get_value(self, part: str) -> int:
        if part.isdigit():
            return self._range_check(int(part))
        elif value := self.names.get(part[:3].lower()):
            return value
        raise CrontabParseError(f"Could not parse value: {part}")

    def parse_part(self, part: str) -> list[int]:
        if "/" in part:
            value, step = part.split("/", 1)
            step = self._range_check(int(step))
            values = self.parse_part(value)
            return values[::step]
        elif part == "*":
            return list(range(self.min_value, self.max_value + 1))
        elif part == "~":
            return [random.randint(self.min_value, self.max_value)]
        elif "-" in part:
            lo, hi = (self._get_value(p) for p in part.split("-", 1))
            if lo > hi:
                raise CrontabParseError(f"{lo}-{hi} is not a valid range ({lo} > {hi})")
            return list(range(lo, hi + 1))
        return [self._get_value(part)]

    def parse(self, spec: str) -> list[int]:
        values: set[int] = set()
        for part in spec.split(","):
            values.update(self.parse_part(part))
        for num, repl in self.replace.items():
            if num in values:
                values.discard(num)
                values.add(repl)
        return list(sorted(values))


minute = CrontabParser(0, 59)
hour = CrontabParser(0, 23)
day = CrontabParser(1, 31)
month = CrontabParser(1, 12, names=MONTHS)
weekday = CrontabParser(0, 7, names=WEEKDAYS, replace={0: 7})


class Crontab(Schedule):
    def __init__(self, spec: str):
        parts = spec.split(None)
        if len(parts) != 5:
            raise CrontabParseError("Crontab specs must have 5 parts")
        self.spec = spec
        self.minutes = minute.parse(parts[0])
        self.hours = hour.parse(parts[1])
        self.days = day.parse(parts[2])
        self.months = month.parse(parts[3])
        self.weekdays = weekday.parse(parts[4])
        self.specifies_day = parts[2] != "*"
        self.specifies_weekday = parts[4] != "*"

    def __repr__(self):
        return f"crontab({self.spec!r})"

    def match(self, dt: datetime) -> bool:
        if dt.minute not in self.minutes:
            return False
        if dt.hour not in self.hours:
            return False
        if dt.month not in self.months:
            return False
        if self.specifies_day and self.specifies_weekday:
            # Special case when both day and weekday are specified - by spec it matches
            # when *either* match.
            if (dt.day not in self.days) and (dt.isoweekday() not in self.weekdays):
                return False
        else:
            # Otherwise when one or none are specified, check them separately.
            if dt.day not in self.days:
                return False
            if dt.isoweekday() not in self.weekdays:
                return False
        return True

    def first(self, after: datetime) -> datetime | None:
        # Crontabs never schedule more than a year out.
        limit = after + timedelta(days=365)
        while after < limit:
            after += timedelta(minutes=1)
            if self.match(after):
                return after.replace(second=0, microsecond=0)


class Duration(timedelta):
    """
    A `datetime.timedelta` subclass that can be constructed with a duration string of
    the format `WwDdHhMmSs` where the capital letters are integers and the lowercase
    letters are duration specifiers (week/day/hour/minute/second). Also adds a
    `duration_string` method for converting back to this format.
    """

    SPECS = {
        "w": 604800,
        "d": 86400,
        "h": 3600,
        "m": 60,
        "s": 1,
    }

    SPLITTER = re.compile("([{}])".format("".join(SPECS)))

    def __new__(cls, value: str | int | timedelta):
        if isinstance(value, timedelta):
            return timedelta.__new__(cls, seconds=value.total_seconds())
        if isinstance(value, int):
            return timedelta.__new__(cls, seconds=value)
        if value.isdigit():
            return timedelta.__new__(cls, seconds=int(value))
        parts = Duration.SPLITTER.split(value.lower().strip())
        seconds = 0
        for pair in zip(parts[::2], parts[1::2]):
            if pair:
                seconds += int(pair[0]) * Duration.SPECS[pair[1]]
        return timedelta.__new__(cls, seconds=seconds)

    def duration_string(self):
        seconds = self.total_seconds()
        duration: list[str] = []
        for fmt, sec in Duration.SPECS.items():
            num = int(seconds // sec)
            if num > 0:
                duration.append("{}{}".format(num, fmt))
                seconds -= num * sec
        return "".join(duration)


class Every(Schedule):
    def __init__(self, period: str | int | timedelta, start: datetime | None = None):
        self.period = Duration(period)
        self.period_seconds = int(self.period.total_seconds())
        self.start = start or datetime.now().replace(microsecond=0)
        self.start_timestamp = int(self.start.timestamp())

    def __repr__(self):
        return "period({})".format(repr(self.period.duration_string()))

    def match(self, dt: datetime) -> bool:
        return (int(dt.timestamp()) - self.start_timestamp) % self.period_seconds == 0

    def first(self, after: datetime) -> datetime | None:
        periods = (
            (int(after.timestamp()) - self.start_timestamp) // self.period_seconds
        ) + 1
        # Using fromtimestamp will skip non-existent times due to DST.
        return datetime.fromtimestamp(
            self.start_timestamp + (self.period_seconds * periods)
        )
