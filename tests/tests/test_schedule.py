import unittest
from datetime import datetime, time, timedelta

from dbtasks.schedule import Crontab, Duration, Every, hour, minute, month, weekday


class CrontabTests(unittest.TestCase):
    def test_parts(self):
        self.assertEqual(minute.parse("30"), [30])
        self.assertEqual(minute.parse("7-12,42"), [7, 8, 9, 10, 11, 12, 42])
        self.assertEqual(len(minute.parse("~")), 1)
        self.assertEqual(minute.parse("*/15"), [0, 15, 30, 45])
        self.assertEqual(minute.parse("30-45/3"), [30, 33, 36, 39, 42, 45])
        self.assertEqual(hour.parse("*/23"), [0, 23])
        self.assertEqual(month.parse("oct,february,jun"), [2, 6, 10])
        self.assertEqual(month.parse("jan-jun/2"), [1, 3, 5])
        self.assertEqual(weekday.parse("0-2,5"), [1, 2, 5, 7])

    def test_crontab(self):
        # 4:30am on the 1st and 15th of each month
        c = Crontab("30 4 1,15 * *")
        matches = list(c.dates(after=datetime(2025, 1, 1), until=datetime(2026, 1, 1)))
        self.assertEqual(len(matches), 12 * 2)
        self.assertTrue(all(d.day in (1, 15) for d in matches))
        self.assertTrue(all(d.time() == time(4, 30) for d in matches))

    def test_duration(self):
        d = Duration("2w3d7h21m10s")
        self.assertIsInstance(d, Duration)
        self.assertIsInstance(d, timedelta)
        self.assertEqual(int(d.total_seconds()), 1495270)
        self.assertEqual(str(d), "17 days, 7:21:10")
        self.assertEqual(d.duration_string(), "2w3d7h21m10s")
        d = Duration("107s")
        self.assertEqual(int(d.total_seconds()), 107)
        self.assertEqual(d.duration_string(), "1m47s")
        d = datetime.now()
        self.assertEqual(d + Duration("2w"), d + timedelta(days=14))
        self.assertEqual(Duration("2w"), Duration("1209600"))
        self.assertEqual(Duration(Duration("2w")), Duration("2w"))

    def test_periodic(self):
        # Weekly starting 2025-01-01 at midnight
        s = Every("1w", datetime(2025, 1, 1))
        self.assertTrue(s.match(datetime(2025, 1, 1)))
        self.assertTrue(s.match(datetime(2025, 2, 5)))
        self.assertFalse(s.match(datetime(2025, 2, 5, 7)))
        self.assertFalse(s.match(datetime(2025, 2, 6)))
        matches = list(s.dates(after=datetime(2025, 1, 1), until=datetime(2025, 2, 1)))
        self.assertEqual(
            matches,
            [
                datetime(2025, 1, 8),
                datetime(2025, 1, 15),
                datetime(2025, 1, 22),
                datetime(2025, 1, 29),
            ],
        )
        # Every hour starting 2025-01-01 at 5:30am
        s = Every("1h", datetime(2025, 1, 1, 5, 30))
        # The first time after 1am on 3/9 is 1:30am
        self.assertEqual(s.first(datetime(2025, 3, 9, 1)), datetime(2025, 3, 9, 1, 30))
        # Due to DST (EST->EDT), the first time after 1:30am is not 2:30am, but 3:30am
        self.assertEqual(
            s.first(datetime(2025, 3, 9, 1, 30)), datetime(2025, 3, 9, 3, 30)
        )
