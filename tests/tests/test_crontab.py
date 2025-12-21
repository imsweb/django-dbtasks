import unittest
from datetime import datetime, time

from dbtasks.schedule import Crontab, hour, minute, month, weekday


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
