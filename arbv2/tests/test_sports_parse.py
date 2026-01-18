import unittest

from arbv2.parse.sports import _clean_team_name


class SportsParseTests(unittest.TestCase):
    def test_clean_team_name_strips_schedule_suffix(self) -> None:
        raw = "PIMBLETT PROFESSIONAL MMA FIGHT SCHEDULED FOR JAN 24 2026"
        self.assertEqual(_clean_team_name(raw), "PIMBLETT")


if __name__ == "__main__":
    unittest.main()
