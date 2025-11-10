import unittest
from trap import (
    is_trap,
    _is_too_long,
    _has_excessive_path_depth,
    _has_admin_segments,
    _has_repetitive_patterns,
    _is_calendar_page,
    _is_path_query_overused,
    _has_trap_query_params
)

class TrapDetectionICSTests(unittest.TestCase):

    def test_is_too_long(self):
        long_url = "http://ics.uci.edu/" + "a" * 2100
        self.assertTrue(_is_too_long(long_url))
        short_url = "http://ics.uci.edu/about"
        self.assertFalse(_is_too_long(short_url))

    def test_excessive_path_depth(self):
        deep_path = "/" + "/".join(["dir"] * 45)
        self.assertTrue(_has_excessive_path_depth(deep_path))
        shallow_path = "/faculty/baldwin"
        self.assertFalse(_has_excessive_path_depth(shallow_path))

    def test_admin_segments(self):
        self.assertTrue(_has_admin_segments("/wp-admin/settings"))
        self.assertTrue(_has_admin_segments("/admin/tools"))
        self.assertFalse(_has_admin_segments("/research/projects"))

    def test_repetitive_patterns(self):
        domain = "ics.uci.edu"
        repetitive_path = "/lab/data/lab/data/lab/data/"
        for _ in range(13):
            _has_repetitive_patterns(repetitive_path, domain)
        self.assertTrue(_has_repetitive_patterns(repetitive_path, domain))

    def test_calendar_page(self):
        domain = "ics.uci.edu"
        calendar_path = "/events/2023/11/09/"
        for _ in range(251):
            _is_calendar_page(calendar_path, domain)
        self.assertTrue(_is_calendar_page(calendar_path, domain))

    def test_path_query_overused(self):
        domain = "ics.uci.edu"
        path = "/search"
        for _ in range(51):
            _is_path_query_overused(domain, path)
        self.assertTrue(_is_path_query_overused(domain, path))

    def test_trap_query_params(self):
        self.assertTrue(_has_trap_query_params("/index", "sessionid=abc123"))
        self.assertTrue(_has_trap_query_params("/calendar", "p=9999"))
        self.assertFalse(_has_trap_query_params("/faculty", "view=profile"))

    def test_is_trap_composite(self):
        trap_url = "http://ics.uci.edu/admin/dashboard"
        self.assertTrue(is_trap(trap_url))

        trap_url2 = "http://ics.uci.edu/calendar?do=edit"
        self.assertTrue(is_trap(trap_url2))

        safe_urls = ["https://ics.uci.edu/calendar-events-archive/", "http://ics.uci.edu/faculty/baldwin", "https://wiki.ics.uci.edu/doku.php/wiki:dokuwiki", "https://www.stat.uci.edu/chairs-message"]

        for url in safe_urls: 
            self.assertFalse(is_trap(url))

if __name__ == "__main__":
    unittest.main()
