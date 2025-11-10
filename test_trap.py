import unittest
from trap import is_trap, _is_too_long, _has_excessive_path_depth, _has_admin_segments, _has_repetitive_patterns, _is_calendar_page, _is_path_query_overused, _has_trap_query_params

class TrapDetectionTests(unittest.TestCase):

    def test_is_too_long(self):
        long_url = "http://example.com/" + "a" * 2100
        self.assertTrue(_is_too_long(long_url))
        short_url = "http://example.com/page"
        self.assertFalse(_is_too_long(short_url))

    def test_excessive_path_depth(self):
        deep_path = "/" + "/".join(["dir"] * 50)
        self.assertTrue(_has_excessive_path_depth(deep_path))
        shallow_path = "/a/b/c"
        self.assertFalse(_has_excessive_path_depth(shallow_path))

    def test_admin_segments(self):
        self.assertTrue(_has_admin_segments("/admin/dashboard"))
        self.assertTrue(_has_admin_segments("/wp-admin/settings"))
        self.assertFalse(_has_admin_segments("/about/team"))

    def test_repetitive_patterns(self):
        domain = "test.com"
        repetitive_path = "/a/b/a/b/a/b/"
        for _ in range(13):
            _has_repetitive_patterns(repetitive_path, domain)
        self.assertTrue(_has_repetitive_patterns(repetitive_path, domain))

    def test_calendar_page(self):
        domain = "calendar.com"
        calendar_path = "/2023/12/25/"
        for _ in range(251):
            _is_calendar_page(calendar_path, domain)
        self.assertTrue(_is_calendar_page(calendar_path, domain))

    def test_path_query_overused(self):
        domain = "overused.com"
        path = "/repeated"
        for _ in range(51):
            _is_path_query_overused(domain, path)
        self.assertTrue(_is_path_query_overused(domain, path))

    def test_trap_query_params(self):
        trap_url = "/page?sessionid=abc123"
        self.assertTrue(_has_trap_query_params("/page", "sessionid=abc123"))

        pagination_url = "/page?p=9999"
        self.assertTrue(_has_trap_query_params("/page", "p=9999"))

        safe_url = "/page?view=summary"
        self.assertFalse(_has_trap_query_params("/page", "view=summary"))

    def test_is_trap_composite(self):
        trap_url = "http://example.com/admin/settings"
        self.assertTrue(is_trap(trap_url))

        safe_url = "http://ics.uci.edu/about"
        self.assertFalse(is_trap(safe_url))


if __name__ == "__main__":
    unittest.main()
