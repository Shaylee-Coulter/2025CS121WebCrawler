import re
from urllib.parse import urlparse, parse_qs
from threading import Lock
from collections import defaultdict

# External config constants (import from config.py if modularized)
MAX_URL_LENGTH = 2000
MAX_PATH_DEPTH = 40
MAX_QUERY_PARAMS = 25
MAX_CALENDAR_PAGES_PER_DOMAIN = 250
MAX_REPETITION_ALLOWED = 12
MAX_PATH_QUERIES = 50

# Shared counters and lock
_cache_lock = Lock()
_calendar_counter = defaultdict(int)
_repetition_counter = defaultdict(int)
_path_query_counter = defaultdict(lambda: defaultdict(int))

def is_trap(url):
    """Return True if URL is considered a trap and should be blocked."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        path = parsed.path or "/"
        query = parsed.query or ""

        return any([
            _is_too_long(url),
            _has_excessive_path_depth(path),
            _has_admin_segments(path),
            _has_repetitive_patterns(path, domain),
            _is_calendar_page(path, domain),
            _is_path_query_overused(domain, path),
            _has_trap_query_params(path, query),
        ])
    except Exception:
        return True

def _is_too_long(url):
    return len(url) > MAX_URL_LENGTH

def _has_excessive_path_depth(path):
    return len([p for p in path.split("/") if p]) > MAX_PATH_DEPTH

def _has_admin_segments(path):
    path_lower = path.lower()
    admin_prefixes = ["/admin/", "/login/", "/logout/", "/.git/", "/.env", "/cgi-bin/"]
    admin_keywords = {"wp-admin", "phpmyadmin", "administrator", "backend"}
    return any(path_lower.startswith(p) for p in admin_prefixes) or \
           any(seg.lower() in admin_keywords for seg in path.split("/")[:3])

def _has_repetitive_patterns(path, domain):
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 4:
        for i in range(len(parts) - 3):
            if parts[i] == parts[i+2] and parts[i+1] == parts[i+3]:
                with _cache_lock:
                    _repetition_counter[domain] += 1
                    if _repetition_counter[domain] > MAX_REPETITION_ALLOWED:
                        return True
    return False

def _is_calendar_page(path, domain):
    if re.search(r'/\d{4}(/\d{1,2}(/\d{1,2})?)?/?$', path):
        with _cache_lock:
            _calendar_counter[domain] += 1
            if _calendar_counter[domain] > MAX_CALENDAR_PAGES_PER_DOMAIN:
                return True
    return False

def _is_path_query_overused(domain, path):
    with _cache_lock:
        key = path.lower()
        _path_query_counter[domain][key] += 1
        if _path_query_counter[domain][key] > MAX_PATH_QUERIES:
            return True
    return False

def _has_trap_query_params(path, query):
    if not query:
        return False

    params = parse_qs(query, keep_blank_values=True)
    keys = [k.lower() for k in params]

    trap_keys = {"sessionid", "sid", "token", "auth", "key", "print", "email"}
    if any(k in trap_keys for k in keys):
        return True

    if 'doku.php' in path.lower():
        doku_keys = {"do", "tab_files", "tab_details", "image", "ns", "rev", "search"}
        if sum(1 for k in keys if k in doku_keys) >= 2:
            return True

    trap_actions = {"edit", "history", "diff", "revisions", "admin", "login", "register", "delete"}
    for k, vals in params.items():
        if k.lower() in {"action", "do", "cmd"} and any(v.lower() in trap_actions for v in vals):
            return True

    for k, vals in params.items():
        if k.lower() in {"page", "p", "offset", "start"}:
            for v in vals:
                try:
                    if int(v) > 500:
                        return True
                except:
                    continue

    if len(params) > MAX_QUERY_PARAMS or any(len(v) > 20 for v in params.values()):
        return True

    return False
