# scraper.py
# Production-ready version for ICS.UCI.EDU domain crawling
# All critical issues fixed

import re
import hashlib
import time
import os
from threading import Lock
from urllib.parse import (
    urlparse,
    urljoin,
    urldefrag,
    parse_qs,
    parse_qsl,
    urlunparse,
)
from collections import defaultdict, deque
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from trap import is_trap

# ----------------------------
# Configurable limits / params
# ----------------------------
MAX_SIMHASH_CACHE = 200_000
MAX_CHECKSUM_CACHE = 200_000
SIMHASH_EVICT_BATCH = 10_000
CHECKSUM_EVICT_BATCH = 10_000
MAX_ROBOTS_CACHE_AGE = 24 * 3600  # 24 hours

# Min content thresholds
MIN_CHARS = 75  # Increased from 60
MIN_TOKENS = 25  # Increased from 15

# Simhash Hamming distance threshold
SIMHASH_THRESHOLD = 3  # Allow small differences

# ----------------------------
# Global thread-safe caches
# ----------------------------
_cache_lock = Lock()

_seen_simhashes = deque(maxlen=MAX_SIMHASH_CACHE)  # Auto-bounded
_seen_simhash_set = set()

_seen_checksums = deque(maxlen=MAX_CHECKSUM_CACHE)  # Auto-bounded
_seen_checksum_set = set()

_robots_cache = {}
_robots_cache_time = {}

_calendar_counter = defaultdict(int)
_repetition_counter = defaultdict(int)
_path_query_counter = defaultdict(lambda: defaultdict(int))  # Simple counter

# Load stopwords (optional)
STOPWORDS = set()
try:
    if os.path.exists("stopwords.txt"):
        with open("stopwords.txt", "r", encoding="utf-8") as f:
            STOPWORDS = set(line.strip().lower() for line in f if line.strip())
except Exception:
    pass

# ----------------------------
# Helper: bounded cache utilities
# ----------------------------
def _evict_if_needed():
    """Sync sets with deques when deque auto-evicts."""
    with _cache_lock:
        # Deques auto-evict, so we rebuild sets from current deque contents
        if len(_seen_simhash_set) > len(_seen_simhashes) * 1.2:
            _seen_simhash_set.clear()
            _seen_simhash_set.update(_seen_simhashes)
        
        if len(_seen_checksum_set) > len(_seen_checksums) * 1.2:
            _seen_checksum_set.clear()
            _seen_checksum_set.update(_seen_checksums)

# ----------------------------
# Main scraper entrypoint
# ----------------------------
def scraper(url, resp, report):
    """
    Main scraping function called by workers.
    Returns list of normalized, valid, non-trap absolute URLs to enqueue next.
    """
    # robots guard
    if not robots_allowed(url):
        return []

    # basic response checks
    if resp is None or getattr(resp, "status", None) != 200 or getattr(resp, "raw_response", None) is None:
        return []

    # content-type guard
    content_type = (resp.raw_response.headers.get("content-type", "") or "").lower()
    content_type_main = content_type.split(";")[0].strip()
    
    # Allow HTML-like types
    allowed_types = ("text/html", "application/xhtml+xml", "text/plain")
    if content_type_main:
        if not any(content_type_main == t or content_type_main.startswith(t + ";") for t in allowed_types):
            # Block non-HTML types
            return []

    # try to extract text
    try:
        raw = resp.raw_response.content
        # Binary check
        if b"\x00" in raw[:8192]:
            return []

        text = extract_visible_text(raw)
    except Exception:
        return []

    if not text or len(text) < MIN_CHARS:
        return []

    tokens = tokenize(text)
    if len(tokens) < MIN_TOKENS:
        return []

    # Duplicate detection with near-duplicate checking
    checksum = compute_checksum(text)
    simhash = compute_simhash(tokens)

    with _cache_lock:
        _evict_if_needed()

        # Exact duplicate check
        if checksum in _seen_checksum_set:
            return []

        # Near-duplicate check: Hamming distance on recent simhashes
        # Check last 1000 only to keep it fast
        if simhash in _seen_simhash_set:
            return []
        
        # Check Hamming distance against recent entries (bounded check)
        recent_count = min(1000, len(_seen_simhashes))
        for i in range(recent_count):
            try:
                old_simhash = _seen_simhashes[-(i+1)]
                if hamming_distance(simhash, old_simhash) <= SIMHASH_THRESHOLD:
                    return []
            except IndexError:
                break

        # Register both
        _seen_checksums.append(checksum)
        _seen_checksum_set.add(checksum)

        _seen_simhashes.append(simhash)
        _seen_simhash_set.add(simhash)

    # record page into report
    try:
        report.process_page(url, tokens)
    except Exception:
        pass

    # extract links and normalize
    try:
        raw_links = extract_next_links(url, resp)
    except Exception:
        raw_links = []

    out_links = []
    for link in raw_links:
        normalized = normalize_url(link)
        if not normalized:
            continue
        if not is_valid(normalized):
            continue
        if is_trap(normalized):
            continue
        out_links.append(normalized)

    return out_links

# ----------------------------
# URL normalization - FIXED
# ----------------------------
def normalize_url(url):
    """
    Normalize URL to canonical form with proper trailing slash handling.
    """
    try:
        url, _ = urldefrag(url)
        parsed = urlparse(url)

        scheme = parsed.scheme.lower() or "http"
        netloc = parsed.netloc.lower()
        path = parsed.path or ""

        # Remove duplicate slashes
        if path:
            path = re.sub(r"/{2,}", "/", path)

        # Normalize empty path to "/"
        if not path:
            path = "/"

        # Trailing slash normalization
        # Add trailing slash for directories (no extension), remove for files
        if path != "/":
            last_segment = path.split("/")[-1]
            has_extension = "." in last_segment and not last_segment.startswith(".")
            
            if has_extension:
                # File: remove trailing slash
                path = path.rstrip("/")
            else:
                # Directory: ensure trailing slash
                if not path.endswith("/"):
                    path = path + "/"

        # Sort query parameters
        query = parsed.query or ""
        if query:
            pairs = parse_qsl(query, keep_blank_values=True)
            pairs.sort(key=lambda kv: (kv[0], kv[1]))
            query = "&".join(f"{k}={v}" for k, v in pairs)

        normalized = urlunparse((scheme, netloc, path, "", query, ""))
        return normalized
    except Exception:
        return None


# ----------------------------
# Link extraction
# ----------------------------
def extract_next_links(base_url, resp):
    """Return absolute links found on page."""
    if resp is None or getattr(resp, "raw_response", None) is None:
        return []

    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    except Exception:
        return []

    actual_url = getattr(resp, "url", base_url)
    
    out = []
    for tag in soup.find_all("a", href=True):
        href = tag.get("href", "").strip()
        if not href:
            continue
        if href.startswith(("javascript:", "mailto:", "tel:", "data:", "#")):
            continue
        try:
            abs_url = urljoin(actual_url, href)
            abs_url, _ = urldefrag(abs_url)
            out.append(abs_url)
        except Exception:
            continue
    return out

# ----------------------------
# Text extraction + tokenization
# ----------------------------
def extract_visible_text(content_bytes):
    """Return cleaned, main visible text from HTML bytes."""
    soup = BeautifulSoup(content_bytes, "html.parser")

    # Remove noise
    for tag in soup(["script", "style", "noscript", "iframe", "object", "embed", "svg", "canvas", "meta", "link"]):
        tag.decompose()

    # Prefer main content
    main_content = (
        soup.find("main")
        or soup.find("article")
        or soup.find("div", class_=re.compile(r"(content|main|body|post|article)", re.I))
        or soup.find("body")
        or soup
    )

    text = main_content.get_text(separator=" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def tokenize(text):
    """Tokenize and filter stopwords."""
    tokens = re.findall(r"[a-zA-Z]+", text.lower())
    out = []
    for t in tokens:
        if len(t) <= 2:
            continue
        if t in STOPWORDS:
            continue
        if len(t) > 50:
            continue
        out.append(t)
    return out

# ----------------------------
# Robots.txt handling - FIXED
# ----------------------------
def robots_allowed(url):
    """
    Return True if allowed by robots.txt.
    """
    try:
        parsed = urlparse(url)
        domain_base = parsed.scheme + "://" + parsed.netloc

        current = time.time()
        with _cache_lock:
            # Check if cached and not expired
            if domain_base in _robots_cache:
                age = current - _robots_cache_time.get(domain_base, 0)
                if age > MAX_ROBOTS_CACHE_AGE:
                    # Expired, remove
                    del _robots_cache[domain_base]
                    del _robots_cache_time[domain_base]

            # Fetch if not cached
            if domain_base not in _robots_cache:
                rp = RobotFileParser()
                rp.set_url(domain_base + "/robots.txt")
                try:
                    rp.read()
                    _robots_cache[domain_base] = rp
                except Exception:
                    # Failed - cache None and mark as fetched
                    _robots_cache[domain_base] = None
                
                # Always set cache time on first fetch
                _robots_cache_time[domain_base] = current

            rp = _robots_cache[domain_base]

        # If None (fetch failed), allow crawling
        if rp is None:
            return True

        return rp.can_fetch("*", url)

    except Exception:
        return True

# ----------------------------
# Duplicate detection
# ----------------------------
def compute_checksum(text):
    """Normalized MD5 checksum for exact-duplicate detection."""
    normalized = re.sub(r"\s+", " ", text.lower().strip())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def compute_simhash(tokens):
    """
    Compute a 64-bit simhash from tokens with frequency weighting.
    """
    if not tokens:
        return 0

    v = [0] * 64
    freq = defaultdict(int)
    for t in tokens:
        freq[t] += 1

    for token, count in freq.items():
        h = hashlib.sha256(token.encode("utf-8")).digest()
        hv = int.from_bytes(h[:8], "big")
        for i in range(64):
            bit = (hv >> i) & 1
            v[i] += count if bit else -count

    result = 0
    for i in range(64):
        if v[i] > 0:
            result |= (1 << i)
    return result


def hamming_distance(x, y):
    """Calculate Hamming distance between two integers."""
    return bin(x ^ y).count('1')

# ----------------------------
# URL validity filter
# ----------------------------
def is_valid(url):
    """Return True if URL is valid and in allowed domains."""
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        if scheme not in {"http", "https"}:
            return False

        hostname = parsed.hostname
        if not hostname:
            return False
        hostname = hostname.lower()

        # Allowed domains
        allowed_domains = {
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        }

        # Check domain
        if hostname not in allowed_domains:
            if not any(hostname.endswith("." + d) for d in allowed_domains):
                return False

        # Check extensions
        path = parsed.path or ""
        last_segment = path.split("/")[-1].lower()
        if "." in last_segment:
            ext = last_segment.split(".")[-1]
            blocked = {
                "css", "js", "bmp", "gif", "jpg", "jpeg", "ico", "png", "tif", "tiff",
                "mid", "mp2", "mp3", "mp4", "wav", "avi", "mov", "mpeg", "ram", "m4v", "mkv",
                "ogg", "ogv", "pdf", "ps", "eps", "tex", "ppt", "pptx", "doc", "docx",
                "xls", "xlsx", "data", "dat", "exe", "bz2", "tar", "msi", "bin", "7z",
                "psd", "dmg", "iso", "epub", "dll", "cnf", "tgz", "sha1", "thmx", "mso",
                "arff", "rtf", "jar", "csv", "rm", "smil", "wmv", "swf", "wma", "zip", "rar",
                "gz", "mpg", "flv", "webm", "ttf", "otf", "woff", "woff2", "eot", "sql", "db",
                "sqlite", "mdb", "log", "bak", "tmp", "temp", "cache", "class", "pyc", "o", "so"
            }
            if ext in blocked:
                return False

        return True
    except Exception:
        return False