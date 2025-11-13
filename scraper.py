import re
import hashlib
from threading import Lock
from urllib.parse import urlparse, urljoin, urldefrag, parse_qs
from collections import defaultdict
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
from urllib.robotparser import RobotFileParser

# Thread-safe global caches with locks
_seen_simhashes = set()
_seen_checksums = set()
_robots_cache = {}
_path_counter = defaultdict(int)  # Track URL patterns per domain
_path_query_counter = defaultdict(int)  # Track same path with different queries
_cache_lock = Lock()

stemmer = PorterStemmer()

# Trap detection limits
MAX_PATH_DEPTH = 10
MAX_QUERY_PARAMS = 5  # Very strict
MAX_PATH_VISITS_PER_DOMAIN = 20  # Much more aggressive
MAX_SAME_PATH_DIFFERENT_QUERY = 5  # Max variations of same path


# ----------------------------------------------------------------------
# MAIN SCRAPER
# ----------------------------------------------------------------------
def scraper(url, resp, report):
    """
    Scrapes a page with trap detection:
    - robots.txt verification
    - trap detection (calendar, query params, path depth)
    - duplicate content filtering
    - extract text + stem
    - update report
    - extract outgoing links
    """

    # --------------------- robots.txt check ---------------------
    if not robots_allowed(url):
        return []

    # --------------------- bad response -------------------------
    if resp.status != 200 or resp.raw_response is None:
        return []

    # --------------------- extract text --------------------------
    try:
        text = extract_visible_text(resp.raw_response.content)
    except Exception:
        return []
    
    if not text.strip() or len(text) < 100:  # Minimum content threshold
        return []

    # ------------------ stemming + tokenizing --------------------
    words = tokenize_and_stem(text)
    
    if len(words) < 50:  # Avoid low-content pages
        return []

    # ------------------ duplicate detection ----------------------
    with _cache_lock:
        sim = compute_simhash(words)
        chk = compute_checksum(text)

        if sim in _seen_simhashes or chk in _seen_checksums:
            return []  # near-duplicate page

        # mark as seen
        _seen_simhashes.add(sim)
        _seen_checksums.add(chk)

    # ------------------ update report ----------------------------
    report.process_page(url, words)

    # ------------------ extract links ----------------------------
    links = extract_next_links(url, resp)
    valid_links = []
    
    for link in links:
        normalized = normalize_url(link)
        if normalized and is_valid(normalized) and not is_trap(normalized):
            valid_links.append(normalized)
    
    return valid_links


# ----------------------------------------------------------------------
# URL NORMALIZATION
# ----------------------------------------------------------------------
def normalize_url(url):
    """
    Normalize URL to avoid duplicates:
    - Remove fragments (#)
    - Lowercase scheme and domain
    - Add trailing slash to domain-only URLs
    - Sort query parameters
    """
    try:
        # Remove fragment
        url, _ = urldefrag(url)
        
        parsed = urlparse(url)
        
        # Lowercase scheme and netloc
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path
        
        # Add trailing slash if path is empty
        if not path:
            path = "/"
        
        # Sort query parameters for consistency
        query = parsed.query
        if query:
            params = parse_qs(query, keep_blank_values=True)
            sorted_params = sorted(params.items())
            query = "&".join(f"{k}={v[0]}" for k, v in sorted_params)
        
        # Rebuild URL
        from urllib.parse import urlunparse
        normalized = urlunparse((scheme, netloc, path, "", query, ""))
        
        return normalized
    except Exception:
        return None


# ----------------------------------------------------------------------
# TRAP DETECTION
# ----------------------------------------------------------------------
def is_trap(url):
    """
    Detect common crawler traps:
    1. ANY query parameters (reject all - safest approach)
    2. Deep path nesting
    3. Calendar/date patterns
    4. Repetitive paths
    5. Too many visits to similar paths on same domain
    """
    try:
        parsed = urlparse(url)
        path = parsed.path
        query = parsed.query
        domain = parsed.netloc
        
        # 1. SIMPLE: Reject ALL query parameters (most effective trap prevention)
        if query:
            return True
        
        # 2. Path depth check
        path_parts = [p for p in path.split("/") if p]
        if len(path_parts) > MAX_PATH_DEPTH:
            return True
        
        # 3. Calendar/date trap detection
        # Match patterns like /2024/11/08 or /events/2024/11/08
        date_pattern = r"/\d{4}(/\d{1,2}){0,2}"
        if re.search(date_pattern, path):
            return True
        
        # 4. Repetitive path segments (e.g., /a/b/a/b)
        if len(path_parts) >= 4:
            for i in range(len(path_parts) - 1):
                if path_parts[i] == path_parts[i + 1]:
                    return True
        
        # 5. Track visits per domain/path pattern (aggressive limit)
        # Create a simplified path pattern (first 2 segments only)
        path_pattern = "/".join(path_parts[:2]) if len(path_parts) >= 2 else path
        pattern_key = f"{domain}:{path_pattern}"
        
        with _cache_lock:
            _path_counter[pattern_key] += 1
            if _path_counter[pattern_key] > MAX_PATH_VISITS_PER_DOMAIN:
                return True
        
        return False
        
    except Exception:
        return True  # If we can't parse it, treat as trap


# ----------------------------------------------------------------------
# LINK EXTRACTION
# ----------------------------------------------------------------------
def extract_next_links(url, resp):
    if resp.raw_response is None:
        return []

    try:
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    except Exception:
        return []
    
    out = []

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith(("javascript:", "mailto:", "tel:", "data:")):
            continue
        
        try:
            abs_url = urljoin(url, href)
            out.append(abs_url)
        except Exception:
            continue

    return out


# ----------------------------------------------------------------------
# TEXT EXTRACTION + STEMMING
# ----------------------------------------------------------------------
def extract_visible_text(content):
    soup = BeautifulSoup(content, "html.parser")
    
    # Remove script, style, and other non-content tags
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()
    
    return soup.get_text(separator=" ", strip=True)


def tokenize_and_stem(text):
    tokens = re.findall(r"[a-zA-Z]+", text.lower())
    stemmed = [stemmer.stem(t) for t in tokens if len(t) > 2]  # Skip very short words
    return stemmed


# ----------------------------------------------------------------------
# ROBOTS.TXT HANDLING
# ----------------------------------------------------------------------
def robots_allowed(url):
    """
    Returns True if URL is permitted by robots.txt.
    Caches RobotFileParser per domain in _robots_cache.
    """
    try:
        parsed = urlparse(url)
        domain = parsed.scheme + "://" + parsed.netloc

        with _cache_lock:
            if domain not in _robots_cache:
                rp = RobotFileParser()
                rp.set_url(domain + "/robots.txt")
                try:
                    rp.read()
                    _robots_cache[domain] = rp
                except Exception:
                    # If robots.txt fails, allow by default
                    _robots_cache[domain] = None

            rp = _robots_cache[domain]
        
        if rp is None:
            return True  # allow by fallback

        return rp.can_fetch("*", url)
    
    except Exception:
        return True  # Allow on error


# ----------------------------------------------------------------------
# DUPLICATE DETECTION 
# ----------------------------------------------------------------------
def compute_simhash(words):
    joined = " ".join(sorted(words))  # Sort for consistency
    h = hashlib.sha256(joined.encode("utf-8")).hexdigest()
    return int(h[:16], 16)


def compute_checksum(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# ----------------------------------------------------------------------
# URL VALIDITY FILTER 
# ----------------------------------------------------------------------
def is_valid(url):
    """
    Check if URL is valid for crawling.
    Only allow ICS-related UCI domains.
    """
    try:
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        # Disallowed extensions
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            r"|png|tiff?|mid|mp2|mp3|mp4"
            r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            r"|epub|dll|cnf|tgz|sha1"
            r"|thmx|mso|arff|rtf|jar|csv"
            r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            parsed.path.lower(),
        ):
            return False

        hostname = parsed.hostname
        if not hostname:
            return False
            
        hostname = hostname.lower()

        # FIXED: Only allow ICS-related domains
        # Check specific domains first
        allowed_domains = {
            "ics.uci.edu",
            "cs.uci.edu",
            "informatics.uci.edu",
            "stat.uci.edu",
        }
        
        # Exact match
        if hostname in allowed_domains:
            return True
        
        # make sure its in allowed domains
        for domain in allowed_domains:
            if hostname.endswith("." + domain):
                return True
        
        return False

    except Exception:
        return False