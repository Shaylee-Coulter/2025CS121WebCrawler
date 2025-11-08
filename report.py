import re
from collections import Counter, defaultdict
from urllib.parse import urlparse
from threading import RLock
from utils import get_logger

# Standard English stopwords list.
STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any",
    "are","aren't","as","at","be","because","been","before","being","below",
    "between","both","but","by","can't","cannot","could","couldn't","did",
    "didn't","do","does","doesn't","doing","don't","down","during","each",
    "few","for","from","further","had","hadn't","has","hasn't","have",
    "haven't","having","he","he'd","he'll","he's","her","here","here's",
    "hers","herself","him","himself","his","how","how's","i","i'd","i'll",
    "i'm","i've","if","in","into","is","isn't","it","it's","its","itself",
    "let's","me","more","most","mustn't","my","myself","no","nor","not","of",
    "off","on","once","only","or","other","ought","our","ours","ourselves",
    "out","over","own","same","shan't","she","she'd","she'll","she's",
    "should","shouldn't","so","some","such","than","that","that's","the",
    "their","theirs","them","themselves","then","there","there's","these",
    "they","they'd","they'll","they're","they've","this","those","through",
    "to","too","under","until","up","very","was","wasn't","we","we'd","we'll",
    "we're","we've","were","weren't","what","what's","when","when's","where",
    "where's","which","while","who","who's","whom","why","why's","with",
    "won't","would","wouldn't","you","you'd","you'll","you're","you've",
    "your","yours","yourself","yourselves"
}


class Report:
    """
    Thread-safe singleton report class for web crawler statistics.
    All workers share the same instance via class-level attributes.
    """
    
    # Class-level (static) attributes shared by all instances
    _unique_urls = set()
    _longest_page_url = None
    _longest_page_wordcount = 0
    _word_counter = Counter()
    _uci_subdomains = defaultdict(int)
    _lock = RLock()
    _instance = None
    _log = None
    
    def __new__(cls):
        """Singleton pattern: ensure only one instance exists."""
        if cls._instance is None:
            cls._instance = super(Report, cls).__new__(cls)
            cls._log = get_logger("REPORT")
        return cls._instance
    
    @classmethod
    def reset(cls):
        """Reset all statistics (useful for testing or restart)."""
        with cls._lock:
            cls._unique_urls.clear()
            cls._longest_page_url = None
            cls._longest_page_wordcount = 0
            cls._word_counter.clear()
            cls._uci_subdomains.clear()

    def process_page(self, url: str, text_or_words):
        """
        Call this once per successfully scraped page.
        Thread-safe: multiple workers can call this concurrently.
        
        - url: final URL (normalized)
        - text_or_words: either raw text string OR list of pre-tokenized words
        """
        with self._lock:
            # Track unique URLs
            self._unique_urls.add(url)

            # Handle both string and list inputs
            if isinstance(text_or_words, list):
                # Already tokenized - use directly
                words = text_or_words
            else:
                # String text - tokenize it
                words = self._tokenize(text_or_words)

            # Count words in this page
            word_count = len(words)

            # Check if this is the longest page
            if word_count > self._longest_page_wordcount:
                self._longest_page_wordcount = word_count
                self._longest_page_url = url

            # Count global word frequencies
            self._word_counter.update(words)

            # Check subdomain stats for uci.edu
            self._track_uci_subdomain(url)

    def _tokenize(self, text: str):
        """Tokenizes text, removes stopwords, returns lowercase words."""
        tokens = re.findall(r"[a-zA-Z]+", text.lower())
        return [t for t in tokens if t not in STOPWORDS]

    def _track_uci_subdomain(self, url: str):
        """Counts subdomain if domain ends with 'uci.edu'."""
        parsed = urlparse(url)
        hostname = parsed.hostname or ""

        if hostname.endswith("uci.edu"):
            # example: www.ics.uci.edu
            # we want: www.ics
            sub = hostname[: -len(".uci.edu")]
            if sub == "":
                # root: uci.edu
                sub = "(root)"
            self._uci_subdomains[sub] += 1

    # ---- Final Output Methods ----

    def get_unique_page_count(self):
        """Thread-safe getter for unique page count."""
        with self._lock:
            return len(self._unique_urls)

    def get_longest_page(self):
        """Thread-safe getter for longest page info."""
        with self._lock:
            return self._longest_page_url, self._longest_page_wordcount

    def get_top_50_words(self):
        """Thread-safe getter for top 50 words."""
        with self._lock:
            return self._word_counter.most_common(50)

    def get_uci_subdomain_stats(self):
        """
        Returns list of (subdomain, count), alphabetically sorted.
        Thread-safe.
        """
        with self._lock:
            return sorted(self._uci_subdomains.items(), key=lambda x: x[0])

    def generate_report(self):
        """
        Logs the complete report.
        Thread-safe: can be called from any thread.
        """
        with self._lock:
            self._log.info("=" * 70)
            self._log.info("CRAWLER REPORT")
            self._log.info("=" * 70)
            
            self._log.info(f"Total unique pages: {len(self._unique_urls)}")
            self._log.info(f"Longest page: {self._longest_page_url} "
                          f"({self._longest_page_wordcount} words)")

            self._log.info("\nTop 50 words:")
            for word, freq in self._word_counter.most_common(50):
                self._log.info(f"  {word}: {freq}")

            self._log.info("\nUCI subdomains:")
            for sub, count in sorted(self._uci_subdomains.items(), key=lambda x: x[0]):
                self._log.info(f"  {sub}: {count}")
            
            self._log.info("=" * 70)