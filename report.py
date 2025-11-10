import re
from collections import Counter, defaultdict
from urllib.parse import urlparse, urldefrag
from threading import RLock
from utils import get_logger
from stopword import load_stopwords

# Standard English stopwords list
STOPWORDS = load_stopwords("stopwords.txt")


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
        
        - url: final URL (should be normalized, but we ensure fragment removal)
        - text_or_words: either raw text string OR list of pre-tokenized words
        
        NOTE: Per assignment requirements, uniqueness is determined by URL
        WITHOUT fragment (e.g., http://example.com#a and http://example.com#b
        are considered the same page).
        """
        with self._lock:
            # Ensure fragment is removed (defensive programming)
            # Even though scraper should normalize, we guarantee it here
            url_no_fragment, _ = urldefrag(url)
            
            # Track unique URLs (without fragments)
            self._unique_urls.add(url_no_fragment)

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
                self._longest_page_url = url_no_fragment

            # Count global word frequencies (with filtering)
            valid_words = [w for w in words if self._is_valid_word(w)]

            # Per-page word frequency limiting
            from collections import Counter
            word_freq_this_page = Counter(valid_words)

            MAX_WORD_COUNT_PER_PAGE = 50

            for word, count in word_freq_this_page.items():
                capped_count = min(count, MAX_WORD_COUNT_PER_PAGE)
                self._word_counter[word] += capped_count

            # Check subdomain stats for uci.edu
            self._track_uci_subdomain(url_no_fragment)



    def _tokenize(self, text: str):
        """Tokenizes text, removes stopwords, returns lowercase words."""
        tokens = re.findall(r"[a-zA-Z]+", text.lower())
        return [t for t in tokens if t not in STOPWORDS]
    
    def _is_valid_word(self, word: str) -> bool:
        """
        Check if a word is valid for reporting.
        Filters out garbage tokens, repetitive characters, etc.
        """
        # Skip if too long
        if len(word) > 20:
            return False
        
        # Skip repetitive character patterns (ccc, aaaa, bbbb, abab)
        # Check if word has very low character diversity
        unique_chars = len(set(word))
        word_length = len(word)
        
        # If 3+ chars and only 1-2 unique characters, it's likely garbage
        if word_length >= 3 and unique_chars <= 2:
            return False
        
        # Check for alternating patterns (abababab)
        if word_length >= 6:
            # Check if first half equals second half (repeated pattern)
            half = word_length // 2
            if word[:half] == word[half:2*half]:
                return False
        
        return True

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
        """
        Thread-safe getter for unique page count.
        Returns count of unique URLs (fragments ignored per assignment).
        """
        with self._lock:
            return len(self._unique_urls)

    def get_longest_page(self):
        """Thread-safe getter for longest page info."""
        with self._lock:
            return self._longest_page_url, self._longest_page_wordcount

    def get_top_50_words(self):
        """
        Thread-safe getter for top 50 words.
        Returns list of (word, count) tuples.
        """
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
            self._log.info(f"  (Uniqueness determined by URL without fragment)")
            self._log.info(f"Longest page: {self._longest_page_url} "
                          f"({self._longest_page_wordcount} words)")

            self._log.info("\nTop 50 words:")
            for word, freq in self._word_counter.most_common(50):
                self._log.info(f"  {word}: {freq}")

            self._log.info("\nUCI subdomains:")
            for sub, count in sorted(self._uci_subdomains.items(), key=lambda x: x[0]):
                self._log.info(f"  {sub}.uci.edu: {count} pages")
            
            self._log.info("=" * 70)