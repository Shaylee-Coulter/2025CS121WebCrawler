from threading import Thread, Lock
from inspect import getsource
from utils.download import download
from utils import get_logger
from report import Report
from urllib.parse import urlparse
import scraper
import time
from collections import defaultdict

# Global per-domain politeness tracking
_domain_locks = defaultdict(Lock)
_last_access_time = defaultdict(float)
_politeness_lock = Lock()

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.report = Report()
        self.config = config
        self.frontier = frontier
        self.worker_id = worker_id

        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, \
            "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, \
            "Do not use urllib.request in scraper.py"
        
        super().__init__(daemon=True)
    
    def _get_domain(self, url):
        """Extract domain from URL for politeness tracking."""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return "unknown"
    
    def _wait_for_politeness(self, domain):
        """
        Enforce politeness delay per domain.
        Only one worker can access a domain at a time.
        """
        with _politeness_lock:
            # Get or create lock for this domain
            domain_lock = _domain_locks[domain]
        
        # Acquire domain-specific lock (blocks if another worker is accessing this domain)
        with domain_lock:
            current_time = time.time()
            last_access = _last_access_time[domain]
            
            # Calculate time since last access to this domain
            time_since_last = current_time - last_access
            
            # If not enough time has passed, wait
            if time_since_last < self.config.time_delay:
                wait_time = self.config.time_delay - time_since_last
                time.sleep(wait_time)
            
            # Update last access time for this domain
            _last_access_time[domain] = time.time()
    
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            
            if tbd_url is None:
                # Wait to see if other threads add URLs
                time.sleep(5)
                tbd_url = self.frontier.get_tbd_url()
    
                if tbd_url is None:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
    
                # Wait for other threads to potentially add URLs
                time.sleep(0.1)
                continue
            
            try:
                # Extract domain for politeness
                domain = self._get_domain(tbd_url)
                
                # Wait for politeness (per-domain rate limiting)
                self._wait_for_politeness(domain)
                
                # Download the page
                resp = download(tbd_url, self.config, self.logger)
                self.logger.info(
                    f"Downloaded {tbd_url}, status <{resp.status}>, "
                    f"using cache {self.config.cache_server}.")
                
                # Scrape URLs from the page
                scraped_urls = scraper.scraper(tbd_url, resp, self.report)
                
                # Add discovered URLs to frontier
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
                
                # Mark as complete only after successful processing
                self.frontier.mark_url_complete(tbd_url)
                
            except Exception as e:
                # Log error but don't mark as complete so it can be retried
                self.logger.error(f"Error processing {tbd_url}: {e}", exc_info=True)
                # Optionally mark as complete anyway to avoid infinite retries:
                # self.frontier.mark_url_complete(tbd_url)
        
        self.logger.info(f"Worker-{self.worker_id} shutting down.")