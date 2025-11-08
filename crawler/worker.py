from threading import Thread
from inspect import getsource
from utils.download import download
from utils import get_logger
from report import Report
import scraper
import time

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
    
    def run(self):
        
        while True:
            tbd_url = self.frontier.get_tbd_url()
            
            if tbd_url is None:
            # Wait to see if other threads add URLs
                time.sleep(5)  # Wait 5 seconds total
                tbd_url = self.frontier.get_tbd_url()
    
                if tbd_url is None:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    self.report.generate_report()
                    break
    
    # Found a URL after waiting, continue processing
                
                # Wait for other threads to potentially add URLs
                time.sleep(0.1)
                continue
            
       
            
            try:
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
            
            # Politeness delay
            time.sleep(self.config.time_delay)
        
        self.logger.info(f"Worker-{self.worker_id} shutting down.")