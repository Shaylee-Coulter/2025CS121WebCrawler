import os
import sqlite3
import atexit
import signal
from threading import RLock
from queue import Queue, Empty
from utils import get_logger, get_urlhash, normalize
from scraper import is_valid


class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = Queue()
        self.lock = RLock()
        
        # Tracking for periodic logging
        self._urls_processed = 0
        self._log_interval = 100  # Log every N URLs
        
        db_file = self.config.save_file + ".db"
        
        if not os.path.exists(db_file) and not restart:
            self.logger.info(
                f"Did not find save file {db_file}, starting from seed.")
        elif os.path.exists(db_file) and restart:
            self.logger.info(f"Found save file {db_file}, deleting it.")
            os.remove(db_file)
        
        # Initialize database
        self._init_db(db_file)
        
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            self._parse_save_file()
            if self._get_total_count() == 0:
                for url in self.config.seed_urls:
                    self.add_url(url)
        
        # Register cleanup handlers to run log_final_stats on exit
        atexit.register(self.log_final_stats)
        
        # Handle Ctrl+C and termination signals
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle interrupt signals (Ctrl+C, kill, etc.)."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.log_final_stats()
        exit(0)
    
    def _init_db(self, db_file):
        """Initialize SQLite database with thread-safe connection."""
        conn = sqlite3.connect(db_file, check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                urlhash TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
        
        self.db_file = db_file
    
    def _get_connection(self):
        """Get a thread-safe connection."""
        return sqlite3.connect(self.db_file, check_same_thread=False)
    
    def _parse_save_file(self):
        """Load unfinished URLs from database into queue."""
        with self.lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT url FROM urls WHERE completed = 0"
                )
                
                tbd_count = 0
                for (url,) in cursor:
                    if is_valid(url):
                        self.to_be_downloaded.put(url)
                        tbd_count += 1
                
                total_count = self._get_total_count_with_conn(conn)
                
                self.logger.info(
                    f"Found {tbd_count} urls to be downloaded from "
                    f"{total_count} total urls discovered.")
            finally:
                conn.close()
    
    def _get_total_count(self):
        """Get total number of URLs in database."""
        conn = self._get_connection()
        try:
            return self._get_total_count_with_conn(conn)
        finally:
            conn.close()
    
    def _get_total_count_with_conn(self, conn):
        """Helper to get count with existing connection."""
        cursor = conn.execute("SELECT COUNT(*) FROM urls")
        return cursor.fetchone()[0]
    
    def _get_completed_count(self, conn=None):
        """Get number of completed URLs."""
        if conn is None:
            conn = self._get_connection()
            close_conn = True
        else:
            close_conn = False
        
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM urls WHERE completed = 1")
            return cursor.fetchone()[0]
        finally:
            if close_conn:
                conn.close()
    
    def get_frontier_stats(self):
        """Get current frontier statistics (thread-safe)."""
        with self.lock:
            conn = self._get_connection()
            try:
                total = self._get_total_count_with_conn(conn)
                completed = self._get_completed_count(conn)
                in_queue = self.to_be_downloaded.qsize()
                pending = total - completed
                
                return {
                    'total_discovered': total,
                    'completed': completed,
                    'in_queue': in_queue,
                    'pending': pending
                }
            finally:
                conn.close()
    
    def get_tbd_url(self):
        """Get next URL to download (thread-safe)."""
        try:
            url = self.to_be_downloaded.get_nowait()
            
            # Log frontier statistics periodically
            with self.lock:
                self._urls_processed += 1
                
                # Log every N URLs
                if self._urls_processed % self._log_interval == 0:
                    stats = self.get_frontier_stats()
                    self.logger.info(
                        f"Frontier Stats - "
                        f"Total: {stats['total_discovered']}, "
                        f"Completed: {stats['completed']}, "
                        f"In Queue: {stats['in_queue']}, "
                        f"Pending: {stats['pending']}"
                    )
            
            return url
        except Empty:
            # Log final stats when frontier is empty
            stats = self.get_frontier_stats()
            self.logger.info(
                f"Frontier EMPTY - "
                f"Total: {stats['total_discovered']}, "
                f"Completed: {stats['completed']}"
            )
            return None
    
    def add_url(self, url):
        """Add URL to frontier if not already seen."""
        url = normalize(url)
        urlhash = get_urlhash(url)
        
        with self.lock:
            conn = self._get_connection()
            try:
                # Try to insert; if urlhash exists, this will fail silently
                cursor = conn.execute(
                    "INSERT OR IGNORE INTO urls (urlhash, url, completed) "
                    "VALUES (?, ?, 0)",
                    (urlhash, url)
                )
                
                # If row was inserted, add to queue
                if cursor.rowcount > 0:
                    self.to_be_downloaded.put(url)
                
                conn.commit()
            except sqlite3.Error as e:
                self.logger.error(f"Database error adding {url}: {e}")
                conn.rollback()
            finally:
                conn.close()
    
    def mark_url_complete(self, url):
        """Mark URL as completed."""
        urlhash = get_urlhash(url)
        
        with self.lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT urlhash FROM urls WHERE urlhash = ?",
                    (urlhash,)
                )
                
                if cursor.fetchone() is None:
                    self.logger.error(
                        f"Completed url {url}, but have not seen it before.")
                
                conn.execute(
                    "UPDATE urls SET completed = 1 WHERE urlhash = ?",
                    (urlhash,)
                )
                conn.commit()
            except sqlite3.Error as e:
                self.logger.error(f"Database error marking {url} complete: {e}")
                conn.rollback()
            finally:
                conn.close()
    
    def log_final_stats(self):
        """Log final frontier statistics (call when crawl is done)."""
        stats = self.get_frontier_stats()
        self.logger.info("=" * 70)
        self.logger.info("FRONTIER FINAL STATISTICS")
        self.logger.info("=" * 70)
        self.logger.info(f"Total URLs discovered: {stats['total_discovered']}")
        self.logger.info(f"URLs completed: {stats['completed']}")
        self.logger.info(f"URLs remaining in queue: {stats['in_queue']}")
        self.logger.info(f"URLs pending (not completed): {stats['pending']}")
        self.logger.info("=" * 70)