import hashlib
import sqlite3
import time
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import logging

import requests
from bs4 import BeautifulSoup

MAX_WORKERS = 5
MAX_PAGES = 10000
DB_NAME = "crawler.db"
USER_AGENT = "WebCrawler (https://github.com/devsondeck)"
DOMAIN = "geeksforgeeks.org"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Database Initialization
def init_db():
    """
    Initialize the SQLite database with normalized tables.
    """
    logger.info("Initializing the database.")
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS domains (
                        id INTEGER PRIMARY KEY,
                        domain_name TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS content (
                        id INTEGER PRIMARY KEY,
                        content TEXT NOT NULL,
                        content_hash TEXT UNIQUE NOT NULL,
                        cleaned_content TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS urls (
                        id INTEGER PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        content_id INTEGER,
                        domain_id INTEGER,
                        status TEXT,
                        last_attempted DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (content_id) REFERENCES content(id),
                        FOREIGN KEY (domain_id) REFERENCES domains(id)
                    )''')

    cur.execute('''CREATE TABLE IF NOT EXISTS keywords (
                        id INTEGER PRIMARY KEY,
                        keyword TEXT NOT NULL,
                        content_id INTEGER NOT NULL,
                        FOREIGN KEY (content_id) REFERENCES content(id)
                    )''')

    conn.commit()
    conn.close()
    logger.info("Database initialization complete.")


# Utility Functions
def save_domain(domain_name):
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute('INSERT OR IGNORE INTO domains (domain_name) VALUES (?)', (domain_name,))
        conn.commit()
        domain_id = cur.execute('SELECT id FROM domains WHERE domain_name = ?', (domain_name,)).fetchone()[0]
        logger.info(f"Domain saved: {domain_name} (ID: {domain_id})")
        return domain_id
    finally:
        conn.close()


def save_content(content, cleaned_content):
    content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()

        cur.execute("SELECT id FROM content WHERE content_hash = ?", (content_hash,))
        content_record = cur.fetchone()

        if content_record:
            content_id = content_record[0]
        else:
            cur.execute('''
                INSERT INTO content (content, content_hash, cleaned_content)
                VALUES (?, ?, ?)
            ''', (content, content_hash, cleaned_content))
            content_id = cur.lastrowid

        conn.commit()
        logger.info(f"Content saved with ID: {content_id}")
        return content_id
    finally:
        conn.close()


def save_keywords(content_id, keywords):
    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        for keyword in keywords:
            cur.execute('INSERT INTO keywords (keyword, content_id) VALUES (?, ?)', (keyword, content_id))
        conn.commit()
        logger.info(f"Keywords saved for content ID: {content_id}")
    finally:
        conn.close()


def save_url(url, content, status):
    domain_name = urlparse(url).netloc
    domain_id = save_domain(domain_name)

    content_id = None
    if content:
        cleaned_content = extract_main_content(content)
        keywords = extract_keywords(cleaned_content)
        content_id = save_content(content, cleaned_content)
        save_keywords(content_id, keywords)

    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()
        cur.execute('''
            INSERT OR REPLACE INTO urls (url, content_id, domain_id, status, last_attempted)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (url, content_id, domain_id, status))
        conn.commit()
        logger.info(f"URL saved: {url} with status {status}")
    finally:
        conn.close()


def can_crawl_url(url, robots_url):
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    allowed = rp.can_fetch(USER_AGENT, url)
    logger.debug(f"Can crawl {url}: {allowed}")
    return allowed


def extract_main_content(html):
    soup = BeautifulSoup(html, 'lxml')
    for script_or_style in soup(['script', 'style']):
        script_or_style.decompose()
    return soup.get_text(strip=True)


def extract_keywords(text):
    words = text.split()
    return list(set([word.lower() for word in words if len(word) > 3]))


# Crawling Functions
def crawl_page(url, visited, queue, robots_url):
    if url in visited:
        logger.debug(f"URL already visited: {url}")
        return

    if not can_crawl_url(url, robots_url):
        save_url(url, "", "disallowed")
        return

    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        save_url(url, response.text, "crawled")
    except requests.RequestException as e:
        save_url(url, "", "failed")
        logger.error(f"Failed to crawl {url}: {e}")
        return

    visited.add(url)

    soup = BeautifulSoup(response.text, 'lxml')
    for tag in soup.find_all('a', href=True):
        link = urljoin(url, tag['href'])
        if DOMAIN in urlparse(link).netloc and link not in visited and link not in queue:
            queue.append(link)


def start_crawl(start_url):
    init_db()
    visited = set()
    queue = [start_url]
    robots_url = urljoin(start_url, "/robots.txt")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        while queue and len(visited) < MAX_PAGES:
            futures = []
            for _ in range(min(MAX_WORKERS, len(queue))):
                current_url = queue.pop(0)
                futures.append(executor.submit(crawl_page, current_url, visited, queue, robots_url))

            for future in futures:
                future.result()

            time.sleep(1)
            logger.info(f"Visited {len(visited)} URLs. Queue length: {len(queue)}")


if __name__ == "__main__":
    logger.info("Starting the crawler.")
    start_crawl("https://geeksforgeeks.org")
