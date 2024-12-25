import hashlib
import os
import sqlite3
import time
import urllib.robotparser
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import requests
import schedule
from bs4 import BeautifulSoup

MAX_WORKERS = 5
MAX_PAGES = 10000
DB_NAME = "geeksforgeeks_crawler.db"
USER_AGENT = "DevsOnDeckCrawler (https://github.com/devsondeck)"
DOMAIN = "geeksforgeeks.org"


def init_db():
    """
    Initialize the SQLite database with normalized tables.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    print("Initializing database...")

    cur.execute('''CREATE TABLE IF NOT EXISTS content (
                        id INTEGER PRIMARY KEY,
                        content TEXT NOT NULL,
                        content_hash TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )''')
    print("Table 'content' is ready.")

    cur.execute('''CREATE TABLE IF NOT EXISTS urls (
                        id INTEGER PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        content_id INTEGER,
                        status TEXT,
                        last_attempted DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (content_id) REFERENCES content(id)
                    )''')
    print("Table 'urls' is ready.")

    conn.commit()
    conn.close()
    print("Database initialization complete.")


def save_content(content):
    """
    Save or update content in the database and return its ID.
    """
    content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()

    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()

        cur.execute("SELECT id FROM content WHERE content_hash = ?", (content_hash,))
        content_record = cur.fetchone()

        if content_record:
            content_id = content_record[0]
            print(f"Content already exists with ID: {content_id}")
        else:
            cur.execute('''
                INSERT INTO content (content, content_hash)
                VALUES (?, ?)
            ''', (content, content_hash))
            content_id = cur.lastrowid
            print(f"New content saved with ID: {content_id}")

        conn.commit()
        return content_id
    except sqlite3.Error as err:
        print(f"Database error: {err}")
    finally:
        conn.close()


def save_url(url, content, status):
    """
    Save or update the URL record with its associated content and status.
    If content has changed for an existing URL, update the corresponding content.
    """
    content_id = save_content(content)

    try:
        conn = sqlite3.connect(DB_NAME)
        cur = conn.cursor()

        cur.execute("SELECT id, content_id FROM urls WHERE url = ?", (url,))
        url_record = cur.fetchone()

        if url_record:
            url_id, existing_content_id = url_record
            if existing_content_id != content_id:
                cur.execute('''
                    UPDATE urls
                    SET content_id = ?, status = ?, last_attempted = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (content_id, status, url_id))
                print(f"Updated URL: {url} with new content ID: {content_id}")
            else:
                cur.execute('''
                    UPDATE urls
                    SET last_attempted = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (url_id,))
                print(f"Timestamp updated for URL: {url}")
        else:
            cur.execute('''
                INSERT INTO urls (url, content_id, status, last_attempted)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ''', (url, content_id, status))
            print(f"New URL saved: {url} with content ID: {content_id}")

        conn.commit()
    except sqlite3.Error as err:
        print(f"Database error: {err}")
    finally:
        conn.close()


def can_crawl_url(url, robots_url):
    """
    Check if the given URL is allowed to be crawled based on robots.txt.
    """
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    can_crawl = rp.can_fetch(USER_AGENT, url)
    print(f"URL {url} crawlable: {can_crawl}")
    return can_crawl


def is_valid_domain(url, base_domain=DOMAIN):
    """
    Verify if a URL belongs to the target domain.
    """
    parsed = urlparse(url)
    is_valid = base_domain in parsed.netloc
    print(f"URL {url} valid for domain {base_domain}: {is_valid}")
    return is_valid


def crawl_page(url, visited, queue, robots_url):
    """
    Fetch the content of the given URL, save it, and extract links to crawl.
    """
    if url in visited:
        print(f"URL already visited: {url}")
        return

    if not can_crawl_url(url, robots_url):
        print(f"Disallowed by robots.txt: {url}")
        save_url(url, "", "disallowed")
        return

    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        save_url(url, response.text, "crawled")
        print(f"Successfully crawled URL: {url}")
    except requests.RequestException as err:
        print(f"Failed to fetch {url}: {err}")
        save_url(url, "", "failed")
        return

    visited.add(url)

    soup = BeautifulSoup(response.text, 'lxml')
    for tag in soup.find_all('a', href=True):
        link = urljoin(url, tag['href'])
        if is_valid_domain(link) and link not in visited and link not in queue:
            queue.append(link)
            print(f"Queued link: {link}")


def start_crawl(start_url):
    """
    Manage the crawling process with threading for concurrent crawling.
    """
    print("Starting crawl...")
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

    print("\nCrawling complete.")
    print(f"Visited {len(visited)} pages.")


def schedule_crawl():
    """
    Run the crawler periodically based on a defined schedule.
    """
    start_url = "https://www.geeksforgeeks.org"
    print("Scheduled crawl starting...")
    start_crawl(start_url)
    schedule.every(1).hour.do(lambda: print("Scheduled crawl triggered.") or start_crawl(start_url))

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    if not os.path.exists(DB_NAME):
        init_db()
    schedule_crawl()
