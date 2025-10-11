"""
Note:
Some documentation and comments were written with the assistance of Google's Gemini AI model
to improve code readability and consistency.
"""
"""
A web scraper designed to extract cryptocurrency data from CoinMarketCap 
using Playwright for dynamic content loading and BeautifulSoup for HTML parsing, 
storing the results in an SQLite database.
"""
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from fake_headers import Headers
import sqlite3
import logging
import time

# Basic logging configuration setup
logging.basicConfig(level=logging.INFO)
# Database file name constant
DB_NAME = 'coins_sync.db'
# ---------- Database Functions ----------
def create_db():
    """
    Creates the SQLite database file and the 'coins' table if it does not already exist.

    The table is structured to store various cryptocurrency details.
    """
    # Establish connection to the database using a context manager
    with sqlite3.connect(DB_NAME) as db:
        # SQL command to define the table structure
        db.execute('''
            CREATE TABLE IF NOT EXISTS coins (
                rank TEXT,
                logo TEXT,
                name TEXT,
                symbol TEXT,
                price TEXT,
                change_1h TEXT,
                change_24h TEXT,
                change_7d TEXT,
                market_cap TEXT,
                volume_24h TEXT,
                circulating_supply TEXT,
                last_7d TEXT 
                )
        ''')
        # Commit changes to finalize table creation
        db.commit()


def insert_coin(db, coin):
    """
    Inserts a coin's data into the 'coins' table, or replaces it if a conflict occurs.

    :param db: The active SQLite database connection object.
    :type db: sqlite3.Connection
    :param coin: A dictionary containing the complete data for one coin.
    :type coin: dict
    :returns: None
    """
    # Execute the INSERT OR REPLACE command
    db.execute('''
        INSERT OR REPLACE INTO coins 
        (rank, logo, name, symbol, price, change_1h, change_24h, change_7d, market_cap, volume_24h, circulating_supply, last_7d)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        coin.get('rank'),
        coin.get('logo'),
        coin.get('name'),
        coin.get('symbol'),
        coin.get('price'),
        coin.get('change_1h'),
        coin.get('change_24h'),
        coin.get('change_7d'),
        coin.get('market_cap'),
        coin.get('volume_24h'),
        coin.get('circulating_supply'),
        coin.get('last_7d')
    ))
    # Commit changes to finalize the insertion
    db.commit()


# ---------- Scraper Function ----------
def coinmarketcap(url, page_index):
    """
    Scrapes cryptocurrency data from a CoinMarketCap page using Playwright and BeautifulSoup.

    :param url: The URL of the CoinMarketCap page to scrape.
    :type url: str
    :param page_index: The page number (used for logging purposes).
    :type page_index: int
    :returns: A list of dictionaries, each containing one coin's information.
    :rtype: list
    """
    # Generate random headers to simulate a real browser request
    headers = Headers().generate()
    logging.info(headers)

    # Initialize Playwright instance
    with sync_playwright() as sp:
        logging.info(f'page {page_index}: open browser')
        # Launch Chromium browser (headless=False means the browser UI is visible)
        with sp.chromium.launch(headless=False) as browser:
            logging.info(f'page {page_index}: open context')
            # Create a new context with the generated headers
            context = browser.new_context(extra_http_headers=headers)
            logging.info(f'page {page_index}: open page')
            page = context.new_page()
            logging.info(f'page {page_index}: go to page {url}')
            # Navigate to the URL, waiting until the network is idle
            response = page.goto(url, wait_until="networkidle", timeout=60000)

            # Check the HTTP response status
            if response:
                status = response.status
                logging.info(f"{url} → Status: {status}")

            # _____________ Scroll page _____________
            logging.info(f'page {page_index}: start scroll')
            # Start the scrolling process to load all data via JavaScript
            count, max_count = 0, 10
            while count < max_count:
                # Get the current scroll height
                old_height = page.evaluate("document.body.scrollHeight")
                # Simulate pressing ArrowDown to scroll
                for _ in range(10):
                    page.keyboard.press("ArrowDown")
                # Wait briefly for new content to load
                page.wait_for_timeout(500)
                # Get the new scroll height
                new_height = page.evaluate("document.body.scrollHeight")
                # If height remains the same, increment the counter
                if old_height == new_height:
                    count += 1
                # If height changed, reset the counter
                else:
                    count = 0
            logging.info(f"page {page_index}: finish scroll {url}")

            # Extract the full HTML content of the page after scrolling
            html_text = page.content()

            # Close the browser page
            page.close()

        # Create BeautifulSoup object to parse the HTML
        soup = BeautifulSoup(html_text, 'html.parser')

        # Extract table column names (headers)
        header_elements = soup.select('th[style*="end"]')
        headers = [header.get_text(strip=True) for header in header_elements]

        # Extract all table rows
        row_elements = soup.select('tr')

        # List to store the extracted coin dictionaries
        coins = []

        # Loop through each row (each coin)
        for row in row_elements:
            coin_info = {}

            # Extract rank
            rank = row.select_one('td[style*="start"]')
            if rank:
                coin_info['rank'] = rank.get_text(strip=True)

            # Extract logo URL
            logo = row.select_one('img[class*="coin-logo"]')
            if logo:
                coin_info['logo'] = logo.attrs.get('src')

            # Extract coin name
            name = row.select_one('p[class*="coin-item-name"]')
            if name:
                coin_info['name'] = name.get_text(strip=True)

            # Extract coin symbol
            symbol = row.select_one('p[class*="coin-item-symbol"]')
            if symbol:
                coin_info['symbol'] = symbol.get_text(strip=True)

            # Extract data cells (Price, Volume, Change, etc.)
            cell_elements = row.select('td[style*="end"]')
            # Extract text from cells, using a separator
            cell_texts = [c.get_text(strip=True, separator=" | ") for c in cell_elements]

            if cell_texts:
                for idx in range(len(cell_texts)):
                    try:
                        header = headers[idx]
                        if idx == len(cell_texts) - 1:
                            # Extract image source for the last column (Last 7 Days chart)
                            img_tag = cell_elements[idx].find('img')
                            if img_tag:
                                coin_info[header] = img_tag.get('src')
                        else:
                            # Assign values to the corresponding headers
                            value = cell_texts[idx]
                            if value:
                                coin_info[header] = value
                    except Exception as e:
                        # Log error if an issue occurs during cell data extraction
                        logging.error(f"ERROR: {e}")

            # Normalization and collection
            if coin_info:
                # Create the final dictionary with consistent keys for database insertion
                normalized_coin = {
                    'rank': coin_info.get('rank'),
                    'logo': coin_info.get('logo'),
                    'name': coin_info.get('name'),
                    'symbol': coin_info.get('symbol'),
                    'price': coin_info.get('Price'),
                    'change_1h': coin_info.get('1h %'),
                    'change_24h': coin_info.get('24h %'),
                    'change_7d': coin_info.get('7d %'),
                    'market_cap': coin_info.get('Market Cap'),
                    'volume_24h': coin_info.get('Volume(24h)'),
                    'circulating_supply': coin_info.get('Circulating Supply'),
                    'last_7d': coin_info.get('Last 7 Days')
                }
                coins.append(normalized_coin)

        # Return the list of coins
        return coins


# ---------- Execution Pipeline ----------
def process_page(url, page_index):
    """
    Scrapes a single page and inserts the extracted data into the database.

    :param url: The URL of the page to process.
    :type url: str
    :param page_index: The page number for logging.
    :type page_index: int
    :returns: None
    """
    # Create database connection (for this page)
    db = sqlite3.connect(DB_NAME)
    try:
        # Call the scraper function
        coins = coinmarketcap(url, page_index)

        # Check if any coins were scraped
        if not coins:
            logging.warning(f"page {page_index}: no coins scraped")
            return

        # Insert coins into the database
        for coin in coins:
            insert_coin(db, coin)
            logging.info(f"page {page_index}: Inserted coin {coin.get('name')}")

    finally:
        # Ensure the database connection is closed
        db.close()


def main():
    """
    The main entry point of the program. Initializes the database and iterates through pages.
    """
    # Create or connect to the database
    create_db()
    # Generate the list of URLs to process (page 1 and 2)
    urls = [f"https://coinmarketcap.com/?page={i}" for i in range(1, 3)]

    # Loop over all URLs
    for idx, url in enumerate(urls, start=1):
        # Process each page
        process_page(url, idx)
        # Short delay between pages to avoid IP blocking
        time.sleep(2)


if __name__ == "__main__":
    # Ensure the main function runs only when the file is executed directly
    main()
