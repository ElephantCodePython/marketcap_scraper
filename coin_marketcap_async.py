"""
Note:
Some documentation and comments were written with the assistance of Google's Gemini AI model
to improve code readability and consistency.
"""
"""
An asynchronous web scraper designed to extract cryptocurrency data from CoinMarketCap.
It uses Playwright for dynamic content loading, BeautifulSoup for HTML parsing,
and stores the results in an aiosqlite database.
"""
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from fake_headers import Headers
import aiosqlite
import logging

# Basic logging configuration setup
logging.basicConfig(level=logging.INFO)
# Global variable for database connection (currently None)
db = None
# Database file name constant
DB_NAME = 'coins_async.db'
# Redefining the global variable db (same value, None)
db = None


# ---------- Asynchronous Database Functions ----------

async def create_db():
    """
    Creates the SQLite database file and the 'coins' table if it does not already exist.

    Uses aiosqlite for running commands in an asynchronous environment.
    """
    # Establish asynchronous connection to the database using a context manager
    async with aiosqlite.connect(DB_NAME) as db:
        # SQL command to define the table structure
        await db.execute('''
            CREATE TABLE IF NOT EXISTS coins (
                rank INTEGER,
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
        await db.commit()


async def open_db():
    """
    Opens the database connection, or returns the existing one if already open.

    :returns: The active asynchronous SQLite database connection object.
    :rtype: aiosqlite.Connection
    """
    global db
    if db is None:
        # Create an asynchronous connection
        db = await aiosqlite.connect(DB_NAME)
    return db


async def insert_coin(db, coin):
    """
    Inserts a coin's data into the 'coins' table, or replaces it if a conflict occurs.

    :param db: The active asynchronous SQLite database connection object.
    :type db: aiosqlite.Connection
    :param coin: A dictionary containing the complete data for one coin.
    :type coin: dict
    :returns: None
    """
    # Execute the INSERT OR REPLACE command asynchronously
    await db.execute('''
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
    await db.commit()


async def close_db():
    """
    Closes the global database connection if it is open.
    """
    global db
    if db is not None:
        # Close the asynchronous connection
        await db.close()
        db = None
        logging.info("Database connection closed.")


# ---------- Asynchronous Scraper Function ----------
async def coinmarketcap(url, page_index):
    """
    Scrapes cryptocurrency data from a CoinMarketCap page using Playwright and BeautifulSoup.

    This function is an asynchronous generator (async generator) that yields coins one by one.

    :param url: The URL of the CoinMarketCap page to scrape.
    :type url: str
    :param page_index: The page number (used for logging purposes).
    :type page_index: int
    :yields: A dictionary containing one coin's information.
    :ytype: dict
    """
    # Generate random headers to simulate a real browser request
    headers = Headers().generate()
    logging.info(headers)

    # Initialize Playwright instance asynchronously
    async with async_playwright() as p:
        logging.info(f'page {page_index}: open browser')
        # Launch Chromium browser
        browser = await p.chromium.launch(headless=False)
        logging.info(f'page {page_index}: open context')
        # Create a new context with the generated headers
        context = await browser.new_context(extra_http_headers=headers)
        logging.info(f'page {page_index}: open page')
        page = await context.new_page()
        logging.info(f'page {page_index}: go to page {url}')
        # Navigate to the URL, waiting until the network is idle
        response = await page.goto(url, wait_until="networkidle", timeout=240000)

        # Check and log the HTTP response status
        if response:
            status = response.status
            logging.info(f"{url} → Status: {status}")

        # Scroll page
        logging.info(f'page {page_index}: start scroll')

        # Start the scrolling process to load all data via JavaScript
        count, max_count = 0, 20
        while count < max_count:
            # Get the current scroll height
            old_height = await page.evaluate("document.body.scrollHeight")
            # Simulate pressing ArrowDown to scroll
            for _ in range(10):
                await page.keyboard.press("ArrowDown")
            # Wait briefly for new content to load (asynchronously)
            await page.wait_for_timeout(500)
            # Get the new scroll height
            new_height = await page.evaluate("document.body.scrollHeight")
            # If height remains the same, increment the counter
            if old_height == new_height:
                count += 1
            # If height changed, reset the counter
            else:
                count = 0

        logging.info(f"page {page_index}: finish scroll {url}")

        # Extract the full HTML content of the page after scrolling
        html_text = await page.content()
        # Close the browser after content extraction
        await browser.close()

    logging.info(f'page {page_index}: start scraping')
    # Create BeautifulSoup object to parse the HTML
    soup = BeautifulSoup(html_text, 'html.parser')
    try:
        # Extract table column names (headers)
        headers = [h.get_text(strip=True) for h in soup.select('th[style*="end"]')]
        # Extract all table rows
        rows = soup.select('tr')

        # Loop through each row (each coin)
        for row in rows:
            coin_info = {}
            # rank
            # Extract rank and convert to integer (int)
            rank = row.select_one('td[style*="start"]')
            if rank:
                coin_info['rank'] = int(rank.get_text(strip=True))
            # logo
            # Extract logo URL
            logo = row.select_one('img[class*="coin-logo"]')
            if logo:
                coin_info['logo'] = logo.get('src')
            # name
            # Extract coin name
            name = row.select_one('p[class*="coin-item-name"]')
            if name:
                coin_info['name'] = name.get_text(strip=True)
            # symbol
            # Extract coin symbol
            symbol = row.select_one('p[class*="coin-item-symbol"]')
            if symbol:
                coin_info['symbol'] = symbol.get_text(strip=True)

            # information cells
            # Extract data cells (Price, Volume, Change, etc.)
            cell_elements = row.select('td[style*="end"]')
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
                        logging.error(f"ERROR: {e}")

            # Normalization and Yielding the data
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
                # Yield the coin as part of the asynchronous generator
                yield normalized_coin
    except Exception as e:
        # Log error if an issue occurs during the scraping process
        logging.error(f'ERROR: page {page_index} error is: {e}')
    logging.info(f'page {page_index}: end scraping')


# ---------- Asynchronous Execution Pipeline ----------
async def process_page(db, url, page_index):
    """
    Scrapes a single page and inserts the extracted data into the database asynchronously and concurrently.

    :param db: The active database connection.
    :type db: aiosqlite.Connection
    :param url: The URL of the page to process.
    :type url: str
    :param page_index: The page number.
    :type page_index: int
    :returns: None
    """
    # Iterate over the asynchronous generator coinmarketcap to receive coins
    async for coin in coinmarketcap(url, page_index):
        # Insert the coin into the database asynchronously
        await insert_coin(db, coin)
        logging.info(f"page {page_index}: Inserted coin {coin.get('name')}")


async def main():
    """
    The main asynchronous entry point of the program. Initializes the database and processes pages
    in concurrent batches.
    """
    # Create database structure
    await create_db()
    # Open the database connection
    database = await open_db()

    # Generate the list of URLs to process (page 1 to 4)
    urls = [f"https://coinmarketcap.com/?page={i}" for i in range(1, 3)]
    # Loop to divide pages into batches of two for concurrent execution
    for batch_index in range(0, len(urls), 2):
        # Select the current batch (e.g., pages 1 and 2, then 3 and 4)
        batch = urls[batch_index:batch_index + 2]
        # Create asynchronous tasks for each page in the batch
        tasks = [process_page(database, url, batch_index + j) for j, url in enumerate(batch)]
        # Execute all tasks in the batch concurrently
        await asyncio.gather(*tasks)

    # Close the database connection at the end
    await close_db()


if __name__ == "__main__":
    # Run the main asynchronous function in Python's Event Loop
    asyncio.run(main())
