from scrapling.parser import Selector
from scrapling.fetchers import Fetcher, AsyncFetcher
import asyncio
import logging
from dotenv import load_dotenv
import os
from dataclasses import dataclass
from urllib.parse import urljoin
import sqlite3
from pathlib import Path
import hashlib

load_dotenv()

# Set to INFO for general use, DEBUG for heavy troubleshooting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Scraper:
    base_url: str = "https://www.fandango.com/"
    seat_api: str = "https://tickets.fandango.com/checkoutapi/showtimes/v2/"

    def read_from_db(self, query, db_path="theaters.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results

    def save_to_db(self, data, db_path="theaters.db", table_name="theaters"):
        if not data:
            logger.warning(f"No data provided to save for table: {table_name}")
            return
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Mapping for table schemas
            schemas = {
                'cities': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, name TEXT, url TEXT)",
                'theaters': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, city_code TEXT, name TEXT, url TEXT)",
                'movietimes': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, theater_code TEXT, date date, time time, name TEXT, url TEXT)",
                'seats': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, movietimes_code TEXT, name TEXT, url TEXT)"
            }
            
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schemas[table_name]}")
            
            # Dynamic placeholders based on the first item in data
            placeholders = ",".join(["?"] * len(data[0]))
            columns = {
                'cities': "code, source, name, url",
                'theaters': "code, source, city_code, name, url",
                'movietimes': "code, theater_code, date, time, name, url",
                'seats': "name, url" # Note: your original logic for seats had only 2 columns in VALUES
            }
            
            cursor.executemany(f"INSERT OR IGNORE INTO {table_name} ({columns[table_name]}) VALUES ({placeholders})", data)
            conn.commit()
            logger.info(f"Successfully saved {cursor.rowcount} rows to {table_name}")
            conn.close()

        except Exception as e:
            logger.error(f"Database Error on {table_name}: {e}")
            raise
    
    async def fetch_pages(self, targets):
        fetcher = AsyncFetcher()
        fetcher.adaptive = True
        
        logger.info(f"Starting async fetch for {len(targets)} pages...")
        
        try:
            # Safer indexing: assumes URL is the last element
            urls = [target[-1] for target in targets]
            tasks = [fetcher.get(url) for url in urls]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            results = []
            success_count = 0
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    logger.warning(f"Request failed for {urls[i]}: {response}")
                    continue
                
                success_count += 1
                # target[1] is the city code from your DB structure
                city_code = targets[i][1]
                results.append((city_code, response)) 
            
            logger.info(f"Fetch complete. Success: {success_count}/{len(targets)}")
            return results

        except Exception as e:
            logger.exception(f"Critical error in fetch_pages: {e}")
            raise

    def fetch_page(self, url):
        fetcher = Fetcher()
        try:
            logger.info(f"Fetching single page: {url}")
            return fetcher.get(url)
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            raise

    def get_cities(self, page_content):
        try:
            city_elements = page_content.css("ul.tsp-no-location__top-cities-list > li > a")
            if not city_elements:
                logger.warning("No city elements found on page. Selector might be outdated.")
                return

            cities = []
            for elem in city_elements:
                cities.append((
                    hashlib.md5(page_content.url.encode()).hexdigest(),
                    page_content.url,
                    elem.text.strip(),
                    urljoin(self.base_url, elem.attrib["href"])
                ))
            self.save_to_db(cities, table_name="cities")

        except Exception as e:
            logger.exception(f"Parsing error in get_cities: {e}")
            raise

    def get_theaters(self, page_contents):
        try:
            theaters = []
            for city_code, response in page_contents:
                if response is None or isinstance(response, Exception):
                    continue

                selector = response if hasattr(response, "css") else Selector(response.body)
                option_elements = selector.css("select#nearby-theaters-select-list > option")

                if not option_elements:
                    # Useful to know which cities are failing
                    logger.debug(f"No theaters found for city_code: {city_code}")
                    continue

                for opt in option_elements:
                    name = opt.text.strip() if hasattr(opt, "text") else ""
                    value = opt.attrib.get("value", "")

                    if value:
                        url = urljoin(self.base_url, value)
                        code = hashlib.md5(url.encode()).hexdigest()
                        theaters.append((code, response.url, city_code, name, url))

            logger.info(f"Parsed {len(theaters)} total theaters from all pages.")
            if theaters:
                self.save_to_db(theaters, table_name="theaters")
            
        except Exception as e:
            logger.error(f"Error in get_theaters: {e}")
            raise

    def main(self):
        try:
            # 1. Get Cities
            city_page = self.fetch_page(urljoin(self.base_url, "movietimes"))
            self.get_cities(city_page)
            
            # 2. Read Cities back
            cities = self.read_from_db("SELECT * FROM cities")
            if not cities:
                logger.error("No cities found in DB. Stopping.")
                return

            # 3. Fetch Theaters Async
            theater_pages = asyncio.run(self.fetch_pages(targets=cities))

            # 4. Parse and Save Theaters
            self.get_theaters(theater_pages)

        except Exception as exc:
            logger.exception("Main loop failed")
    

if __name__ == "__main__":
    scraper = Scraper()
    scraper.main()