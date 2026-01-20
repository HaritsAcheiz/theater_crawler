from scrapling.parser import Selector
from scrapling.fetchers import Fetcher, AsyncFetcher, DynamicFetcher
import asyncio
import json
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


    def write_response_to_file(self, response, filename="response.html", out_dir="out", as_binary=False):
        """Write a fetcher response or raw content to a file.

        - response: object with a `.body` attribute or raw bytes/str
        - filename: target filename (will be created under `out_dir`)
        - as_binary: force binary write mode
        """
        try:
            Path(out_dir).mkdir(parents=True, exist_ok=True)
            path = Path(out_dir) / filename

            # extract body if the fetcher wraps it
            body = None
            if hasattr(response, "body"):
                body = response.body
            else:
                body = response

            # decide binary vs text
            write_binary = as_binary or isinstance(body, (bytes, bytearray))

            if write_binary:
                with open(path, "wb") as f:
                    if isinstance(body, str):
                        f.write(body.encode("utf-8"))
                    else:
                        f.write(body)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    if isinstance(body, (bytes, bytearray)):
                        f.write(body.decode("utf-8", errors="replace"))
                    else:
                        f.write(str(body))

            logger.info(f"Wrote response to {path}")
            return str(path)

        except Exception as e:
            logger.exception(f"Failed to write response to file {filename}: {e}")
            raise

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
                'movietimes': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, theater_code TEXT, date date, time time, name TEXT, showtime_id TEXT)",
                'seats': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, movietimes_code TEXT, name TEXT, url TEXT)"
            }
            
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schemas[table_name]}")
            
            # Dynamic placeholders based on the first item in data
            placeholders = ",".join(["?"] * len(data[0]))
            columns = {
                'cities': "code, source, name, url",
                'theaters': "code, source, city_code, name, url",
                'movietimes': "code, theater_code, date, time, name, showtime_id",
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
    
    def get_movietimes(self, page_contents):
        try:
            movietimes = []

            for theater_code, response in page_contents:
                print(theater_code, response)
            #     if response is None or isinstance(response, Exception):
            #         continue

            #     selector = response if hasattr(response, "css") else Selector(response.body)
            #     showtime_elements = selector.css("div.showtimes-list > div.showtime-item")

            #     if not showtime_elements:
            #         logger.debug(f"No showtimes found for theater_code: {theater_code}")
            #         continue

            #     for showtime in showtime_elements:
            #         name_elem = showtime.css_first("div.movie-title")
            #         time_elem = showtime.css_first("span.showtime")
            #         date_elem = showtime.css_first("span.showdate")
            #         showtime_id = showtime.attrib.get("data-showtime-id", "")

            #         if name_elem and time_elem and date_elem and showtime_id:
            #             name = name_elem.text.strip()
            #             time = time_elem.text.strip()
            #             date = date_elem.text.strip()
            #             code = hashlib.md5((theater_code + showtime_id).encode()).hexdigest()

            #             movietimes.append((code, theater_code, date, time, name, showtime_id))

            # logger.info(f"Parsed {len(movietimes)} total movietimes from all pages.")
            # if movietimes:
            #     self.save_to_db(movietimes, table_name="movietimes")
            
        except Exception as e:
            logger.error(f"Error in get_movietimes: {e}")
            raise

    def fetch_seat_map(self, showtime_id, movie_id=243965, chainCode='REGL', sdate='2026-01-21+19%3A05', theater_id='AAODH', timeout=60000, save_file=None):
        """Use DynamicFetcher to intercept Fandango seat-map JSON for a specific showtime.

        Returns a dict with total_capacity, available_seats, occupied and the raw JSON under 'data' if successful.
        """
        captured_json = []

        def capture_seat_map(response):
            try:
                if f"checkoutapi/showtimes/v2/{showtime_id}/seat-map" in getattr(response, 'url', ''):
                    if getattr(response, 'status', None) == 200:
                        logger.info("Captured seat map data from: %s", response.url)
                        try:
                            captured_json.append(response.json())
                        except Exception:
                            logger.exception("Error parsing JSON from seat-map response")
            except Exception:
                logger.exception("Error in capture_seat_map callback")

        fetcher = DynamicFetcher(headless=True, network_idle=True, timeout=timeout)

        target_url = f"https://tickets.fandango.com/mobileexpress/seatselection?row_count={showtime_id}&mid={movie_id}&chainCode={chainCode}&sdate={sdate}&tid={theater_id}"

        try:
            fetcher.fetch(url=target_url, on_response=capture_seat_map)
        except Exception:
            logger.exception("Dynamic fetch failed for seat map")

        if not captured_json:
            logger.warning("Could not intercept the seat-map JSON for showtime_id=%s", showtime_id)
            return None

        data = captured_json[0]
        seats = data.get('seatingConfig', {}).get('seats', []) if isinstance(data, dict) else []
        total_capacity = len(seats)
        available_seats = sum(1 for seat in seats if seat.get('isAvailable'))

        result = {
            'total_capacity': total_capacity,
            'available_seats': available_seats,
            'occupied': total_capacity - available_seats,
            'data': data,
        }

        if save_file:
            try:
                with open(save_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=4)
                logger.info("Saved seat map JSON to %s", save_file)
            except Exception:
                logger.exception("Failed to save seat map JSON to file: %s", save_file)

        return result

    def main(self):
        try:
            # 1. Get Cities
            # city_page = self.fetch_page(urljoin(self.base_url, "movietimes"))
            # self.get_cities(city_page)
            
            # 2. Read Cities back
            # cities = self.read_from_db("SELECT * FROM cities")
            # if not cities:
            #     logger.error("No cities found in DB. Stopping.")
            #     return

            # 3. Fetch Theaters Async
            # theater_pages = asyncio.run(self.fetch_pages(targets=cities))

            # 4. Parse and Save Theaters
            # self.get_theaters(theater_pages)
            theaters = self.read_from_db("SELECT * FROM theaters")

            # 5. Fetch movietimes
            movietimes_pages = asyncio.run(self.fetch_pages(targets=theaters[0:2]))
            self.write_response_to_file(movietimes_pages[0][1], filename="movietimes.html", out_dir=".")
            
            # 6. Parse and Save Movietimes
            # self.get_movietimes(movietimes_pages)
            # movietimes = self.read_from_db("SELECT * FROM movietimes")

        except Exception as exc:
            logger.exception("Main loop failed")
    

if __name__ == "__main__":
    scraper = Scraper()
    scraper.main()