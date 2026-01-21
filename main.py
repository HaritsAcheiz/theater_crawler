from scrapling.parser import Selector
from scrapling.fetchers import Fetcher, AsyncFetcher, DynamicFetcher
import asyncio
import json
import requests
import logging
from dotenv import load_dotenv
import os
from dataclasses import dataclass
import time
from urllib.parse import urljoin
import sqlite3
from pathlib import Path
import hashlib
from datetime import datetime, timedelta

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
    showtimes_api: str = "https://www.fandango.com/napi/theaterMovieShowtimes/"
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

    def build_cookie_header(self, url="https://www.fandango.com/"):
        """Perform a lightweight requests Session GET to the given URL and
        return a Cookie header string like 'k1=v1; k2=v2'.
        """
        try:
            s = requests.Session()
            # Make a quick GET to establish cookies (adjust headers if needed)
            s.get(url, timeout=10)
            cookie_header = "; ".join(f"{k}={v}" for k, v in s.cookies.items())
            logger.debug("Built cookie header: %s", cookie_header)
            return cookie_header
        except Exception:
            logger.exception("Failed to build cookies from %s", url)
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
            # showtimes = ['viewModel']['movies'][i]['variants'][j]['amenityGroups'][k]['showtimes'][l]:
            # showtime_id = ['id']
            # ticketingDate = ['ticketingDate']
            # url = ['ticketingJumpPageURL']

            # ['viewModel']['movies'][i]:
            # mid = ['id']
            # movie_name = ['title']
            # runtime = ['runtime']
            # release_date = ['releaseDate']
            # rating = ['rating']
            # poster_url = ['poster']['size']['full']
            # genres = ['genres'][index]

            schemas = {
                'cities': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, name TEXT, url TEXT)",
                'theaters': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, city_code TEXT, name TEXT, theater_id TEXT, url TEXT)",
                'showtimes': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, theater_code TEXT, theater_id TEXT, ticketing_date TEXT, movie_id TEXT, movie_title TEXT, runtime INT, release_date TEXT, rating TEXT, poster_url TEXT, genres TEXT, showtime_id TEXT, url TEXT)",
                'seats': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, showtimes_code TEXT, name TEXT, url TEXT)"
            }
            
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schemas[table_name]}")
            
            # Dynamic placeholders based on the first item in data
            placeholders = ",".join(["?"] * len(data[0]))
            columns = {
                'cities': "code, source, name, url",
                'theaters': "code, source, city_code, name, theater_id, url",
                'showtimes': "code, source, theater_code, theater_id, ticketing_date, movie_id, movie_title, runtime, release_date, rating, poster_url, genres, showtime_id, url",
                'seats': "name, url" # Note: your original logic for seats had only 2 columns in VALUES
            }
            
            cursor.executemany(f"INSERT OR IGNORE INTO {table_name} ({columns[table_name]}) VALUES ({placeholders})", data)
            conn.commit()
            logger.info(f"Successfully saved {cursor.rowcount} rows to {table_name}")
            conn.close()

        except Exception as e:
            logger.error(f"Database Error on {table_name}: {e}")
            raise
    
    async def fetch_pages(self, targets, mode='theaters'):
        """Fetch multiple pages asynchronously."""
        # Configure parser settings if needed (not browser settings)
        try:
            AsyncFetcher.configure(adaptive=True)
        except Exception:
            logger.debug("AsyncFetcher.configure not available")
        
        logger.info(f"Starting async fetch for {len(targets)} pages...")
        
        try:
            urls = []
            references = []
            
            if mode == 'theaters':
                for target in targets:
                    urls.append(target[-1])
                    references.append(target[1])
                    
            elif mode == 'showtimes':
                current_date = datetime.now()
                for target in targets:
                    theater_code = target[1]
                    theater_id = target[5]
                    for i in range(7):
                        date = current_date + timedelta(days=i)
                        url = urljoin(
                            self.showtimes_api, 
                            f"{theater_id}?startDate={date.strftime('%Y-%m-%d')}&isdesktop=true&partnerRestrictedTicketing="
                        )
                        urls.append(url)
                        references.append(theater_code)
            else:
                raise ValueError(f"Unknown mode: {mode}")
            
            # Capture cookies for showtimes mode
            cookie_header = None
            if mode == 'showtimes' and targets:
                try:
                    representative_url = targets[0][-1]
                    logger.info(f"Capturing cookies from {representative_url}")
                    
                    cookie_header, cookies_dict = await asyncio.to_thread(
                        self.capture_cookies_via_dynamic, 
                        representative_url
                    )
                    
                    if not cookie_header:
                        logger.warning("No cookies captured; requests may be blocked")
                except Exception:
                    logger.exception("Failed to capture cookies")
            
            # Actually fetch the pages
            # headers = {}
            headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
                'Accept': '*/*',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': 'https://www.fandango.com/'
            }
            if cookie_header:
                headers['Cookie'] = cookie_header
            
            # Fetch all URLs concurrently
            tasks = []
            for url in urls:
                # Pass fetcher options directly to get() method
                task = AsyncFetcher.get(
                    url,
                    stealthy_headers=True,
                    follow_redirects=True,
                    timeout=60000,
                    headers=headers if headers else None
                )
                tasks.append(task)
            
            pages = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Combine pages with their references
            results = []
            for page, ref in zip(pages, references):
                if isinstance(page, Exception):
                    logger.error(f"Failed to fetch page for {ref}: {page}")
                    results.append((ref, None))
                else:
                    results.append((ref, page))
            
            return results
            
        except Exception:
            logger.exception("fetch_pages failed")
            return []

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
                        theater_id = url.split("/")[-2].split("-")[-1].upper()
                        theaters.append((code, response.url, city_code, name, theater_id, url))

            logger.info(f"Parsed {len(theaters)} total theaters from all pages.")
            if theaters:
                self.save_to_db(theaters, table_name="theaters")
            
        except Exception as e:
            logger.error(f"Error in get_theaters: {e}")
            raise
    
    def get_showtimes(self, page_contents):
        """Parse movie showtimes from API responses."""
        try:
            showtimes = []
            
            for theater_code, response in page_contents:
                if response is None or isinstance(response, Exception):
                    logger.warning(f"No data for theater_code: {theater_code}")
                    continue
                
                try:
                    # Use Scrapling to extract JSON from body tag
                    selector = response if hasattr(response, "css") else Selector(response.text)
                    body_element = selector.css("body").get()
                    
                    if not body_element:
                        logger.warning(f"Could not find body element for theater_code: {theater_code}")
                        continue
                    
                    # Get text content from body
                    json_str = body_element.text.strip()
                    json_data = json.loads(json_str)
                    
                    # Navigate the JSON structure
                    view_model = json_data.get('viewModel', {})
                    theater_info = view_model.get('theater', {})
                    theater_id = theater_info.get('id', '')
                    
                    # Get movies list
                    movies = view_model.get('movies', [])
                    
                    if not movies:
                        logger.debug(f"No movies found for theater_code: {theater_code}")
                        continue
                    
                    for movie in movies:
                        # Extract movie details
                        movie_id = movie.get('id', '')
                        movie_title = movie.get('title', '').strip()
                        runtime = movie.get('runtime', 0)  # in minutes
                        release_date = movie.get('releaseDate', '')
                        rating = movie.get('rating', '')
                        
                        # Extract poster URL from nested structure
                        poster_data = movie.get('poster', {})
                        poster_size = poster_data.get('size', {})
                        poster_url = poster_size.get('full', '')
                        
                        # Extract genres (usually a list)
                        genres_list = movie.get('genres', [])
                        genres = ','.join(genres_list) if isinstance(genres_list, list) else str(genres_list)
                        
                        # Get variants (format variations like IMAX, 3D, etc.)
                        variants = movie.get('variants', [])
                        
                        if not variants:
                            logger.debug(f"No variants found for movie {movie_id} in theater {theater_code}")
                            continue
                        
                        for variant in variants:
                            # Get amenity groups (different screening types)
                            amenity_groups = variant.get('amenityGroups', [])
                            
                            if not amenity_groups:
                                logger.debug(f"No amenity groups for movie {movie_id}")
                                continue
                            
                            for amenity_group in amenity_groups:
                                # Get showtimes for this amenity group
                                showtimes_data = amenity_group.get('showtimes', [])
                                
                                if not showtimes_data:
                                    # Record movie without showtimes
                                    code = hashlib.md5(f"{theater_code}_{movie_id}".encode()).hexdigest()
                                    showtimes.append((
                                        code,
                                        'fandango',  # source
                                        theater_code,
                                        theater_id,
                                        None,  # ticketing_date
                                        movie_id,
                                        movie_title,
                                        runtime,
                                        release_date,
                                        rating,
                                        poster_url,
                                        genres,
                                        None,  # showtime_id
                                        None   # url
                                    ))
                                    continue
                                
                                for showtime in showtimes_data:
                                    showtime_id = showtime.get('id', '')
                                    ticketing_date = showtime.get('ticketingDate', '')
                                    ticketing_url = showtime.get('ticketingJumpPageURL', '')
                                    
                                    # Create unique code for this showtime
                                    code = hashlib.md5(
                                        f"{theater_code}_{movie_id}_{showtime_id}".encode()
                                    ).hexdigest()
                                    
                                    # Build full URL if relative
                                    url = urljoin(self.base_url, ticketing_url) if ticketing_url else None
                                    
                                    showtimes.append((
                                        code,
                                        'fandango',
                                        theater_code,
                                        theater_id,
                                        ticketing_date,
                                        movie_id,
                                        movie_title,
                                        runtime,
                                        release_date,
                                        rating,
                                        poster_url,
                                        genres,
                                        showtime_id,
                                        url
                                    ))
                    
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error for theater_code {theater_code}: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error parsing theater_code {theater_code}: {e}")
                    continue
            
            logger.info(f"Parsed {len(showtimes)} total showtimes from all pages.")
            
            if showtimes:
                self.save_to_db(showtimes, table_name="showtimes")
                
        except Exception as e:
            logger.error(f"Error in get_showtimes: {e}")
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

    def capture_cookies_via_dynamic(self, theater_url, timeout=60000):
        """Capture cookies using DynamicFetcher."""
        try:
            # Fetch the page
            page = DynamicFetcher.fetch(
                url=theater_url,
                headless=True,
                network_idle=True,
                timeout=timeout
            )
            
            # page.cookies returns a tuple/list of cookie dicts
            cookies_list = page.cookies if page.cookies else []
            
            logger.info(f"Captured {len(cookies_list)} total cookies")
            
            # Filter to essential cookies
            essential_patterns = [
                'akamai_location', 'akamai_generated_location',
                'searchcity', 'searchstate', 'searchlocation',
                'pcontext', 'source', 'devicefamily',
                'WPPCLdoC', 'OptanonConsent', 'OptanonAlertBoxClosed',
                's_ecid', 'AMCV_'
            ]
            
            filtered = []
            for cookie in cookies_list:
                if not isinstance(cookie, dict):
                    continue
                
                name = cookie.get('name', '')
                # Only include cookies for .fandango.com domain
                domain = cookie.get('domain', '')
                
                if any(pattern in name for pattern in essential_patterns):
                    # Check if it's a fandango cookie
                    if 'fandango.com' in domain or domain == 'www.fandango.com':
                        filtered.append(cookie)
            
            cookie_header = None
            if filtered:
                cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in filtered)
                logger.info(f"Using {len(filtered)} essential cookies out of {len(cookies_list)} total")
                logger.info(f"Cookie names: {[c['name'] for c in filtered]}")
                logger.info(f"Cookie header length: {len(cookie_header)} bytes")
            else:
                logger.warning("No essential cookies found after filtering")
            
            # Convert to dict for backward compatibility
            cookies_dict = {c['name']: c['value'] for c in cookies_list if isinstance(c, dict) and c.get('name')}
            
            return cookie_header, cookies_dict
            
        except Exception:
            logger.exception("Dynamic cookie capture failed for %s", theater_url)
            return None, {}

    def main(self):
        try:
            # 1. Get Cities
            # city_page = self.fetch_page(urljoin(self.base_url, "showtimes"))
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
            # theaters = self.read_from_db("SELECT * FROM theaters WHERE name <> 'Select Theater'")
            # print(theaters[0:2])

            # 5. Fetch showtimes
            # If you have a working curl, prefer passing --cookie or --headers-file to avoid DynamicFetcher capture.
            # showtimes_pages = asyncio.run(self.fetch_pages(targets=theaters[0:2], mode='showtimes'))

            # 6. Parse and Save showtimes
            # self.get_showtimes(showtimes_pages[0:2])
            showtimes = self.read_from_db("SELECT * FROM showtimes")
            print(showtimes[0:20])

        except Exception as exc:
            logger.exception("Main loop failed")
    

if __name__ == "__main__":
    scraper = Scraper()
    scraper.main()