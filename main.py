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
    movietimes_api: str = "https://www.fandango.com/napi/theaterMovieShowtimes/"
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
        """Save data to database with support for multiple tables.
        
        Args:
            data: List of tuples containing data to insert
            db_path: Path to SQLite database file
            table_name: Name of the table to insert into
        """
        if not data:
            logger.warning(f"No data provided to save for table: {table_name}")
            return
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Mapping for table schemas
            schemas = {
                'cities': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, source TEXT, name TEXT, url TEXT)",
                'theaters': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, source TEXT, city_code TEXT, name TEXT, theater_id TEXT, url TEXT)",
                'showtimes': """(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    code TEXT UNIQUE, 
                    source TEXT, 
                    theater_code TEXT, 
                    theater_id TEXT, 
                    ticketing_date TEXT, 
                    movie_id TEXT, 
                    movie_title TEXT, 
                    runtime INT, 
                    release_date TEXT, 
                    rating TEXT, 
                    poster_url TEXT, 
                    genres TEXT, 
                    showtime_id TEXT, 
                    url TEXT
                )""",
                'seat_maps': """(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE,
                    showtime_id TEXT,
                    theater_id TEXT,
                    theater_name TEXT,
                    chain_code TEXT,
                    auditorium_id TEXT,
                    total_seats INTEGER,
                    available_seats INTEGER,
                    seats_data TEXT,
                    areas_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )"""
            }
            
            # Create table if not exists
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schemas[table_name]}")
            
            # Dynamic placeholders based on the first item in data
            placeholders = ",".join(["?"] * len(data[0]))
            
            # Column mappings for each table
            columns = {
                'cities': "code, source, name, url",
                'theaters': "code, source, city_code, name, theater_id, url",
                'showtimes': "code, source, theater_code, theater_id, ticketing_date, movie_id, movie_title, runtime, release_date, rating, poster_url, genres, showtime_id, url",
                'seat_maps': "code, showtime_id, theater_id, theater_name, chain_code, auditorium_id, total_seats, available_seats, seats_data, areas_data"
            }
            
            # Insert data
            cursor.executemany(
                f"INSERT OR IGNORE INTO {table_name} ({columns[table_name]}) VALUES ({placeholders})", 
                data
            )
            
            conn.commit()
            logger.info(f"Successfully saved {cursor.rowcount} rows to {table_name}")
            
            # Log some stats for seat_maps
            if table_name == 'seat_maps' and cursor.rowcount > 0:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(total_seats) as total_capacity,
                        SUM(available_seats) as total_available
                    FROM seat_maps
                """)
                stats = cursor.fetchone()
                logger.info(f"Seat map stats - Total records: {stats[0]}, Total capacity: {stats[1]}, Available: {stats[2]}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Database Error on {table_name}: {e}")
            raise

    async def fetch_pages(self, targets, mode='theaters', max_concurrent=5):
        """Fetch multiple pages asynchronously with rate limiting.
        
        Args:
            targets: List of target data
            mode: 'theaters', 'movietimes', or 'seats'
            max_concurrent: Maximum number of concurrent requests
        """
        logger.info(f"Starting async fetch for {len(targets)} pages in mode: {mode} (max concurrent: {max_concurrent})")
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        try:
            urls = []
            references = []
            cookie_header = None
            auth_token = None
            session_id = None
            extra_headers = {}
            
            if mode == 'theaters':
                for target in targets:
                    urls.append(target[-1])
                    references.append(target[1])
                    
            elif mode == 'movietimes':
                current_date = datetime.now()
                for target in targets:
                    theater_code = target[1]
                    theater_id = target[5]
                    for i in range(7):
                        date = current_date + timedelta(days=i)
                        url = urljoin(
                            self.movietimes_api,
                            f"{theater_id}?startDate={date.strftime('%Y-%m-%d')}&isdesktop=true&partnerRestrictedTicketing="
                        )
                        urls.append(url)
                        references.append(theater_code)
                
                # Capture cookies for movietimes
                if targets:
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
            
            elif mode == 'seats':
                import time
                timestamp = int(time.time() * 1000)
                
                for showtime_id, theater_url in targets:
                    url = f"https://tickets.fandango.com/checkoutapi/showtimes/v2/{showtime_id}/seat-map?_={timestamp}"
                    urls.append(url)
                    references.append(showtime_id)
                    timestamp += 1
                
                # Capture cookies and auth token for seat maps
                if targets:
                    try:
                        representative_url = targets[0][1]  # theater_url from first target
                        logger.info(f"Capturing cookies and auth from {representative_url}")
                        
                        cookie_header, cookies_dict, auth_token, session_id, extra_headers = await asyncio.to_thread(
                            self.capture_cookies_and_auth,
                            representative_url
                        )
                        
                        if not cookie_header:
                            logger.warning("No cookies captured")
                        if not auth_token:
                            logger.warning("No auth token captured - seat map requests will likely fail")
                    except Exception:
                        logger.exception("Failed to capture cookies/auth for seat maps")
            
            else:
                raise ValueError(f"Unknown mode: {mode}")
            
            # Set headers based on mode
            headers = {}
            
            if mode == 'movietimes':
                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'X-Requested-With': 'XMLHttpRequest',
                    'Referer': 'https://www.fandango.com/',
                }
                if cookie_header:
                    headers['Cookie'] = cookie_header
                    
            elif mode == 'seats':
                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Content-Type': 'application/json',
                    'Referer': 'https://tickets.fandango.com/mobileexpress/seatselection',
                    'Sec-Fetch-Dest': 'empty',
                    'Sec-Fetch-Mode': 'cors',
                    'Sec-Fetch-Site': 'same-origin',
                }
                if cookie_header:
                    headers['Cookie'] = cookie_header
                if auth_token:
                    headers['Authorization'] = auth_token
                if session_id:
                    headers['X-FD-SessionId'] = session_id
                # Add any extra anti-bot headers
                if extra_headers:
                    headers.update(extra_headers)
            
            # Define a semaphore-controlled fetch function
            async def fetch_with_semaphore(url, headers_dict):
                async with semaphore:
                    try:
                        # Optional: Add small delay between requests
                        await asyncio.sleep(0.1)
                        
                        return await AsyncFetcher.get(
                            url,
                            stealthy_headers=True,
                            follow_redirects=True,
                            timeout=60000,
                            headers=headers_dict if headers_dict else None
                        )
                    except Exception as e:
                        logger.error(f"Request failed for {url}: {e}")
                        return e
            
            # Fetch all pages with semaphore control
            logger.info(f"Fetching {len(urls)} URLs with max {max_concurrent} concurrent requests")
            pages = await asyncio.gather(*[
                fetch_with_semaphore(url, headers) for url in urls
            ], return_exceptions=True)
            
            # Process results based on mode
            results = []
            for page, ref in zip(pages, references):
                if isinstance(page, Exception):
                    logger.error(f"Failed to fetch page for {ref}: {page}")
                    results.append((ref, None))
                else:
                    try:
                        content = page.text if hasattr(page, 'text') else str(page)
                        
                        # For seats mode, expect pure JSON
                        if mode == 'seats':
                            try:
                                json_data = json.loads(content)
                                logger.info(f"Successfully fetched seat map for {ref}")
                                results.append((ref, json_data))
                            except json.JSONDecodeError as e:
                                logger.error(f"JSON decode error for {ref}: {e}")
                                # Save error response for debugging
                                self.write_response_to_file(
                                    content,
                                    filename=f"seat_error_{ref}.json",
                                    out_dir="out/errors"
                                )
                                results.append((ref, None))
                        
                        # For movietimes, extract JSON from HTML wrapper
                        elif mode == 'movietimes':
                            selector = page if hasattr(page, "css") else Selector(content)
                            body_element = selector.css("body").get()
                            
                            if body_element:
                                json_str = body_element.text.strip()
                                try:
                                    json_data = json.loads(json_str)
                                    logger.info(f"Successfully extracted JSON for {ref}")
                                    results.append((ref, json_data))
                                except json.JSONDecodeError as e:
                                    logger.error(f"JSON decode error for {ref}: {e}")
                                    # Save error response for debugging
                                    self.write_response_to_file(
                                        content,
                                        filename=f"movietimes_error_{ref}.html",
                                        out_dir="out/errors"
                                    )
                                    results.append((ref, None))
                            else:
                                logger.warning(f"Could not extract JSON from HTML for {ref}")
                                results.append((ref, None))
                        
                        # For theaters, return the response object directly
                        else:
                            results.append((ref, page))
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error for {ref}: {e}")
                        results.append((ref, None))
                    except Exception as e:
                        logger.error(f"Failed to parse page for {ref}: {e}")
                        results.append((ref, None))
            
            logger.info(f"Completed fetch for {len(results)} pages in mode: {mode}")
            return results
            
        except Exception as e:
            logger.error(f"Error in fetch_pages: {e}")
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
                    hashlib.md5(urljoin(self.base_url, elem.attrib["href"]).encode()).hexdigest(),
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
                    # Navigate the JSON structure
                    view_model = response.get('viewModel', {})
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
        
    def capture_cookies_and_auth(self, ticketing_url, timeout=60000):
        """Capture cookies and Authorization token from ticketing page.
        
        Args:
            ticketing_url: The jump.aspx URL that loads the seat selection page
        """
        captured_auth = None
        captured_session_id = None
        captured_headers = {}
        
        def _on_response(resp):
            nonlocal captured_auth, captured_session_id, captured_headers
            try:
                url = getattr(resp, 'url', '')
                
                # Only capture from seat-map API requests
                if 'seat-map' in url or 'checkoutapi' in url:
                    req_obj = getattr(resp, 'request', None)
                    if req_obj:
                        try:
                            headers = getattr(req_obj, 'headers', None)
                            if callable(headers):
                                headers = headers()
                            
                            if headers and isinstance(headers, dict):
                                for k, v in headers.items():
                                    k_lower = k.lower()
                                    
                                    # Capture Authorization
                                    if k_lower == 'authorization' and v:
                                        captured_auth = v
                                        logger.info(f"✓ Captured Authorization header")
                                    
                                    # Capture SessionId
                                    elif k_lower == 'x-fd-sessionid' and v:
                                        captured_session_id = v
                                        logger.info(f"✓ Captured X-FD-SessionId: {v}")
                                    
                                    # Capture anti-bot headers (o3b4ZbAEVo-*)
                                    elif k_lower.startswith('o3b4zbaevo-'):
                                        captured_headers[k] = v
                                        logger.info(f"✓ Captured header: {k}")
                        except Exception as e:
                            logger.debug(f"Error extracting request headers: {e}")
            except Exception:
                pass
        
        try:
            fetcher = DynamicFetcher()
            
            logger.info(f"Loading ticketing page: {ticketing_url}")
            
            # Fetch the ticketing page - this will trigger the seat-map API call
            page = fetcher.fetch(
                url=ticketing_url,
                headless=True,
                network_idle=True,
                timeout=timeout
            )
            
            # Wait for any additional requests
            import time
            time.sleep(3)
            
            # Try to trigger seat map load if not already loaded
            try:
                # The page might need interaction to trigger the seat map API
                if hasattr(fetcher, 'page') and hasattr(fetcher.page, 'evaluate'):
                    # Wait for page to be ready
                    fetcher.page.wait_for_load_state('networkidle', timeout=10000)
            except Exception as e:
                logger.debug(f"Could not wait for network idle: {e}")
            
            # Get cookies from the page
            cookies_list = page.cookies if page.cookies else []
            
            # Filter essential cookies
            essential_patterns = [
                'akamai_location', 'akamai_generated_location',
                'searchcity', 'searchstate', 'searchlocation',
                'source', 'devicefamily', 'pcontext',
                'ASP.NET_SessionId', 'WPPCLdoC', 'OptanonConsent',
                's_ecid', 'AMCV_', 'eproperties', 'check', 'PurchaseChannel'
            ]
            
            filtered = []
            
            for cookie in cookies_list:
                if not isinstance(cookie, dict):
                    continue
                
                name = cookie.get('name', '')
                domain = cookie.get('domain', '')
                
                # Capture ASP.NET_SessionId
                if name == 'ASP.NET_SessionId' and not captured_session_id:
                    captured_session_id = cookie.get('value', '')
                    logger.info(f"✓ Found SessionId in cookies: {captured_session_id}")
                
                if any(pattern in name for pattern in essential_patterns):
                    if 'fandango.com' in domain or domain.startswith('.'):
                        filtered.append(cookie)
            
            cookie_header = None
            if filtered:
                cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in filtered)
                logger.info(f"✓ Using {len(filtered)} essential cookies")
            
            cookies_dict = {c['name']: c['value'] for c in cookies_list if isinstance(c, dict) and c.get('name')}
            
            # Log what we captured
            if captured_auth:
                logger.info("✓ Successfully captured Authorization token")
            else:
                logger.warning("✗ Failed to capture Authorization token")
            
            if captured_session_id:
                logger.info("✓ Successfully captured SessionId")
            else:
                logger.warning("✗ Failed to capture SessionId")
            
            if captured_headers:
                logger.info(f"✓ Captured {len(captured_headers)} anti-bot headers")
            
            return cookie_header, cookies_dict, captured_auth, captured_session_id, captured_headers
            
        except Exception:
            logger.exception(f"Failed to capture cookies and auth from {ticketing_url}")
            return None, {}, None, None, {}

    def get_seats(self, seat_map_data):
        """Parse seat map data to extract seat availability."""
        try:
            seat_records = []
            
            for showtime_id, json_data in seat_map_data:
                if json_data is None or isinstance(json_data, Exception):
                    logger.warning(f"No seat map data for showtime_id: {showtime_id}")
                    continue
                
                try:
                    data = json_data.get('data', {})
                    
                    # Extract metadata
                    theater_id = data.get('theaterId', '')
                    theater_name = data.get('theaterName', '')
                    chain_code = data.get('chainCode', '')
                    auditorium_id = data.get('auditoriumId', '')
                    total_seats = data.get('totalSeatCount', 0)
                    available_seats = data.get('totalAvailableSeatCount', 0)
                    
                    # Extract seat details
                    seats = data.get('seats', [])
                    areas = data.get('areas', [])
                    
                    # Create unique code
                    code = hashlib.md5(showtime_id.encode()).hexdigest()
                    
                    seat_records.append((
                        code,
                        showtime_id,
                        theater_id,
                        theater_name,
                        chain_code,
                        str(auditorium_id),
                        total_seats,
                        available_seats,
                        json.dumps(seats),  # Store full seat layout
                        json.dumps(areas),  # Store area/pricing info
                    ))
                    
                except Exception as e:
                    logger.error(f"Error parsing seat map for {showtime_id}: {e}")
                    continue
            
            logger.info(f"Parsed {len(seat_records)} seat maps.")
            
            if seat_records:
                self.save_to_db(seat_records, table_name="seat_maps")
                
        except Exception as e:
            logger.error(f"Error in parse_seat_maps: {e}")
            raise


    # def main(self):
    #     try:
    #         # 1. Get Cities
    #         city_page = self.fetch_page(urljoin(self.base_url, "movietimes"))
    #         self.get_cities(city_page)
            
    #         # 2. Read Cities back
    #         cities = self.read_from_db("SELECT * FROM cities")
    #         if not cities:
    #             logger.error("No cities found in DB. Stopping.")
    #             return

    #         # 3. Fetch Theaters Async
    #         theater_pages = asyncio.run(self.fetch_pages(targets=cities[0:2]))

    #         # 4. Parse and Save Theaters
    #         self.get_theaters(theater_pages)
    #         theaters = self.read_from_db("SELECT * FROM theaters WHERE name <> 'Select Theater'")

    #         # 5. Fetch showtimes
    #         showtimes_pages = asyncio.run(self.fetch_pages(targets=theaters[0:2], mode='movietimes'))
    #         self.write_response_to_file(showtimes_pages[0][1], filename="showtimes_page_example.html")

    #         # 6. Parse and Save showtimes
    #         self.get_showtimes(showtimes_pages[0:2])
    #         showtimes = self.read_from_db("SELECT * FROM showtimes")
    #         print(showtimes[0:5])

            
    #         # 7. Fetch seats
    #         # seats_pages = asyncio.run(self.fetch_pages(targets=showtimes[19:21], mode='seats'))
    #         # self.write_response_to_file(seats_pages[0][1], filename="seat_page_example.html")

    #         # 8. Parse and Save seats
    #         # self.get_seats(seats_pages[0:2])
    #         # seat_map_results = await scraper.fetch_seat_maps_from_showtimes(showtimes)


    #     except Exception as exc:
    #         logger.exception("Main loop failed")

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

            # 3. Fetch Theaters Async (limit to 5 concurrent)
            theater_pages = asyncio.run(
                self.fetch_pages(targets=cities[0:1], max_concurrent=5)
            )

            # 4. Parse and Save Theaters
            self.get_theaters(theater_pages)
            theaters = self.read_from_db("SELECT * FROM theaters WHERE name <> 'Select Theater'")

            # 5. Fetch showtimes (limit to 10 concurrent to avoid overwhelming the server)
            showtimes_pages = asyncio.run(
                self.fetch_pages(targets=theaters[0:1], mode='movietimes', max_concurrent=10)
            )
            self.write_response_to_file(showtimes_pages[0][1], filename="showtimes_page_example.html")

            # 6. Parse and Save showtimes
            self.get_showtimes(showtimes_pages[0:1])
            showtimes = self.read_from_db("SELECT * FROM showtimes WHERE showtime_id IS NOT NULL")


            # 7. Prepare seat map targets
            # showtimes_for_seats = [
            #     (showtime[13], showtime[14])  # (showtime_id, url)
            #     for showtime in showtimes[0:20]
            #     if showtime[13] and showtime[14]
            # ]
            
            # 8. Fetch seats (very conservative rate limit)
            # seats_pages = asyncio.run(
            #     self.fetch_pages(targets=showtimes_for_seats, mode='seats', max_concurrent=3)
            # )
            
            # if seats_pages and seats_pages[0][1]:
            #     self.write_response_to_file(seats_pages[0][1], filename="seat_page_example.json")

            # 9. Parse and Save seats
            # self.get_seats(seats_pages)

            print('Cities:')
            print(len(cities))
            print(cities[0])

            print('Theaters:')
            print(len(theaters))
            print(theaters[0])

            print('Showtimes:')
            print(len(showtimes))
            print(showtimes[0])


        except Exception as exc:
            logger.exception("Main loop failed")
    

if __name__ == "__main__":
    scraper = Scraper()
    scraper.main()