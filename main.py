from scrapling.parser import Selector
from scrapling.fetchers import Fetcher, AsyncFetcher, DynamicFetcher
import asyncio
import json
import requests
import logging
from dotenv import load_dotenv
import os
from dataclasses import dataclass
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
        if not data:
            logger.warning(f"No data provided to save for table: {table_name}")
            return
            
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Mapping for table schemas
            schemas = {
                'cities': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, name TEXT, url TEXT)",
                'theaters': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, city_code TEXT, name TEXT, theater_id TEXT, url TEXT)",
                'movietimes': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, source TEXT, theater_code TEXT, date_part date, time_part time, name TEXT, movie_id TEXT, theater_id TEXT, rating TEXT, url TEXT)",
                'seats': "(id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, movietimes_code TEXT, name TEXT, url TEXT)"
            }
            
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} {schemas[table_name]}")
            
            # Dynamic placeholders based on the first item in data
            placeholders = ",".join(["?"] * len(data[0]))
            columns = {
                'cities': "code, source, name, url",
                'theaters': "code, source, city_code, name, theater_id, url",
                'movietimes': "code, source, theater_code, date_part, time_part, name, movie_id, theater_id, rating, url",
                'seats': "name, url" # Note: your original logic for seats had only 2 columns in VALUES
            }
            
            cursor.executemany(f"INSERT OR IGNORE INTO {table_name} ({columns[table_name]}) VALUES ({placeholders})", data)
            conn.commit()
            logger.info(f"Successfully saved {cursor.rowcount} rows to {table_name}")
            conn.close()

        except Exception as e:
            logger.error(f"Database Error on {table_name}: {e}")
            raise
    
    async def fetch_pages(self, targets, mode='theaters', cookie_header=None, cookie_url=None, headers_template=None):
        # prefer the configure API to set defaults (avoids deprecation warnings)
        try:
            AsyncFetcher.configure(adaptive=True, stealthy_headers=True, follow_redirects=True, timeout=60000)
        except Exception:
            logger.debug("AsyncFetcher.configure not available; proceeding to instantiate fetcher with defaults")

        fetcher = AsyncFetcher()
        
        logger.info(f"Starting async fetch for {len(targets)} pages...")
        
        try:
            # Build urls and a parallel references list so each response can be mapped
            # back to its originating target (e.g. theater_code). urls_len may differ
            # from len(targets) when mode='movietimes' because we generate multiple
            # dates per theater.
            urls = []
            references = []

            if mode == 'theaters':
                for target in targets:
                    urls.append(target[-1])
                    # use index 1 as the canonical code (matches your DB schema)
                    references.append(target[1])

            elif mode == 'movietimes':
                current_date = datetime.now()
                for target in targets:
                    theater_code = target[1]
                    theater_id = target[5]
                    for i in range(7):
                        date = current_date + timedelta(days=i)
                        urls.append(urljoin(self.movietimes_api, f"{theater_id}?startDate={date.strftime('%Y-%m-%d')}&isdesktop=true&partnerRestrictedTicketing="))
                        # repeat the theater_code for each generated URL
                        references.append(theater_code)

            else:
                raise ValueError(f"Unknown mode specified for fetch_pages: {mode}")

            # If requested, build a cookie header from a URL (quick requests-based
            # session) when cookie_header isn't provided directly. If not provided
            # and we're in movietimes mode, attempt to capture cookies via a
            # headless browser using a representative theater page from targets.
            if cookie_header is None:
                if cookie_url:
                    try:
                        cookie_header = self.build_cookie_header(cookie_url)
                    except Exception:
                        logger.exception("Failed to build cookie header from %s", cookie_url)
                elif mode == 'movietimes' and targets:
                    # attempt to capture cookies using DynamicFetcher on a theater page
                    try:
                        # targets entries are rows where the last element is the theater URL
                        representative_url = targets[0][-1]
                        logger.info("No cookie header provided; capturing cookies via DynamicFetcher from %s", representative_url)
                        # capture_cookies_via_dynamic uses Playwright's sync API; run it in a thread
                        cookie_header, cookies_list, set_cookie_headers, captured_requests = await asyncio.to_thread(
                            self.capture_cookies_via_dynamic, representative_url
                        )
                        if not cookie_header:
                            logger.warning("Dynamic cookie capture returned no cookie header; movietimes requests may still be blocked")
                    except Exception:
                        logger.exception("Failed to capture cookies via DynamicFetcher for movietimes")

            tasks = []
            # If we captured a representative NAPI request, use its headers as a template
            # `headers_template` parameter (explicit) takes precedence over captured template
            replay_headers_template = None
            try:
                if 'captured_requests' in locals() and captured_requests:
                    # pick first captured request that looks like the movietimes API
                    for r in captured_requests:
                        u = r.get('url') if isinstance(r, dict) else None
                        if u and 'napi/theaterMovieShowtimes' in u:
                            replay_headers_template = r.get('request_headers') or r.get('headers')
                            if replay_headers_template:
                                # normalize keys to str
                                replay_headers_template = dict(replay_headers_template)
                                break
            except Exception:
                logger.debug("Could not build replay headers template from captured requests")

            for url in urls:
                # Try per-request headers first; if AsyncFetcher.get doesn't accept
                # headers, fall back to setting fetcher.headers (best-effort).
                # Start from explicit headers_template (highest priority), otherwise
                # fall back to captured replay_headers_template if present.
                headers = None
                if headers_template:
                    headers = dict(headers_template)
                elif replay_headers_template:
                    headers = dict(replay_headers_template)

                # Always include cookie_header if available
                if cookie_header:
                    if headers is None:
                        headers = {"Cookie": cookie_header}
                    else:
                        headers.update({"Cookie": cookie_header})

                if headers:
                    try:
                        task = fetcher.get(url, headers=headers)
                    except TypeError:
                        # method signature likely doesn't accept headers
                        try:
                            if hasattr(fetcher, 'headers') and isinstance(fetcher.headers, dict):
                                fetcher.headers.update(headers)
                        except Exception:
                            logger.debug("Could not set fetcher.headers; proceeding without per-request cookies/headers")
                        task = fetcher.get(url)
                else:
                    task = fetcher.get(url)
                tasks.append(task)
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            results = []
            success_count = 0
            for i, response in enumerate(responses):
                # Map response at index i back to the reference at index i
                ref = references[i] if i < len(references) else None

                if isinstance(response, Exception):
                    logger.warning(f"Request failed for {urls[i]} (ref={ref}): {response}")
                    continue

                success_count += 1
                results.append((ref, response))

            logger.info(f"Fetch complete. Success: {success_count}/{len(urls)} requests")
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
                        theater_id = url.split("/")[-2].split("-")[-1].upper()
                        theaters.append((code, response.url, city_code, name, theater_id, url))

            logger.info(f"Parsed {len(theaters)} total theaters from all pages.")
            if theaters:
                self.save_to_db(theaters, table_name="theaters")
            
        except Exception as e:
            logger.error(f"Error in get_theaters: {e}")
            raise
    
    def get_movietimes(self, page_contents):
        try:
            movietimes = []

            print(page_contents[0][1])
            for theater_code, response in page_contents:
                if response is None or isinstance(response, Exception):
                    continue
                

                selector = response if hasattr(response, "css") else Selector(response.body)
                showtime_elements = selector.css("li.shared-movie-showtimes")
                # print(len(showtime_elements))

                if not showtime_elements:
                    logger.debug(f"No showtimes found for theater_code: {theater_code}")
                    continue

                for showtime in showtime_elements:
                    name_elem = showtime.css_first("a.shared-movie-showtimes__movie-title-link")
                    rating_elem = showtime.css_first("data.shared-showtimes__movie-rating")
                    schedule_elems = showtime.css("showtimes-btn-list__item > a")
                    for sched in schedule_elems:
                        code = hashlib.md5(url.encode()).hexdigest()
                        source = response.url
                        url = sched.attrib.get("href", "")
                        # url = 'https://tickets.fandango.com/transaction/ticketing/mobile/jump.aspx?sdate=2026-01-21%2B12%3A00&from=mov_det_showtimes&source=desktop&mid=2040&tid=AAYAH&dfam=webbrowser&showtimehashcode=v2-e8be2d0ea039ad92bcc49cd16144d2dd968ba5d87205ad2fefd848870637d4dc'
                        sdate = url.split("sdate=")[-1].split("&")[0]
                        date_part = sdate.split("+")[0]
                        time_part = sdate.split("+")[-1].replace("%3A", ":")
                        movie_id = url.split("mid=")[-1].split("&")[0]
                        theater_id = url.split("tid=")[-1].split("&")[0]
                        name = name_elem.text.strip() if name_elem else ""
                        rating = rating_elem.attrib.get("value", "") if rating_elem else ""
                        movietimes.append((code, source, theater_code, date_part, time_part, name, movie_id, theater_id, rating, url))

            logger.info(f"Parsed {len(movietimes)} total movietimes from all pages.")
            if movietimes:
                self.save_to_db(movietimes, table_name="movietimes")
            
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

    def capture_cookies_via_dynamic(self, theater_url, timeout=60000):
        """Use DynamicFetcher (Playwright) to open the theater page and capture cookies.

        Returns a tuple (cookie_header, cookies_list, captured_set_cookie_headers)
        - cookie_header: str like 'k1=v1; k2=v2' suitable for passing as Cookie header
        - cookies_list: list of cookie dicts from the browser context if available
        - captured_set_cookie_headers: list of Set-Cookie header strings observed during the load
        """
        captured_set_cookie = []
        captured_requests = []

        def _on_response(resp):
            try:
                # robustly get headers (some wrappers expose as dict, some as callable)
                headers = None
                try:
                    hdrs = getattr(resp, 'headers', None)
                    headers = hdrs() if callable(hdrs) else hdrs
                except Exception:
                    headers = None

                if not headers:
                    headers = getattr(resp, 'response_headers', None) or {}

                # normalize keys to check for set-cookie
                if headers:
                    for k, v in list(headers.items()):
                        if k.lower() == 'set-cookie' and v:
                            captured_set_cookie.append(v)
                            break

                # record request/response url and status for debugging
                # attempt to capture request headers/body for NAPI endpoints
                req_info = {'url': getattr(resp, 'url', None), 'status': getattr(resp, 'status', None)}
                try:
                    # try common attributes for request object on the response
                    req_obj = getattr(resp, 'request', None) or getattr(resp, '_request', None) or getattr(resp, '_playwright_request', None)
                    if req_obj:
                        # headers may be callable or dict-like
                        rheaders = None
                        try:
                            hdrs = getattr(req_obj, 'headers', None)
                            rheaders = hdrs() if callable(hdrs) else hdrs
                        except Exception:
                            rheaders = None

                        if not rheaders:
                            # some wrappers expose request.headers as dict directly
                            rheaders = getattr(req_obj, 'request_headers', None) or getattr(req_obj, 'headers', None)

                        req_info['request_headers'] = dict(rheaders) if isinstance(rheaders, dict) else rheaders

                        # try to get post data / body
                        post_data = None
                        try:
                            pd = getattr(req_obj, 'post_data', None)
                            post_data = pd() if callable(pd) else pd
                        except Exception:
                            post_data = None
                        req_info['request_post_data'] = post_data
                except Exception:
                    logger.debug("Failed to extract request object from response wrapper")

                captured_requests.append(req_info)
            except Exception:
                logger.exception("Error in capture response callback")

        # use the recommended configure API to avoid deprecation behavior
        try:
            DynamicFetcher.configure(headless=True, network_idle=True, timeout=timeout)
        except Exception:
            # ignore if configure not available or fails; continue with defaults
            logger.debug("DynamicFetcher.configure not available or failed; proceeding with defaults")

        fetcher = DynamicFetcher()

        try:
            # Load the page; this will trigger network requests and our callback
            fetcher.fetch(url=theater_url, on_response=_on_response)

            # Try multiple ways to read cookies from the wrapper/browser context
            cookies_list = []
            try:
                # 1) common scrapling wrapper: fetcher.context().cookies()
                if hasattr(fetcher, 'context') and callable(getattr(fetcher, 'context')):
                    try:
                        cookies_list = fetcher.context().cookies()
                    except Exception:
                        # maybe context() returns an object with cookies() method
                        ctx = fetcher.context()
                        if hasattr(ctx, 'cookies') and callable(getattr(ctx, 'cookies')):
                            cookies_list = ctx.cookies()

                # 2) fetcher.page.context.cookies()
                if not cookies_list and hasattr(fetcher, 'page'):
                    page = fetcher.page
                    if hasattr(page, 'context') and callable(getattr(page.context, 'cookies', None)):
                        cookies_list = page.context.cookies()

                # 3) some wrappers expose cookies property directly
                if not cookies_list and hasattr(fetcher, 'cookies'):
                    try:
                        c = fetcher.cookies
                        if callable(c):
                            cookies_list = c()
                        else:
                            cookies_list = c
                    except Exception:
                        pass
            except Exception:
                logger.debug("Could not read browser context cookies via wrapper; falling back to Set-Cookie headers")

            # Build cookie header from cookies_list or from captured Set-Cookie headers
            cookie_header = None
            if cookies_list:
                try:
                    cookie_header = "; ".join(f"{c.get('name')}={c.get('value')}" for c in cookies_list if c.get('name'))
                except Exception:
                    cookie_header = None

            if not cookie_header and captured_set_cookie:
                # Parse simple name=value pairs from Set-Cookie strings
                pairs = []
                for sc in captured_set_cookie:
                    try:
                        first = sc.split(';', 1)[0].strip()
                        if '=' in first:
                            pairs.append(first)
                    except Exception:
                        continue
                if pairs:
                    cookie_header = "; ".join(pairs)

            return cookie_header, cookies_list, captured_set_cookie, captured_requests

        except Exception:
            logger.exception("Dynamic cookie capture failed for %s", theater_url)
            return None, [], [], []

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
            theaters = self.read_from_db("SELECT * FROM theaters WHERE name <> 'Select Theater'")
            print(theaters[0:2])

            # 5. Fetch movietimes
            movietimes_pages = asyncio.run(self.fetch_pages(targets=theaters[0:2], mode='movietimes'))
            print(movietimes_pages[0][1])

            # 6. Parse and Save Movietimes
            # self.get_movietimes(movietimes_pages[0:2])
            # movietimes = self.read_from_db("SELECT * FROM movietimes")

        except Exception as exc:
            logger.exception("Main loop failed")
    

if __name__ == "__main__":
    scraper = Scraper()
    scraper.main()