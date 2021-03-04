import logging
import pickle as pkl
import re
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import sleep
from types import new_class
from urllib.parse import parse_qs, urljoin, urlparse

import cfscrape
from bs4 import BeautifulSoup as bs

rfh = logging.handlers.RotatingFileHandler(
    filename='data/scraper.log', 
    mode='w',
    maxBytes=1*1024*1024*1024,
    backupCount=2,
    encoding=None,
    delay=0
)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-25s %(levelname)-8s %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    handlers=[
        rfh
    ]
)

# Deeply nested data structure means recursive pickling needs a larger stack to work with
sys.setrecursionlimit(50000)

# Check for previous run
if Path('page_reached.tmp').is_file():
    logging.debug('Detected previous attempted run - restarting on: ')
    with open('page_reached.tmp', 'r') as f:
        INITIAL_PAGE = int(f.readline())
    logging.debug(f"page {INITIAL_PAGE}")
else:
    INITIAL_PAGE = 1
    logging.debug(f'No previous run - starting from page {INITIAL_PAGE}')

BACKUP_FREQ = 10     # pages
SORT_KEY = 'relevance'
GUID_PATTERN = 'window\.AT\.correlationId = "([\w|.|-]+)'
DELAY = 1 # seconds
TIMEOUT = 3.05 # seconds

ROOT = 'https://www.autotrader.co.uk'
JSON_ENDPOINT = urljoin(ROOT, '/json/fpa/initial/')
DETAILS_ENDPOINT = urljoin(ROOT, '/car-details/')
SEARCH_ENDPOINT = urljoin(ROOT, '/car-search')
SPECS_ENDPOINT = urljoin(ROOT,'/json/taxonomy/technical-specification')

data = []
page = INITIAL_PAGE
backup_page = INITIAL_PAGE

# TODO: Look into using a session that stores cookies?
scraper = cfscrape.create_scraper()

# Get first search of all ads - do outside loop to retrieve how many pages we can scrape
params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY}
search_page = scraper.get(SEARCH_ENDPOINT, params=params)
soup = bs(search_page.content, 'html.parser')
max_page = int(soup.find('li', class_='paginationMini__count').contents[3].string)
delay_s = search_page.elapsed.total_seconds()
logging.debug(f'Starting scrape on page {page}/{max_page}')

while page < max_page:
    ads = soup.body.main.find_all('li', class_='search-page__result')
    ad_idx = 1
    for ad in ads:
        sleep(max(delay_s,DELAY))
        status_string = f"\r Page {page}/{max_page} | Ad {ad_idx}/{len(ads)}                      "
        print(status_string, end='')
        images = []
        # Get ad page and price
        ad_url = ad.find('a', class_='js-click-handler').attrs['href']
        price = ad.find('div', class_="product-card-pricing__price").find('span').string
        price_float = float(price.strip('£').replace(',',''))
        ad_id = ad.attrs['id']
        ad_page = scraper.get(urljoin(ROOT, ad_url), timeout=TIMEOUT)

        # Get correct params from ad page for API to accept the request to JSON endpoint
        ad_guid = re.search(GUID_PATTERN, str(ad_page.content)).group(1)
        details_obj = urlparse(ad_url)
        params = parse_qs(details_obj.query)

        # Get raw data used to fill in the ad page from JSON endpoint
        ad_details = scraper.get(urljoin(JSON_ENDPOINT, ad_id), params = {**params, 'guid':ad_guid}, timeout=TIMEOUT)
        ad_json = ad_details.json()
        image_urls = ad_json['advert']['imageUrls']
        for image_url in image_urls:
            images.append(scraper.get(image_url, timeout=TIMEOUT).content)
        desc = ad_json['advert']['description']
        vehicle_data = ad_json['vehicle']
        try:
            deriv = ad_json['vehicle']['derivativeId']
            specs = scraper.get(SPECS_ENDPOINT, params={'derivative':deriv, 'channel':'cars'}, timeout=TIMEOUT).json()
        except KeyError:
            specs = {'Missing_deriv':True}

        # Store relevant data in RAM 
        data.append({'price':price, 'price_float': price_float, 'description':desc, 'images':images, 'vehicle': vehicle_data, 'specs':specs})
        ad_idx += 1

    if (page - INITIAL_PAGE) % BACKUP_FREQ == 0 and page > BACKUP_FREQ:
        print("Backup")
        logging.debug(f'Backing up progress from page {backup_page} to {page}')
        with open(f'data/backup_{backup_page}_{page}.pickle', 'wb') as f:
            pkl.dump(data, f, protocol=pkl.HIGHEST_PROTOCOL)
            data = []
            backup_page = page

        with open('page_reached.tmp', 'w') as g:
            g.write(f'{page}')


    page += 1
    logging.debug('Scraped page - sleeping')
    sleep(max(2*delay_s,DELAY))
    logging.debug(f'Moving on to scrape page {page}')
    params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY}
    
    search_page = scraper.get(SEARCH_ENDPOINT, params=params, timeout=TIMEOUT)
    new_delay = search_page.elapsed.total_seconds()
    if new_delay > 5*delay_s:
        logging.warning(f"Appear to be barraging server, delay went from {delay_s} to {new_delay}, taking a 10s break.")
        sleep(10)
    delay_s = new_delay
    soup = bs(search_page.content, 'html.parser')

