import logging
import logging.handlers
import pickle as pkl
import random
import re
import sys
from itertools import product, tee
from pathlib import Path
from time import sleep
from urllib.parse import parse_qs, urljoin, urlparse

import cfscrape
from numpy.random import default_rng
rng = default_rng()
import requests
from bs4 import BeautifulSoup as bs

#TODO: Run with >1 thread + proxies - would simply segment these partitions i reckon, or split the retrieval of ads
#TODO: Extract exactly the data we need and tabulate it so we don't have pickled objects
#TODO: Look into just using the 'https://www.autotrader.co.uk/json/fpa/initial/' endpoint and getting everything we want 
# from there via a list of ad ids on search page
#TODO: Look into using a session that stores cookies?

def pairwise(iterable):
    # From https://docs.python.org/3/library/itertools.html#itertools-recipes
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

rfh = logging.handlers.RotatingFileHandler(
    filename='data/scraper.log', 
    mode='w',
    maxBytes=0.5*1024*1024*1024,
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


BACKUP_FREQ = 130     # ads, rememebr there is ~13 per page
SORT_KEY = 'relevance'
GUID_PATTERN = 'window\.AT\.correlationId = "([\w|.|-]+)'
DELAY = 1 # seconds, minimum delay between a barrage of requests to play nicely
TIMEOUT = 3.05 # seconds
IMAGES_TO_KEEP = 7 # Usually after first 7 images it is all interiors etc.

ROOT = 'https://www.autotrader.co.uk'
JSON_ENDPOINT = urljoin(ROOT, '/json/fpa/initial/')
DETAILS_ENDPOINT = urljoin(ROOT, '/car-details/')
SEARCH_ENDPOINT = urljoin(ROOT, '/car-search')
SPECS_ENDPOINT = urljoin(ROOT,'/json/taxonomy/technical-specification')

BODY_TYPES = ['Convertible', 'Hatchback', 'Pickup', 'Coupe', 'Estate', 'MPV', 'SUV', 'Saloon']
PRICE_POINTS = pairwise(range(0, 500000, 1000))
SEARCH_PARTITION = list(product(PRICE_POINTS,BODY_TYPES))
# SEARCH_PARTITION = rng.permutation(SEARCH_PARTITION)

data = []
partition_ad_count = 0
global_ad_count = 0
previous_backup = 0
page = 1
timeouts = 0
previous_partitions = []

# Check for previous run
if Path('partition_reached.tmp').is_file():
    logging.info('Detected previous attempted run - excluding previously searched partitions:')
    with open('partition_reached.tmp', 'r') as f:
        for line in f:
            body_type, price_from, price_to = line.strip().split(',') 
            price_from, price_to = int(price_from), int(price_to)
            previous_partitions.append((body_type,(price_from, price_to)))
    print(previous_partitions)
else:
    logging.info(f'No previous run detected.')


scraper = cfscrape.create_scraper()

for (price_from, price_to), body_type in SEARCH_PARTITION:
    if (body_type, (price_from, price_to)) in previous_partitions:
        logging.info(f"Seen partition {body_type} £{price_from}-{price_to} - skipping")
        continue
    # Get first search of all ads in our partition
    # we do this outside of the loop to retrieve how many pages we can scrape
    page = 1
    params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY, 'price-from':price_from, 'price-to':price_to, 'body-type':body_type}
    search_page = scraper.get(SEARCH_ENDPOINT, params=params)
    soup = bs(search_page.content, 'html.parser')
    max_page = int(soup.find('li', class_='paginationMini__count').contents[3].string)
    delay_s = search_page.elapsed.total_seconds()
    while page < max_page and page < 100:
        logging.info(f'Starting scrape on page {page}/{max_page}')
        ads = soup.body.main.find_all('li', class_='search-page__result')
        ad_idx = 1
        
        if len(ads) == 0:
            print(f"\nNo ads for {body_type} £{price_from}-{price_to} page {page}")
        
        for ad in ads:
            save_data = True
            try:
                noise = random.gauss(0,1)
                sleep(max(max(delay_s,DELAY) + noise,1))
                status_string = f"\rTotal ads {global_ad_count} || {body_type} £{price_from}-{price_to} Page {page}/{max_page} | Ad {ad_idx}/{len(ads)}        "
                print(status_string, end='')
                images = []
                # Get ad page and price
                ad_url = ad.find('a', class_='js-click-handler').attrs['href']
                price = ad.find('div', class_="product-card-pricing__price").find('span').string
                price_float = float(price.strip('£').replace(',',''))
                ad_id = ad.attrs['id']
                ad_page = scraper.get(urljoin(ROOT, ad_url), timeout=TIMEOUT)

                # Get correct params from ad page for API to accept the request to JSON endpoint
                try:
                    ad_guid = re.search(GUID_PATTERN, str(ad_page.content)).group(1)
                except AttributeError:
                    logging.error("Can't regex guid")
                    ad_idx += 1
                    continue
                details_obj = urlparse(ad_url)
                params = parse_qs(details_obj.query)

                # Get raw data used to fill in the ad page from JSON endpoint
                ad_details = scraper.get(urljoin(JSON_ENDPOINT, ad_id), params = {**params, 'guid':ad_guid}, timeout=TIMEOUT)
                ad_json = ad_details.json()
                try:
                    image_urls = ad_json['advert']['imageUrls']

                    for image_url in image_urls[:IMAGES_TO_KEEP]:
                        images.append(scraper.get(image_url.replace('/%7Bresize%7D/','/'), timeout=TIMEOUT).content)

                    desc = ad_json['advert']['description']
                except KeyError:
                    logging.error(f"Missing advert on {status_string}")
                    save_data = False
                try:
                    vehicle_data = ad_json['vehicle']
                except KeyError:
                    logging.error(f"Missing vehicle on {status_string}")
                    save_data = False
                try:
                    deriv = ad_json['vehicle']['derivativeId']
                    specs = scraper.get(SPECS_ENDPOINT, params={'derivative':deriv, 'channel':'cars'}, timeout=TIMEOUT).json()
                except KeyError:
                    logging.error(f"Missing deriv on {status_string}")
                    save_data = False
                partition_ad_count += 1
                global_ad_count += 1
                # Store relevant data in RAM 
                if save_data:
                    data.append({'price':price, 'price_float': price_float, 'description':desc, 'images':images, 'vehicle': vehicle_data, 'specs':specs, 'guid':ad_guid})
            except requests.Timeout:
                err_msg = f"\nTimed out on page {page} ad {ad_idx} - taking a break for 4 seconds and continuing"
                print(err_msg)
                logging.error(err_msg)
                with open('timeouts.csv', 'a') as f:
                    f.write(f'{page},{ad_idx}\n')
                sleep(4)
            except requests.ConnectionError:
                err_msg = f"\nConnection aborted on page {page} ad {ad_idx} - taking a break for 60 seconds and continuing"
                print(err_msg)
                logging.error(err_msg)
                with open('aborts.csv', 'a') as f:
                    f.write(f'{page},{ad_idx}\n')
                sleep(60)
            ad_idx += 1

            if (partition_ad_count - previous_backup) >= BACKUP_FREQ:
                print("Backup")
                logging.info(f'Backup')
                with open(f'/mnt/data/car_auction_data/backup_{body_type}_{price_from}-{price_to}_{previous_backup}_{partition_ad_count}.pickle', 'wb') as f:
                    pkl.dump(data, f, protocol=pkl.HIGHEST_PROTOCOL)
                    data = []
                    previous_backup = partition_ad_count


        page += 1
        logging.info('Scraped page - sleeping')
        noise = random.gauss(1,1)
        sleep(max(max(delay_s,DELAY) + noise,1))
        logging.info(f'Moving on to scrape page {page}')
        params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY}
        
        search_page = scraper.get(SEARCH_ENDPOINT, params=params, timeout=TIMEOUT)
        new_delay = search_page.elapsed.total_seconds()
        if new_delay > 5*delay_s:
            logging.warning(f"Appear to be barraging server, delay went from {delay_s} to {new_delay}, taking a 10s break.")
            sleep(10)
        delay_s = new_delay
        soup = bs(search_page.content, 'html.parser')

    print("Partition rounding backup")
    logging.info('Partition rounding backup')
    with open(f'/mnt/data/car_auction_data/backup_{body_type}_{price_from}-{price_to}_{previous_backup}_{partition_ad_count}.pickle', 'wb') as f:
        pkl.dump(data, f, protocol=pkl.HIGHEST_PROTOCOL)
        data = []
        partition_ad_count = 0
        previous_backup = 0

    with open('partition_reached.tmp', 'a') as g:
        g.write(f'{body_type},{price_from},{price_to}\n')
