import logging
import pickle as pkl
import re
import sys
from datetime import timedelta
from pathlib import Path
from time import sleep
from urllib.parse import parse_qs, urljoin, urlparse

import cfscrape
from bs4 import BeautifulSoup as bs

logging.basicConfig(filename='scraper.log', encoding='utf-8', filemode='w', level=logging.DEBUG)

# Check for previous run
if Path('page_reached.tmp').is_file():
    logging.debug('Detected previous attempted run - restarting on: ', end='')
    with open('page_reached.tmp', 'r') as f:
        INITIAL_PAGE = int(f.readline())
    logging.debug(f"page {INITIAL_PAGE}")
else:
    INITIAL_PAGE = 1
    logging.debug(f'No previous run - starting from page {INITIAL_PAGE}')

OUTPUT_FILE = sys.argv[1]
BACKUP_FREQ = 50      # In pages
SORT_KEY = 'relevance'
GUID_PATTERN = 'window\.AT\.correlationId = "([\w|.|-]+)'
DELAY = 5 # seconds
TIMEOUT = 3.05 # seconds

root = 'https://www.autotrader.co.uk'
json_endpoint = urljoin(root, '/json/fpa/initial/')
details_endpoint = urljoin(root, '/car-details/')
search = urljoin(root, '/car-search')
tech_specs = urljoin(root,'/json/taxonomy/technical-specification')

data = []
scrape_count = 0
# TODO: Look into using a session that stores cookies?
scraper = cfscrape.create_scraper()

page = INITIAL_PAGE
backup_page = INITIAL_PAGE
params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY}
search_page = scraper.get(search, params=params)
soup = bs(search_page.content, 'html.parser')
max_page = soup.find('li', class_='paginationMini__count').contents[1].string
delay_s = search_page.elapsed.total_seconds()
logging.debug(f'Starting scrape on page {page}/{max_page}')

while page < max_page:
    ads = soup.body.main.find_all('li', class_='search-page__result')

    for ad in ads:
        sleep(max(delay_s,1))
        images = []
        # Get ad page and price
        ad_url = ad.find('a', class_='js-click-handler').attrs['href']
        price = ad.find('div', class_="product-card-pricing__price").find('span').string
        price_float = float(price.strip('£').replace(',',''))
        ad_id = ad.attrs['id']
        ad_page = scraper.get(urljoin(root, ad_url), timeout=TIMEOUT)

        # Get correct params from ad page for API to accept the request to JSON endpoint
        ad_guid = re.search(GUID_PATTERN, str(ad_page.content)).group(1)
        details_obj = urlparse(ad_url)
        params = parse_qs(details_obj.query)

        # Get raw data used to fill in the ad page from JSON endpoint
        ad_details = scraper.get(urljoin(json_endpoint, ad_id), params = {**params, 'guid':ad_guid}, timeout=TIMEOUT)
        ad_json = ad_details.json()
        image_urls = ad_json['advert']['imageUrls']
        for image_url in image_urls:
            images.append(scraper.get(image_url).content, timeout=TIMEOUT)
        desc = ad_json['advert']['description']
        deriv = ad_json['vehicle']['derivativeId']
        specs = scraper.get(tech_specs, params={'derivative':deriv, 'channel':'cars'}).json()

        # Store relevant data in RAM 
        data.append({'price':price, 'price_float': price_float, 'description':desc, 'images':images, 'specs':specs})

    if (page - INITIAL_PAGE) % BACKUP_FREQ == 0 and page > BACKUP_FREQ:
        logging.debug(f'Backing up progress from page {backup_page} to {page}')
        with open('backup_{backup_page}_{page}', 'wb') as f, open('page_reached.tmp', 'w') as g:
            pkl.dump(data, f)
            data = []
            backup_page = page
            g.write(f'{page}')


    page += 1
    logging.debug(f'Scraped page - sleeping')
    sleep(max(delay_s,1))
    logging.debug(f'Moving on to scrape page {page}')
    params = {'postcode':'eh42ar', 'page': page, 'sort':SORT_KEY}
    
    search_page = scraper.get(search, params=params, )
    soup = bs(search_page.content, 'html.parser')
