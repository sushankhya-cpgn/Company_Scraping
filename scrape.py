from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import string
import time
import pandas as pd
import os
import argparse

# WEBDRIVER CONFIG
CHROMEDRIVER_PATH = '/opt/homebrew/bin/chromedriver'
CSV_FILE = "company_data.csv"

# SAVE TO CSV FUNCTION 
def save_to_csv_batch(data_batch, is_first=False):
    if not data_batch:
        return
    df = pd.DataFrame(data_batch)
    df.to_csv(CSV_FILE, mode='w' if is_first else 'a', header=is_first, index=False)
    print(f"Saved {len(data_batch)} records to CSV.")

# WEBDRIVER POOL
class WebDriverPool:
    def __init__(self, pool_size=5):
        self.pool_size = pool_size
        self.drivers = []
        self.options = Options()
        self.options.add_argument('--headless')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--blink-settings=imagesEnabled=false')
        self.options.page_load_strategy = 'eager'

    def get_driver(self):
        if len(self.drivers) < self.pool_size:
            driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=self.options)
            self.drivers.append(driver)
            return driver
        return self.drivers.pop(0)

    def release_driver(self, driver):
        self.drivers.append(driver)

    def close_all(self):
        for driver in self.drivers:
            driver.quit()
        self.drivers = []

# SCRAPE COMPANY LIST
def scrape_letter(letter, driver_pool):
    driver = driver_pool.get_driver()
    url = f"https://www.bdtradeinfo.com/company-list/{letter}"
    try:
        driver.get(url)
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        categories = soup.find_all('a', class_='alphaSubcat')
        results = [(letter, cat.text.strip(), cat['href']) for cat in categories]
        return results
    except Exception as e:
        print(f"Error processing letter {letter}: {e}")
        return []
    finally:
        driver_pool.release_driver(driver)

# SCRAPE COMPANY DETAILS 
def scrape_company_details(name, url, driver_pool):
    driver = driver_pool.get_driver()
    try:
        driver.get(url)
        time.sleep(1)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        data = {'Company Name': name, 'URL': url}

        # Extract main info table
        info_table = soup.find('table', {'class': 'details'})
        if info_table:
            for row in info_table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) == 2:
                    key = cells[0].text.strip().rstrip(':')
                    value = cells[1].text.strip()
                    data[key] = value
                    if key.lower() == 'fax':
                        data['Fax'] = value

        # Extract address and phone
        address_block = soup.find('address')
        if address_block:
            locations = address_block.find_all('ul', class_='location')
            for ul in locations:
                icon = ul.find('span')
                items = ul.find_all('li')
                if len(items) > 1 and icon:
                    icon_class = icon.get('class', [])
                    text = items[1].get_text(strip=True).replace('\n', ' ')
                    if 'glyphicon-map-marker' in icon_class:
                        data['Address'] = text
                    elif 'glyphicon-earphone' in icon_class:
                        data['Phone'] = text

        # Extract Business Type
        category_tag = soup.find('a', class_='alphaSubcat')
        if category_tag:
            data['Business Type'] = category_tag.text.strip()

        return data
    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {'Company Name': name, 'URL': url, 'Error': str(e)}
    finally:
        driver_pool.release_driver(driver)

#  MAIN FUNCTION 
def main():
    parser = argparse.ArgumentParser(description="Scrape companies by starting letters.")
    parser.add_argument('letters', metavar='L', type=str, nargs='+',
                        help='One or more starting letters (e.g., A B C)')
    args = parser.parse_args()

    target_letters = [letter.upper() for letter in args.letters]
    print(f"🔤 Target letters: {target_letters}")

    driver_pool = WebDriverPool(pool_size=5)
    results_all = []
    batch_size = 50
    data_batch = []
    is_first = not os.path.exists(CSV_FILE)

    # Step 1: Scrape company links
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(scrape_letter, letter, driver_pool): letter for letter in target_letters}
        for future in as_completed(futures):
            results = future.result()
            results_all.extend(results)
    print(f"Total company links found: {len(results_all)}")

    # Step 2: Scrape company details and save in batches
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(scrape_company_details, name, link, driver_pool): (name, link)
            for _, name, link in results_all
        }
        for future in as_completed(futures):
            detail = future.result()
            print(f"{detail.get('Company Name')} --> {detail.get('Address', 'No Address')}")
            data_batch.append(detail)
            if len(data_batch) >= batch_size:
                save_to_csv_batch(data_batch, is_first=is_first)
                is_first = False
                data_batch = []

    # Save any remaining data
    if data_batch:
        save_to_csv_batch(data_batch, is_first=is_first)

    print(f"🎉 Scraping complete. Data saved to {CSV_FILE}.")
    driver_pool.close_all()

# ------------------ ENTRY POINT ------------------
if __name__ == "__main__":
    main()