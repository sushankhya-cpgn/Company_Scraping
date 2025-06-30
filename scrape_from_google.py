import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pandas as pd
import time
import random
import os

def scrape_glassdoorlink_from_google(
    input_csv_file='output/a.csv',
    headless=False,
    proxy=None
):
    print(f"Reading company names from {input_csv_file}")
    company_names = []

    # Read company names from CSV
    try:
        df = pd.read_csv(input_csv_file)
        if 'Company Name' in df.columns:
            company_names = df['Company Name'].dropna().tolist()
        else:
            company_names = df.iloc[:, 0].dropna().tolist()
        print(f"Found {len(company_names)} company names.")
    except Exception as e:
        print(f"Error! Could not read CSV file: {e}")
        return

    options = uc.ChromeOptions()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # Add proxy if provided
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    # Optional: Fake user-agent handled by undetected_chromedriver internally

    driver = None
    results = []

    try:
        driver = uc.Chrome(options=options, version_main=0)  # auto-selects version
        wait = WebDriverWait(driver, 15)

        for i, company_name in enumerate(company_names):
            print(f"\nProcessing company {i+1}/{len(company_names)}: {company_name}")
            glassdoor_link = 'N/A'
            status = "Failed to find link"

            try:
                search_query = f"{company_name} Glassdoor Bangladesh"
                driver.get("https://www.google.com")
                time.sleep(random.uniform(2, 5))

                # Handle cookie consent
                try:
                    accept_button = wait.until(EC.element_to_be_clickable((
                        By.XPATH,
                        "//button[contains(., 'Accept all') or contains(., 'I agree') or @aria-label='Accept the use of cookies and other data for the purposes described']"
                    )))
                    accept_button.click()
                    print("Accepted cookies.")
                    time.sleep(1)
                except TimeoutException:
                    pass

                # Perform search
                search_box = wait.until(EC.presence_of_element_located((
                    By.CSS_SELECTOR, "textarea[name='q'], input[aria-label='Search'], input[title='Search']"
                )))
                search_box.clear()
                search_box.send_keys(search_query)
                search_box.send_keys(Keys.RETURN)

                wait.until(EC.presence_of_element_located((By.ID, "search")))
                time.sleep(random.uniform(3, 6))

                result_links = driver.find_elements(By.CSS_SELECTOR, "a[href]")
                print(f"Found {len(result_links)} result links, checking for Glassdoor...")

                for link in result_links:
                    href = link.get_attribute('href')
                    if href and 'glassdoor.com' in href:
                        glassdoor_link = href
                        status = "Found Glassdoor link"
                        break

                print(f"Result: {glassdoor_link} - {status}")

            except Exception as e:
                print(f"Error processing '{company_name}': {e}")

            results.append({
                'Company Name': company_name,
                'Glassdoor Link': glassdoor_link,
                'Status': status
            })

            # Random human-like delay before next search
            time.sleep(random.uniform(5, 12))

    except WebDriverException as e:
        print(f"WebDriver error: {e}")
    finally:
        if driver:
            driver.quit()
            print("\nScraping complete. Browser closed.")

        if results:
            os.makedirs('output', exist_ok=True)
            df_out = pd.DataFrame(results)
            output_file = 'output/glassdoor_links_uc.csv'
            df_out.to_csv(output_file, index=False)
            print(f"\nResults saved to {output_file}")

if __name__ == '__main__':
    scrape_glassdoorlink_from_google(
        headless=False,
        proxy=None  
    )
