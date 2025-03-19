from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import random
import logging
import shutil
from selenium.common.exceptions import WebDriverException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SeleniumHandler:
    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument("--enable-javascript")
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_argument('--disable-popup-blocking')
        chrome_options.add_argument('--disable-notifications')
        chrome_options.add_argument('--disable-translate')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        logger.info("Initializing Chrome WebDriver")

        # Set Chrome binary path if available
        chrome_binary_path = shutil.which("google-chrome")
        if chrome_binary_path:
            chrome_options.binary_location = chrome_binary_path
            logger.info(f"Chrome binary path: {chrome_binary_path}")
        else:
            logger.warning("Could not find Chrome binary path")

        # Get the latest ChromeDriver version
        try:
            service = Service(ChromeDriverManager().install())
            logger.info("ChromeDriver installed successfully")
        except Exception as e:
            logger.error(f"Error initializing ChromeDriver: {e}")
            raise WebDriverException("Failed to initialize ChromeDriver")

        try:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info(f"ChromeDriver version: {self.driver.capabilities['chrome']['chromedriverVersion']}")
            logger.info(f"Chrome version: {self.driver.capabilities['browserVersion']}")
        except WebDriverException as e:
            logger.error(f"Error initializing Chrome WebDriver: {e}")
            raise e

        wait_time = random.randint(3, 6)
        logger.info(f"Waiting for {wait_time} seconds after initialization")
        WebDriverWait(self.driver, wait_time)

    def crawl_job_links(self, company_config):
        logger.info(f"Crawling job links for company: {company_config['companyCareerLink']}")
        self.driver.get(company_config['companyCareerLink'])

        # Wait for job listings to load
        wait_time = random.randint(3, 6)
        logger.info(f"Waiting for {wait_time} seconds for job listings to load")
        WebDriverWait(self.driver, wait_time).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, company_config['selector']))
        )

        # Find all job links
        logger.info("Finding job elements")
        job_elements = self.driver.find_elements(By.CSS_SELECTOR, company_config['selector'])

        # Extract href attributes
        logger.info("Extracting job links")
        job_links = [element.get_attribute('href') for element in job_elements]

        logger.info(f"Found {len(job_links)} job links")
        return job_links

    def get_page_source(self, url):
        logger.info(f"Getting page source for URL: {url}")
        self.driver.get(url)
        wait_time = random.randint(2, 5)
        logger.info(f"Waiting for {wait_time} seconds after page load")
        WebDriverWait(self.driver, wait_time)
        return self.driver.page_source

    def close(self):
        logger.info("Closing WebDriver")
        self.driver.delete_all_cookies()
        self.driver.quit()
        logger.info("WebDriver closed successfully")
