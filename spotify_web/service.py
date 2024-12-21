import logging

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


class WebService:

    def __init__(self):
        self.logger = self.config_logger()
        self.driver = self.get_driver()

    def config_logger(self):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def get_driver(self):
        self.logger.info('Loading chrome driver...')
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--ignore-ssl-errors=true")
        service = Service(executable_path=ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)

    def get_track_plays_count_by_id(self, track_id: str) -> int:
        self.logger.info(f'Get track plays_count of {track_id}')
        self.driver.get(f'https://open.spotify.com/track/{track_id}')
        try:
            element = WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//span[@data-testid="playcount"]'))
            )
        except:
            self.logger.error(f'Can not access {track_id}')
            return

        return int(element.text.replace(',', ''))
