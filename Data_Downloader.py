from datetime import date
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pickle


class FuturesDataDownloader:
    def __init__(self, symbol):
        self.key_for_march_contracts = 'h'
        self.key_for_june_contracts = 'm'
        self.key_for_september_contracts = 'u'
        self.key_for_december_contracts = 'z'
        self.symbol = symbol
        self.main_link = 'https://www.barchart.com/futures/quotes/ESh00/historical-download'
        self.options = Options()
        # self.options.add_argument('headless')  # Configures to start chrome in headless mode
        self.options.add_argument('--start-maximized')  # Configures to start it with maximum window size
        self.options.add_argument('--disable-extensions')
        self.options.add_argument('--disable-popup-blocking')
        self.options.add_argument(
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/89.0.4389.114 Safari/537.36')

    @staticmethod
    def get_auth_credentials():
        with open('/home/mubbashir/Projects/Barchart-Data-Pipeline/'
                  'auth_credentials.pkl', 'rb') as auth_file:
            return pickle.load(auth_file)

    def download_data(self, start_date=date(2000, 1, 5), end_date=date(2000, 3, 7), contract_quarterly_key='h'):
        auth_credentials_dict = FuturesDataDownloader.get_auth_credentials()

        with Chrome(executable_path='./chromedriver', options=self.options) as driver:
            start_date_str = start_date.strftime('%Y/%m/%d')
            end_date_str = end_date.strftime('%Y/%m/%d')
            updated_download_link = self.main_link.replace('ESh00', self.symbol + contract_quarterly_key +
                                                           end_date_str.split('/')[0][2:4])

            """ ------ Loads the Barchart Page ------"""

            driver.get(updated_download_link)

            """ ------ Opens Login Modal ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            'bc-glyph-user')).click()
            time.sleep(2)

            """ ------ Enters Email ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'email')).send_keys(
                auth_credentials_dict['email']
            )
            time.sleep(2)

            """ ------ Enters Password ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'password')).send_keys(
                auth_credentials_dict['password']
            )
            time.sleep(2)

            """ ------ Pressed enter to Login ------"""

            driver.find_element_by_tag_name('button').send_keys(Keys.RETURN)

            time.sleep(2)

            """ ----- Enters Start Date ------ """

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateFrom')).send_keys(
               Keys.CONTROL + 'a'
            )
            time.sleep(1)

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateFrom')).send_keys(
                Keys.BACKSPACE
            )
            time.sleep(1)

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateFrom')).send_keys(
                f"{start_date_str.split('/')[1]}/{start_date_str.split('/')[2]}/{start_date_str.split('/')[0]}"
            )
            time.sleep(2)

            """ ----- Enters End Date ------ """

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateTo')).send_keys(
                Keys.CONTROL + 'a'
            )
            time.sleep(1)

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateTo')).send_keys(
                Keys.BACKSPACE
            )
            time.sleep(1)

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'dateTo')).send_keys(
                f"{end_date_str.split('/')[1]}/{end_date_str.split('/')[2]}/{end_date_str.split('/')[0]}"
            )
            time.sleep(2)

            """ ----- Clicks on Download ----- """

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            "add")).click()
            time.sleep(10)


if __name__ == '__main__':
    fut_data_dnldr = FuturesDataDownloader('ES')
    fut_data_dnldr.get_auth_credentials()
