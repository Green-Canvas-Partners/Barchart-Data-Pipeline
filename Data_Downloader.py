from datetime import date, datetime, timedelta
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pickle
import os


class FuturesDailyDataDownloader:
    def __init__(self, symbol: str, contract_month: int, contract_year: int):
        self.key_for_march_contracts = 'h'
        self.key_for_june_contracts = 'm'
        self.key_for_september_contracts = 'u'
        self.key_for_december_contracts = 'z'
        self.start_date = None
        self.end_date = None
        self.symbol = symbol
        self.contract_month = contract_month
        self.contract_year = contract_year
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
    def get_auth_credentials() -> dict:
        with open('/home/mubbashir/Projects/Barchart-Data-Pipeline/'
                  'auth_credentials.pkl', 'rb') as auth_file:
            return pickle.load(auth_file)

    def get_contract_expiration_date(self) -> datetime:
        for i in range(15, 22):
            if datetime(year=self.contract_year, month=self.contract_month, day=i).weekday() == 4:
                return datetime(year=self.contract_year, month=self.contract_month, day=i)

    @staticmethod
    def get_rollover_date(contract_expiration_date: date) -> datetime:
        return (datetime(year=contract_expiration_date.year,
                         month=contract_expiration_date.month,
                         day=contract_expiration_date.day) - timedelta(days=8))

    def get_start_date(self) -> datetime:
        start_date = self.get_contract_expiration_date() - timedelta(days=90)
        return self.get_rollover_date(start_date.date()) + timedelta(days=1)

    def move_file_to_designated_directory(self, start_date_str: str,
                                          end_date_str: str,
                                          contract_quarterly_key: str):
        if contract_quarterly_key == 'h':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}h"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_h_data/"
                      f"{self.symbol}h_{start_date_str}_{end_date_str}.csv")

        elif contract_quarterly_key == 'm':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}m"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_m_data/"
                      f"{self.symbol}m_{start_date_str}_{end_date_str}.csv")
        elif contract_quarterly_key == 'u':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}u"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_u_data/"
                      f"{self.symbol}u_{start_date_str}_{end_date_str}.csv")
        else:
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}z"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_z_data/"
                      f"{self.symbol}z_{start_date_str}_{end_date_str}.csv")

        print('***** File Downloaded and moved to folder *****\n\n')

    def download_data(self):
        start_date = self.get_start_date()
        end_date = self.get_rollover_date(
            self.get_contract_expiration_date()
        )

        if end_date.date().month == 3:
            contract_quarterly_key = self.key_for_march_contracts
        elif end_date.date().month == 6:
            contract_quarterly_key = self.key_for_june_contracts
        elif end_date.date().month == 9:
            contract_quarterly_key = self.key_for_september_contracts
        else:
            contract_quarterly_key = self.key_for_december_contracts

        auth_credentials_dict = FuturesDailyDataDownloader.get_auth_credentials()

        with Chrome(executable_path='./chromedriver', options=self.options) as driver:
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            updated_download_link = self.main_link.replace('ESh00', self.symbol + contract_quarterly_key +
                                                           end_date_str.split('-')[0][2:4])

            """ ------ Loads the Barchart Page ------"""

            driver.get(updated_download_link)
            print('***** Barchart Site Loaded *****')

            """ ------ Opens Login Modal ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            'bc-glyph-user')).click()
            time.sleep(2)

            """ ------ Enters Email ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'email')).send_keys(
                auth_credentials_dict['email']
            )
            print('***** Email entered *****')
            time.sleep(2)

            """ ------ Enters Password ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'password')).send_keys(
                auth_credentials_dict['password']
            )
            print('***** Password entered *****')
            time.sleep(2)

            """ ------ Pressed enter to Login ------"""

            driver.find_element_by_tag_name('button').send_keys(Keys.RETURN)
            print('***** User Authenticated *****')
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
                f"{start_date_str.split('-')[1]}/{start_date_str.split('-')[2]}/{start_date_str.split('-')[0]}"
            )
            print('***** Start Date entered *****')
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
                f"{end_date_str.split('-')[1]}/{end_date_str.split('-')[2]}/{end_date_str.split('-')[0]}"
            )
            print('***** End Date entered *****')
            time.sleep(2)

            """ ----- Clicks on Download ----- """

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            "add")).click()
            time.sleep(10)

            self.move_file_to_designated_directory(start_date_str,
                                                   end_date_str,
                                                   contract_quarterly_key)


class N2FuturesDailyDataDownloader:
    def __init__(self, symbol: str, contract_month: int, contract_year: int):
        self.key_for_march_contracts = 'h'
        self.key_for_june_contracts = 'm'
        self.key_for_september_contracts = 'u'
        self.key_for_december_contracts = 'z'
        self.start_date = None
        self.end_date = None
        self.symbol = symbol
        self.contract_month = contract_month
        self.contract_year = contract_year
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
    def get_auth_credentials() -> dict:
        with open('/home/mubbashir/Projects/Barchart-Data-Pipeline/'
                  'auth_credentials.pkl', 'rb') as auth_file:
            return pickle.load(auth_file)

    def get_contract_expiration_date(self) -> datetime:
        expiration_date = datetime(year=self.contract_year, month=self.contract_month, day=15)
        expiration_date = (expiration_date - timedelta(days=90)).date()

        for i in range(15, 22):
            if datetime(year=expiration_date.year,
                        month=expiration_date.month, day=i).weekday() == 4:
                return datetime(year=expiration_date.year,
                                month=expiration_date.month, day=i)

    @staticmethod
    def get_rollover_date(contract_expiration_date: date) -> datetime:
        return (datetime(year=contract_expiration_date.year,
                         month=contract_expiration_date.month,
                         day=contract_expiration_date.day) - timedelta(days=8))

    def get_start_date(self) -> datetime:
        start_date = self.get_contract_expiration_date() - timedelta(days=90)
        return self.get_rollover_date(start_date.date()) + timedelta(days=1)

    def move_file_to_designated_directory(self, start_date_str: str,
                                          end_date_str: str,
                                          contract_quarterly_key: str):
        if contract_quarterly_key == 'h':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}h"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_h_data/"
                      f"{self.symbol}h_{start_date_str}_{end_date_str}.csv")

        elif contract_quarterly_key == 'm':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}m"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_m_data/"
                      f"{self.symbol}m_{start_date_str}_{end_date_str}.csv")
        elif contract_quarterly_key == 'u':
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}u"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_u_data/"
                      f"{self.symbol}u_{start_date_str}_{end_date_str}.csv")
        else:
            os.system(f"mv /home/mubbashir/Downloads/{self.symbol.lower()}z"
                      f"{end_date_str.split('/')[0][2:4]}*.csv "
                      f"/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_z_data/"
                      f"{self.symbol}z_{start_date_str}_{end_date_str}.csv")

        print('***** File Downloaded and moved to folder *****\n\n')

    def download_data(self):
        start_date = self.get_start_date()
        end_date = self.get_rollover_date(
            self.get_contract_expiration_date()
        )

        if end_date.date().month == 3:
            contract_quarterly_key = self.key_for_june_contracts
        elif end_date.date().month == 6:
            contract_quarterly_key = self.key_for_september_contracts
        elif end_date.date().month == 9:
            contract_quarterly_key = self.key_for_december_contracts
        else:
            contract_quarterly_key = self.key_for_march_contracts

        auth_credentials_dict = FuturesDailyDataDownloader.get_auth_credentials()

        with Chrome(executable_path='./chromedriver', options=self.options) as driver:
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            updated_download_link = self.main_link.replace('ESh00', self.symbol + contract_quarterly_key +
                                                           end_date_str.split('-')[0][2:4])

            """ ------ Loads the Barchart Page ------"""

            driver.get(updated_download_link)
            print('***** Barchart Site Loaded *****')

            """ ------ Opens Login Modal ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            'bc-glyph-user')).click()
            time.sleep(2)

            """ ------ Enters Email ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'email')).send_keys(
                auth_credentials_dict['email']
            )
            print('***** Email entered *****')
            time.sleep(2)

            """ ------ Enters Password ------"""

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.NAME,
                                                                     'password')).send_keys(
                auth_credentials_dict['password']
            )
            print('***** Password entered *****')
            time.sleep(2)

            """ ------ Pressed enter to Login ------"""

            driver.find_element_by_tag_name('button').send_keys(Keys.RETURN)
            print('***** User Authenticated *****')
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
                f"{start_date_str.split('-')[1]}/{start_date_str.split('-')[2]}/{start_date_str.split('-')[0]}"
            )
            print('***** Start Date entered *****')
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
                f"{end_date_str.split('-')[1]}/{end_date_str.split('-')[2]}/{end_date_str.split('-')[0]}"
            )
            print('***** End Date entered *****')
            time.sleep(2)

            """ ----- Clicks on Download ----- """

            WebDriverWait(driver, 10).until(lambda d: d.find_element(By.CLASS_NAME,
                                            "add")).click()
            time.sleep(10)

            self.move_file_to_designated_directory(start_date_str,
                                                   end_date_str,
                                                   contract_quarterly_key)


if __name__ == '__main__':
    fut_data_downloader = N2FuturesDailyDataDownloader('ES', 6, 2000)
    print(fut_data_downloader.get_start_date())
