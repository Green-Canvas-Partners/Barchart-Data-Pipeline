from os import listdir
from os.path import isfile, join
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


class FuturesDailyDataPreProcessor:
    def __init__(self):
        self.dir_path_for_h_contracts = '/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_h_data'
        self.dir_path_for_m_contracts = '/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_m_data'
        self.dir_path_for_u_contracts = '/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_u_data'
        self.dir_path_for_z_contracts = '/home/mubbashir/Projects/Barchart-Data-Pipeline/fut_daily_z_data'
        self.dir_path_for_processed_data = '/home/mubbashir/Projects/Barchart-Data-Pipeline/' \
                                           'fut_daily_processed_data'
        self.files_to_process_for_h_contracts = []
        self.files_to_process_for_m_contracts = []
        self.files_to_process_for_u_contracts = []
        self.files_to_process_for_z_contracts = []
        self.dfs_list_for_h_contracts = []
        self.dfs_list_for_m_contracts = []
        self.dfs_list_for_u_contracts = []
        self.dfs_list_for_z_contracts = []

        self.main_df_for_h_contracts = pd.DataFrame()
        self.main_df_for_m_contracts = pd.DataFrame()
        self.main_df_for_u_contracts = pd.DataFrame()
        self.main_df_for_z_contracts = pd.DataFrame()

        self.main_df = pd.DataFrame()

    def get_files_name(self):
        self.files_to_process_for_h_contracts = [file for file in listdir(self.dir_path_for_h_contracts) if
                                                 isfile(join(self.dir_path_for_h_contracts, file))]
        self.files_to_process_for_m_contracts = [file for file in listdir(self.dir_path_for_m_contracts) if
                                                 isfile(join(self.dir_path_for_m_contracts, file))]
        self.files_to_process_for_u_contracts = [file for file in listdir(self.dir_path_for_u_contracts) if
                                                 isfile(join(self.dir_path_for_u_contracts, file))]
        self.files_to_process_for_z_contracts = [file for file in listdir(self.dir_path_for_z_contracts) if
                                                 isfile(join(self.dir_path_for_z_contracts, file))]

    def load_all_dfs(self):
        for file in self.files_to_process_for_h_contracts:
            self.dfs_list_for_h_contracts.append(pd.read_csv(join(self.dir_path_for_h_contracts, file)))

        for file in self.files_to_process_for_m_contracts:
            self.dfs_list_for_m_contracts.append(pd.read_csv(join(self.dir_path_for_m_contracts, file)))

        for file in self.files_to_process_for_u_contracts:
            self.dfs_list_for_u_contracts.append(pd.read_csv(join(self.dir_path_for_u_contracts, file)))

        for file in self.files_to_process_for_z_contracts:
            self.dfs_list_for_z_contracts.append(pd.read_csv(join(self.dir_path_for_z_contracts, file)))

    def change_date_format(self):
        for df in self.dfs_list_for_h_contracts:
            df['Time'] = pd.to_datetime(df['Time'], errors='coerce')

        for df in self.dfs_list_for_m_contracts:
            df['Time'] = pd.to_datetime(df['Time'], errors='coerce')

        for df in self.dfs_list_for_z_contracts:
            df['Time'] = pd.to_datetime(df['Time'], errors='coerce')

        for df in self.dfs_list_for_u_contracts:
            df['Time'] = pd.to_datetime(df['Time'], errors='coerce')

    def drop_interest_rate_column(self):
        for df in self.dfs_list_for_h_contracts:
            df.drop(columns='Open Int', inplace=True)

        for df in self.dfs_list_for_m_contracts:
            df.drop(columns='Open Int', inplace=True)

        for df in self.dfs_list_for_u_contracts:
            df.drop(columns='Open Int', inplace=True)

        for df in self.dfs_list_for_z_contracts:
            df.drop(columns='Open Int', inplace=True)

    def append_all_dfs_together(self):
        self.main_df_for_h_contracts = self.dfs_list_for_h_contracts[0]
        self.main_df_for_m_contracts = self.dfs_list_for_m_contracts[0]
        self.main_df_for_u_contracts = self.dfs_list_for_u_contracts[0]
        self.main_df_for_z_contracts = self.dfs_list_for_z_contracts[0]

        for i in range(1, len(self.dfs_list_for_h_contracts)):
            self.main_df_for_h_contracts = self.main_df_for_h_contracts.append(
                self.dfs_list_for_h_contracts[i]
            )

        for i in range(1, len(self.dfs_list_for_m_contracts)):
            self.main_df_for_m_contracts = self.main_df_for_m_contracts.append(
                self.dfs_list_for_m_contracts[i]
            )

        for i in range(1, len(self.dfs_list_for_u_contracts)):
            self.main_df_for_u_contracts = self.main_df_for_u_contracts.append(
                self.dfs_list_for_u_contracts[i]
            )

        for i in range(1, len(self.dfs_list_for_z_contracts)):
            self.main_df_for_z_contracts = self.main_df_for_z_contracts.append(
                self.dfs_list_for_z_contracts[i]
            )

    def prepare_data(self):
        self.get_files_name()
        self.load_all_dfs()
        self.change_date_format()
        self.drop_interest_rate_column()
        self.append_all_dfs_together()

        self.main_df_for_h_contracts = self.main_df_for_h_contracts.drop_duplicates()
        self.main_df_for_m_contracts = self.main_df_for_m_contracts.drop_duplicates()
        self.main_df_for_u_contracts = self.main_df_for_u_contracts.drop_duplicates()
        self.main_df_for_z_contracts = self.main_df_for_z_contracts.drop_duplicates()

        self.main_df_for_h_contracts = self.main_df_for_h_contracts.sort_values(by='Time',
                                                                                ignore_index=False)
        self.main_df_for_m_contracts = self.main_df_for_m_contracts.sort_values(by='Time',
                                                                                ignore_index=False)
        self.main_df_for_u_contracts = self.main_df_for_u_contracts.sort_values(by='Time',
                                                                                ignore_index=False)
        self.main_df_for_z_contracts = self.main_df_for_z_contracts.sort_values(by='Time',
                                                                                ignore_index=False)

        self.main_df = self.main_df_for_h_contracts
        self.main_df = self.main_df.append(self.main_df_for_m_contracts)
        self.main_df = self.main_df.append(self.main_df_for_u_contracts)
        self.main_df = self.main_df.append(self.main_df_for_z_contracts)

        self.main_df = self.main_df.drop_duplicates()
        self.main_df = self.main_df.sort_values(by='Time')

        self.main_df.to_csv(f"{self.dir_path_for_processed_data}/processed_data.csv", index=False)


class FuturesRatioBasedRollover:
    def __init__(self, path_to_csv: str):
        self.path_to_csv = path_to_csv
        self.df = pd.DataFrame()

    def load_df(self):
        self.df = pd.read_csv(self.path_to_csv)
        self.df['Time'] = pd.to_datetime(self.df['Time'])

    @staticmethod
    def get_expiration_date(year, month):
        for i in range(15, 22):
            if datetime(year=year,
                        month=month,
                        day=i).weekday() == 4:
                return datetime(year=year,
                                month=month,
                                day=i)

    @staticmethod
    def is_rollover_date(datetime_obj: datetime) -> bool:
        if datetime_obj.month in [3, 6, 9, 12]:
            if (FuturesRatioBasedRollover.get_expiration_date(
                    year=datetime_obj.year,
                    month=datetime_obj.month
            ) - timedelta(days=8)) == datetime_obj:
                return True
            return False

        return False

    def multiply_ratio_with_previous_contracts(self, df_index: int, ratio: float):
        for i in range(df_index):
            self.df.at[i, 'Open'] *= ratio
            self.df.at[i, 'High'] *= ratio
            self.df.at[i, 'Low'] *= ratio
            self.df.at[i, 'Last'] *= ratio

    def adjust_for_continuous_data(self):
        self.load_df()

        for i in range(self.df.shape[0]):
            if FuturesRatioBasedRollover.is_rollover_date(
                self.df['Time'].iloc[i]
            ):
                self.multiply_ratio_with_previous_contracts(
                    i,
                    (self.df['Open'].iloc[i + 1] / self.df['Last'].iloc[i])
                )

        self.df.to_csv('/home/mubbashir/Projects/Barchart-Data-Pipeline/temp.csv',
                       index=False)


class InterpolateFuturesDailyData:
    def __init__(self, path_to_csv: str):
        self.df_path = path_to_csv
        self.df = pd.DataFrame()

    @staticmethod
    def is_data_missing(date_1: datetime, date_2: datetime) -> bool:
        if (date_2 - date_1) > timedelta(days=1):
            return True
        return False

    def load_df(self):
        self.df = pd.read_csv(self.df_path)
        self.df['Time'] = pd.to_datetime(self.df['Time'])

    def fill_nans_for_missing_data(self):
        for i in range(1000000):
            if i + 1 == self.df.shape[0]:
                break

            temp_df = self.df.iloc[i]

            if InterpolateFuturesDailyData.is_data_missing(
                self.df['Time'].iloc[i],
                self.df['Time'].iloc[i + 1]
            ):
                temp_df['Time'] = temp_df['Time'] + timedelta(days=1)
                temp_df['Open'] = np.nan
                temp_df['High'] = np.nan
                temp_df['Low'] = np.nan
                temp_df['Last'] = np.nan

                self.df = self.df.append(temp_df)
                self.df = self.df.sort_values(by='Time', ignore_index=True)
                i = 0

    def interpolate(self):
        self.load_df()
        self.fill_nans_for_missing_data()
        self.df['Open'] = self.df['Open'].interpolate(method='linear')
        self.df['High'] = self.df['High'].interpolate(method='linear')
        self.df['Low'] = self.df['Low'].interpolate(method='linear')
        self.df['Last'] = self.df['Last'].interpolate(method='linear')
        self.df['Volume'] = self.df['Volume'].interpolate(method='linear')

        self.df.to_csv('/home/mubbashir/Projects/Barchart-Data-Pipeline/temp.csv',
                       index=False)


if __name__ == '__main__':
    # fut_data_preprocessor = FuturesDailyDataPreProcessor()
    # fut_data_preprocessor.prepare_data()
    fut_rollover = FuturesRatioBasedRollover(
        '/home/mubbashir/Projects/Barchart-Data-Pipeline/'
        'ES_Fut_Daily_1999-12-13_2021-07-21.csv'
    )

    fut_rollover.adjust_for_continuous_data()

    fut_interpolate = InterpolateFuturesDailyData(
        '/home/mubbashir/Projects/Barchart-Data-Pipeline/'
        'ES_Fut_Daily_1999-12-13_2021-07-21.csv'
    )

    fut_interpolate.interpolate()
