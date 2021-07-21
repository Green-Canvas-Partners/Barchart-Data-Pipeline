from os import listdir
from os.path import isfile, join
import pandas as pd


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


if __name__ == '__main__':
    fut_data_preprocessor = FuturesDailyDataPreProcessor()
    fut_data_preprocessor.prepare_data()
