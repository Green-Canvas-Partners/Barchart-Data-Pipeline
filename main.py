from Data_Downloader import FuturesDailyDataDownloader
import sys

if __name__ == '__main__':
    for i in range(int(sys.argv[2]), int(sys.argv[3]) + 1):
        fut_data_downloader = FuturesDailyDataDownloader(
            symbol=sys.argv[1],
            contract_month=int(sys.argv[4]),
            contract_year=i
        )

        fut_data_downloader.download_data()
