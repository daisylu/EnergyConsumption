"""
Scraper for Australia energy data: https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data
"""

import pytz
import tqdm
import datetime
import requests
import pandas as pd
import numpy as np
import concurrent.futures


class AEMO:
    def __init__(self):
        super().__init__()

        # List of regions taken from the load website
        self.regions = ["NSW", "QLD", "VIC", "SA", "TAS"]

        # Setting a browser for User-Agent prevents the request from failing
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }

        # Initialize empty list so it can stored failed url downloads
        self.errors = []
        
    
    def create_urls(self, start_date, end_date):
        """
        Create direct links to monthly energy files.
        """

        time_range = pd.date_range(start_date.replace(day=1), end_date, freq="MS")
        time_range = [d.strftime("%Y%m") for d in time_range]

        complete_data_urls = []

        # Data is in files by region and YYYYMM
        for region in self.regions:
            for t in time_range:
                url = f"https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{t}_{region}1.csv"
                complete_data_urls.append(url)
                    
        self.urls = complete_data_urls 


    def download_data(self, url):
        # Send request to website to download data
        r = requests.get(url, headers = self.headers)
        if r.status_code == 200:
            return r
        else:
            self.errors.append(url)


    def parse_data(self, resp):
        df = pd.DataFrame([row.split(",") for row in resp.text.split("\r\n")])
        df.columns = df.iloc[0]
        df = df.iloc[1:].copy()

        # Column names changed over time
        date_name = [c for c in df.columns if "DATE" in c][0]
        region_name = [c for c in df.columns if "REGION" in c][0]
        demand_name = [c for c in df.columns if "DEMAND" in c][0]
        
        df.rename(columns = {date_name: "timestamp", region_name: "pca_abbrev", demand_name: "load_mw"}, inplace=True)
        
        df["timestamp"] = df["timestamp"].str.replace('"', "")
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y/%m/%d %H:%M:%S", utc=True)
        
        # AU data is reported in AEST, which is `local time - 10 hours`
        # subtracting extra hour because data is reported hour ENDING
        df["timestamp"] = df["timestamp"] + datetime.timedelta(hours = -11)
        
        df["load_mw"] = df["load_mw"].astype(float)
        df["source"] = "au"
        
        return df


    def singlethread_download(self):
        results = []

        # Send requests sequentially rather than simultaneously 
        for url in tqdm.tqdm(self.urls, desc="AU - AEMO - S/T download csvs"):
            r = self.download_data(url)
            if r:
                df = self.parse_data(r)
                results.append(df)

        results = pd.concat(results)

        self.raw_results = results


    def multithread_download(self):
        # Max threads are 10 here to not overwhelm their server
        MAX_THREADS = 10
        threads = min(MAX_THREADS, len(self.urls))

        # Multithreading, iterating over url list
        with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
            results = list(tqdm.tqdm(executor.map(self.download_data, self.urls), total=len(self.urls), desc = "AU - AEMO - M/T download csvs"))

        # Wrap results in list, otherwise will be iterator object
        results = list(results)

        # When request fails, a `None` would be returned, so this list gives just the parsed successful responses
        results = [self.parse_data(r) for r in results if r]
        results = pd.concat(results)
        self.raw_results = results


    def filter_results(self, start_date, end_date):
        df = self.raw_results.copy()

        # Data is monthly, so return only the requested dates within downloaded data
        df = df.loc[
            (df["timestamp"] >= start_date.replace(tzinfo=pytz.UTC)) 
            & (df["timestamp"] <= end_date.replace(tzinfo=pytz.UTC))
        ]

        final_cols = ["timestamp", "pca_abbrev", "load_mw", "source"]
        self.parsed_data = df[final_cols].reset_index(drop=True)
    

    def get_data(self, start_date, end_date, multithread, **kwargs):
        self.create_urls(start_date, end_date)
        
        if multithread:
            self.multithread_download()
        else:
            self.singlethread_download()
        
        self.filter_results(start_date, end_date)
        

if __name__ == "__main__":

    aemo = AEMO()
    aemo.get_data(start_date=datetime.datetime(2020,10,1), end_date=datetime.datetime.now(), multithread=False)
    print(aemo.parsed_data.head())
