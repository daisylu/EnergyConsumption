import pandas as pd
import datetime
from scrape.scraper import GetData

class AEMO(GetData):
    def __init__(self, regions, years):
        self.regions = regions
        self.years = years
        super().__init__()
    
    @property
    def headers(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"
        }
        return headers
    
    def create_urls(self):
        complete_data_urls = []
        for region in self.regions:
            for year in self.years:
                for month in range(1, 13):
                    month_str = str(month).zfill(2)
                    year_month = str(year) + month_str
                    url = f"https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{year_month}_{region}1.csv"
                    complete_data_urls.append(url)
        return complete_data_urls 

    def format_date_string(self, date_str):
        if date_str:
            date_str = date_str.replace("\"", "")
            date_str = datetime.datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
        else:
            date_str = None
        return date_str

    def parse_aemo_data(self, resp):
        resp_text = resp.text
        # convert to df
        data = pd.DataFrame([row.split(",") for row in resp_text.split("\r\n")])
        final_df = data.iloc[1:].copy()
        final_df.columns = data.iloc[0]

        # fix datatypes
        final_df["SETTLEMENTDATE"] = final_df["SETTLEMENTDATE"].map(self.format_date_string)
        float_cols = ["TOTALDEMAND", "RRP"]
        for col in float_cols:
            final_df[col] = final_df[col].astype(float)

        return final_df
    
    def get_aemo_data(self):
        urls = self.create_urls()
        results = super().multithread_download(urls, self.headers)
        parsed_results = [self.parse_aemo_data(r) for r in results]
        parsed_results = pd.concat(parsed_results).reset_index(drop=True)
        return parsed_results

if __name__ == "__main__":
    years = range(2016, 2020)
    regions = ["NSW", "QLD", "VIC", "SA", "TAS"]
    
    aemo = AEMO(regions, years)
    aemo_data = aemo.get_aemo_data()
    len(aemo.errors)
    
    aemo_data.head()
