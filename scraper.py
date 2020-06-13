import requests
import pandas as pd
import concurrent.futures

def create_aemo_urls(years, regions):
    complete_data_urls = []
    for region in regions:
        for year in years:
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                year_month = str(year) + month_str
                url = f"https://www.aemo.com.au/aemo/data/nem/priceanddemand/PRICE_AND_DEMAND_{year_month}_{region}1.csv"
                complete_data_urls.append(url)
    return complete_data_urls 
    
def get_aemo_consumption(data_link):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    }
    resp = requests.get(data_link, headers=headers)
    
    if resp.status_code == 200:
        data = resp.text
        return data
    else:
        print(f"Error for {data_link}")

def parse_aemo_consumption(resp_text):
    data = pd.DataFrame([row.split(",") for row in resp_text.split("\r\n")])
    final_df = data.iloc[1:]
    final_df.columns = data.iloc[0]
    return final_df
    
def multithread_download(urls):
    MAX_THREADS = 300
    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(get_aemo_consumption, urls)
    
    results = list(results)
    
    return results

if __name__ == "__main__":
    aemo_urls = create_aemo_urls(range(2016,2020), ["NSW", "QLD", "VIC", "SA", "TAS"])
    au_data = multithread_download(aemo_urls)
    parsed_au_data = [parse_aemo_consumption(r) for r in au_data]
    parsed_au_data = pd.concat(parsed_au_data).reset_index(drop=True)