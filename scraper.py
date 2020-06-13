import requests
import concurrent.futures
from functools import partial

class GetData:
    def __init__(self):
        self.errors = []

    def download_data(self, url, headers):
        resp = requests.get(url, headers = headers)

        if resp.status_code == 200:
            data = resp.text
            return data
        else:
            self.errors.append(url)
    
    def multithread_download(self, url_list, headers = None):
        MAX_THREADS = 300
        threads = min(MAX_THREADS, len(url_list))
        download_data = partial(self.download_data, headers = headers)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
            results = executor.map(download_data, url_list)

        results = list(results)

        return results