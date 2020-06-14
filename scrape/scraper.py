import requests
import concurrent.futures
from functools import partial, wraps
from io import BytesIO
from zipfile import ZipFile
import tqdm
import time

def time_it(func):
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        start = time.time()
        value = func(*args, **kwargs)
        end = time.time()
        duration = end - start
        if duration >= 60:
            duration /= 60
            time_name = "minutes"
        else:
            time_name = "seconds"
        print(f"{func.__name__}: {round(duration,2)} {time_name}")
        return value
    return wrapped_func
    
class GetData:
    def __init__(self):
        self.errors = []

    def download_data(self, url, headers = None):
        resp = requests.get(url, headers = headers)

        if resp.status_code == 200:
            return resp
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
    
    @time_it
    def unzip_files(self, results):
        zipfile = ZipFile(BytesIO(results.content))
        zip_names = zipfile.namelist()
        unzipped_result = [zipfile.open(file_name) for file_name in zip_names]
        return unzipped_result
            
        
        