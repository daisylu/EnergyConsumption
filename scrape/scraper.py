import requests
import concurrent.futures
from functools import partial, wraps
from io import BytesIO
from zipfile import ZipFile
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
            # save urls that failed to download for debugging
            self.errors.append(url)
    
    def multithread_download(self, url_list, headers = None):
        # set maximum number of threads to use - arbitrary
        MAX_THREADS = 300
        threads = min(MAX_THREADS, len(url_list))
        
        # for multithreading, use partial to insert headers
        download_data = partial(self.download_data, headers = headers)
        
        # multithreading, iterating over url list
        with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
            results = executor.map(download_data, url_list)

        # wrap results in list, otherwise will be iterator object
        results = list(results)

        return results
    
    @time_it
    def unzip_files(self, results):
        # in the response object, read bytes
        zipfile = ZipFile(BytesIO(results.content))
        
        # get names of all files in the zipped folder
        zip_names = zipfile.namelist()
        
        # open all files within zipped folder - still needs to be parsed
        unzipped_result = [zipfile.open(file_name) for file_name in zip_names]
        
        return unzipped_result