import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time
from scraper import GetData

class CEN(GetData):
    def __init__(self, years):
        self.years = years
        super().__init__()

    def create_urls(self):
        url = "https://sic.coordinador.cl/informes-y-documentos/fichas/operacion-real/"
        driver = webdriver.Chrome(executable_path="C:\\Users\\owner\\Downloads\\chromedriver.exe")
        
        driver.get(url)
        time.sleep(3)
        
        expand_buttons = driver.find_elements_by_class_name('accordeon-holder')
        for button in expand_buttons:
            button.click()
            time.sleep(3)
            
        soup = BeautifulSoup(driver.page_source, "lxml")

        driver.close()
        driver.quit()
        
        years = [str(year) for year in self.years]
        url_dict = {a.get("title"):a.get("href") for a in soup.find_all("a")
                   if a.get("title") 
                    and "Diaria" in a.get("title") 
                    and a.get("title").split(" ")[-1] in years}

        return url_dict
    
    #TODO: write parsing function
    def parse_cen_data(self, unzipped_results):
        for file in unzipped_results:
            file = pd.read_excel(file)
    
    def get_cen_data(self):
        url_dict = self.create_urls()
        results = super().multithread_download(url_dict.values())
        unzipped_results = super().unzip_files(results)
        
        return unzipped_results

if __name__ == "__main__":
    years = range(2016, 2020)
    
    cen = CEN(years)
    cen_data = cen.get_cen_data()