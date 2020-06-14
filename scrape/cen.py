import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time
import tqdm
from scrape.scraper import GetData, time_it
import pandas as pd

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
            
        soup = BeautifulSoup(driver.page_source, "html.parser")

        driver.close()
        driver.quit()
        
        years = [str(year) for year in self.years]
        url_dict = {a.get("title"):a.get("href") for a in soup.find_all("a")
                   if a.get("title") 
                    and "Diaria" in a.get("title") 
                    and a.get("title").split(" ")[-1] in years}

        return url_dict
    
    def parse_cen_data(self, unzipped_results):
        daily_consumption = []
        for file in tqdm.tqdm(unzipped_results):
            df = pd.read_excel(file)
            energy_date = df.iloc[1,0]
            col_names = df.iloc[2]
            total_consumption = df.loc[df.iloc[:,1]=="Total Generación SIC"].copy()
            total_consumption.columns = col_names
            total_consumption["date"] = energy_date
            final_df = total_consumption[["date"] + list(total_consumption.columns[1:])]
            daily_consumption.append(final_df)
        
        daily_consumption = pd.concat(daily_consumption, sort=False)
        return daily_consumption

    def get_cen_data(self):
        url_dict = self.create_urls()
        url_list = url_dict.values()
        
        complete_cen_data = []
        # single-threaded
        for url in url_list:
            download_data = time_it(super().download_data)
            results = download_data(url)
            unzipped_results = super().unzip_files(results)
            parsed_results = self.parse_cen_data(unzipped_results)
            complete_cen_data.append(parsed_results)
            
        complete_cen_data = pd.concat(complete_cen_data, sort=False).reset_index(drop=True)
        
        return complete_cen_data

if __name__ == "__main__":
    years = [2018]
    
    cen = CEN(years)
    cen_data = cen.get_cen_data()