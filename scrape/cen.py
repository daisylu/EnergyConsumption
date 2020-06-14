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
        # use selenium to load site
        url = "https://sic.coordinador.cl/informes-y-documentos/fichas/operacion-real/"
        driver = webdriver.Chrome(executable_path="C:\\Users\\owner\\Downloads\\chromedriver.exe")
        
        driver.get(url)
        time.sleep(3)
        
        # find buttons to expand tables to get daily data, otherwise will not be in html
        expand_buttons = driver.find_elements_by_class_name('accordeon-holder')
        
        # click buttons found on page
        for button in expand_buttons:
            button.click()
            # must set time to pause otherwise button clicks won't work
            time.sleep(3)
            
        # get the fully rendered page source
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # close chrome window
        driver.close()
        driver.quit()
        
        years = [str(year) for year in self.years]
        
        # create dictionary of links to daily data
        url_dict = {a.get("title"):a.get("href") for a in soup.find_all("a")
                   if a.get("title") 
                    and "Diaria" in a.get("title") # string in archived data
                    and a.get("title").split(" ")[-1] in years}

        return url_dict
    
    def parse_cen_data(self, unzipped_results):
        daily_consumption = []
        
        for file in tqdm.tqdm(unzipped_results):
            # parse each excel file from the zipped folder
            df = pd.read_excel(file)
            
            # find index of where data begins
            index_of_date = df.loc[df.iloc[:,0].notnull()].index
            
            # get date from individual daily file
            energy_date = df.iloc[index_of_date].values[0][0]
            
            # get column names that have hours of day
            col_names = df.iloc[index_of_date+1].iloc[0]
            
            # search for row with total consumption - hard-coded row name
            total_consumption = df.loc[df.iloc[:, 1]=="Total Generación SIC"].copy()
            
            # create final df that has date + energy consumption
            total_consumption.columns = col_names
            total_consumption["date"] = energy_date
            final_df = total_consumption[["date"] + list(total_consumption.columns[1:-1])]
            
            # append each daily file to list
            daily_consumption.append(final_df)
        
        # final data of all days for one given year
        daily_consumption = pd.concat(daily_consumption, sort=False)
        
        return daily_consumption

    def get_cen_data(self):
        # create urls to download data
        url_dict = self.create_urls()
        url_list = url_dict.values()
        
        complete_cen_data = []
        
        # run single-threaded because zip files are larger
        for k, v in url_dict.items():
            # print year being retrieved for debugging
            print(k.split(" ")[-1])
            
            # time how long it takes to retrieve zip
            download_data = time_it(super().download_data)
            results = download_data(v)
            
            # after downloading zip, unzip the folder
            unzipped_results = super().unzip_files(results)
            parsed_results = self.parse_cen_data(unzipped_results)
            
            # append each parsed daily file to list 
            complete_cen_data.append(parsed_results)
            
        # final data of all years
        complete_cen_data = pd.concat(complete_cen_data, sort=False).reset_index(drop=True)
        
        return complete_cen_data

if __name__ == "__main__":
    years = [2018]
    
    cen = CEN(years)
    cen_data = cen.get_cen_data()