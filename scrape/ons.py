"""
Scraper for Brazil energy data: http://www.ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/curva_carga_horaria.aspx
"""

import re
import io
import csv
import time
import requests
import datetime
import pandas as pd

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

class ONS:
    def __init__(self):
        super().__init__()
        self.graph_type = "comparativo"
        self.timeout = 5
        self.errors = []
        
    
    @property
    def graph_element(self):
        """
        The Tableau graph has three tabs: Simples, Comparativo, and Temporal.
        The graph that has load for all regions we are interested in is Comparativo.
        """
        return {"comparativo": "view7056255131361021128_2936287492945516384"}
       
        
    def _launch_ons_site(self):
        url = "https://tableau.ons.org.br/t/ONS_Publico/views/CurvadeDemandaHorria/HistricoCurvadeDemandaHorria?:embed=y&:display_count=n&:showAppBanner=false&:showVizHome=n&:origin=viz_share_link"
        self.driver.get(url)

        
    def _move_to_comparativo_graph(self):
        self.driver.find_element_by_xpath(
            '//*[@class="tabFlipboardNavNext tab-widget ArrowLarge"]'
        ).click()
    
    def _change_date_range(self, start_date, end_date):
        start_date_str =  start_date.strftime("%m/%d/%Y")
        end_date_str = end_date.strftime("%m/%d/%Y")
        
        # Ensure the query box is loaded before proceeding  
        WebDriverWait(self.driver, self.timeout).until(
            EC.presence_of_element_located((By.CLASS_NAME , "QueryBox"))
        )
        
        # The QueryBox element takes data dates, so set the element to an object
        dates = self.driver.find_elements_by_class_name("QueryBox")

        # The second QueryBox element is the start date
        start_date = dates[1]
        # The box has placeholder text that needs to be manually deleted
        for _ in range(10):
            start_date.send_keys(Keys.BACKSPACE)
        # After deleting, insert the desired start date
        start_date.send_keys(start_date_str)
        # Submit the start date
        start_date.send_keys(Keys.ENTER)

        # Allow the web page to load before inputting the end date
        time.sleep(30)
        
        # The first QueryBox element is the end date, follow the same process
        end_date = dates[0]
        end_date.clear()
        end_date.send_keys(Keys.CONTROL + "a")
        time.sleep(3)
        for _ in range(10):
            end_date.send_keys(Keys.BACKSPACE)
        end_date.send_keys(end_date_str)
        end_date.send_keys(Keys.ENTER)

        
    def _select_all_data_points(self):
        """
        In order to download the data from the graph, all the data points need to be highlighted
        """
        
        # Select the "Comparativo" graph
        graph_element = self.driver.find_element_by_id(self.graph_element[self.graph_type])
        
        # Scroll down on the page, otherwise can't select all the data points
        graph_element.send_keys(Keys.PAGE_DOWN)
        time.sleep(10)

        # Create a sequence of actions for the driver to perform
        actions = webdriver.ActionChains(self.driver)
        # Define where the action should be positioned, in this case, it is in the upper corner of the graph
        actions.move_to_element_with_offset(graph_element, 0, 0)
        # Have the browser click and hold as if to "drag the cursor"
        actions.click_and_hold(on_element=None)
        # Define the position that the cursor should drag to
        actions.move_by_offset(graph_element.size["width"]-2, graph_element.size["height"]-2)
        # Let go of the cursor
        actions.release()
        
        # Perform the actions listed above
        actions.perform()
        
        
    def _open_download_window(self):
        """
        After highlighting all the data points, navigate to the download button on the Tableau graph
        """

        # Identify where the download button is
        self.driver.find_element_by_id("download-ToolbarButton").click()
        
        # Allow everything to load
        time.sleep(10)

        # Once the download menu pops up, click the "Data" button
        self.driver.find_element_by_xpath(
            '//*[@data-tb-test-id="DownloadData-Button"]'
        ).click() 

        
    def _locate_data_link(self):
        """
        After clicking the data button, a new pop-up will appear with a table of the requested data
        """
        
        # Switch from the main browser to the popup
        popup = self.driver.window_handles[1]
        self.driver.switch_to.window(popup)
        
        # The page source is the underlying HTML
        soup = BeautifulSoup(self.driver.page_source, "html.parser")

        # Look for the link to download the data
        data_links = soup.findAll("a", attrs={'href': re.compile("^https://")})
        summary_data_link = data_links[0].get("href")
        self.full_data_link = summary_data_link
    
    
    def _download_final_data(self):
    	# Download the data table from the pop-up
        r = requests.get(self.full_data_link)
        
        if r.status_code == 200:
        	# Parse the data into a usable format
            csvio = io.StringIO(r.text, newline="")
            self.raw_data = []
            for row in csv.DictReader(csvio):
                self.raw_data.append(row)
            
            
    def data_selection_wait(self, start_date, end_date):
        """
        The larger the time range between the requested start and end dates, the longer the page will take to load.
        Calculate a simple value to insert as the wait time based on this time range.
        """
        
        start_year = start_date.year
        end_year = end_date.year
        years = int(end_year) - int(start_year)
        
        return years * 30

    @property
    def column_name_dict(self):
        return {
            'Data Escala de Tempo 1 CDH Comp 3': "timestamp", 
            'Selecione Tipo de CDH Comp 3': "load_mw", 
            'Subsistema': "pca_abbrev",
        }
        
        
    def _parse_data(self):
        df = pd.DataFrame(self.raw_data)
        df.rename(columns = self.column_name_dict, inplace = True)
        df.dropna(subset = ["pca_abbrev"], inplace = True)
        df = df.loc[df["pca_abbrev"]!=""]
        
        df["source"] = "br"
        
        # utc conversion - hardcoded because unclear how DST is in original data
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%m/%d/%Y %I:%M:%S %p", utc=True)
        df["timestamp"] = df["timestamp"] + datetime.timedelta(hours=3)

        columns = ["timestamp", "pca_abbrev", "load_mw", "source"]
        self.parsed_data = df[columns]    

        
    def get_data(self, start_date, end_date, **kwargs):
        try:
            self.driver = webdriver.Chrome(executable_path = ChromeDriverManager().install())
            # selenium navigation
            self._launch_ons_site()
            time.sleep(5)
            self._move_to_comparativo_graph()
            time.sleep(5)
            self._change_date_range(start_date, end_date)
            time.sleep(30)
            
            # Most critical section - if no data is selected, no exports
            self._select_all_data_points()
            time.sleep(
                self.data_selection_wait(start_date, end_date)
            )
            
            self._open_download_window()
            time.sleep(30)
            self._locate_data_link()
            self._download_final_data()
            self.driver.quit()
        except Exception as e:
            self.errors.append(f"BR - ONS - Webscraping error: {e}")
        
        self._parse_data()
                

if __name__ == "__main__":

    ons = ONS()
    ons.get_data(datetime.datetime(2020,8,1), datetime.datetime.now())
    
    print(ons.parsed_data.head())