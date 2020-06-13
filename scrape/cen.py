import requests
from bs4 import BeautifulSoup
from scraper import GetData

class CEN(GetData):
    def __init__(self, years):
        self.years = years
        super().__init__()
        
    @property
    def months(self):
        months = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
        return months
        
    def create_data_labels(self):
        years = [str(year)[2:] for year in self.years]
        
        complete_data_labels = []
        for year in years:
            for month in self.months:
                month_year = month + year
                data_label = f"Operación Real Mensual {month_year}"
                complete_data_labels.append(data_label)
                
        return complete_data_labels

    def create_urls(self, data_labels):
        resp = requests.get("https://sic.coordinador.cl/informes-y-documentos/fichas/operacion-real/")
        soup = BeautifulSoup(resp.text, "html.parser")

        monthly_actual_operations_table = soup.find("div", "accordeon-content-holder").tbody
        rows = monthly_actual_operations_table.find_all("tr")
        data_dict = {}
        for row in rows:
            data_column = row.find_all("td")[1]
            data_label = data_column.get_text()
            data_url = data_column.a.get("href")
            data_dict[data_label] = data_url

        url_dict = {k: v for k, v in data_dict.items() if k in data_labels}
        return url_dict
    
    #TODO: write parsing function
    
    def get_cen_data(self):
        data_labels = self.create_data_labels()
        url_dict = self.create_urls(data_labels)
        url_list = list(url_dict.values())
        results = super().multithread_download(url_list)
        return results

if __name__ == "__main__":
    years = range(2016, 2020)
    
    cen = CEN(years)
    cen_data = cen.get_cen_data()
