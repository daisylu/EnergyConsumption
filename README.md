# Energy Consumption Data

## Purpose
This repo retrieves the hourly energy consumption data for Australia and Chile.
* Australia data downloaded from [AEMO](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data). 
* Chile data downloaded from [Coordinador Eléctrico Nacional](https://sic.coordinador.cl/informes-y-documentos/fichas/operacion-real/).

## Directory
* `data/`
    * contains the final csvs extracted
* `scrape/`
    * `aemo.py` - methods to scrape AU data from AEMO 
    * `cen.py` - methods to scrape CL data from CEN
    * `scrape.py` - parent class for downloading and unzipping data
* `energy_analysis.ipynb`
    * runs the scraper code, saves data to csvs, create graphs showing load over time