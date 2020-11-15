# Energy Consumption Data

## Purpose
This repo retrieves the hourly energy consumption data for Australia and Brazil.
* Australia data downloaded from [AEMO](https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/aggregated-data). 
* Brazil data downloaded from [Operador Nacional do Sistema Eléctrico](http://www.ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/curva_carga_horaria.aspx).

## Directory
* `scrape/`
    * `aemo.py` - methods to scrape AU data from AEMO 
    * `ons.py` - methods to scrape BR data from ONS
    
## Expected Data

Below is a sample output for Brazil:

| timestamp |	pca_abbrev	 | load_mw |	source |
|---|---|---|--|
2020-08-01 03:00:00+00:00 | Sul | 9309.50002331 | br
2020-08-01 04:00:00+00:00 |	Sul | 8510.33800256 | br
2020-08-01 05:00:00+00:00 | Sul | 8061.871 | br
2020-08-01 06:00:00+00:00 | Sul | 7884.583 | br
2020-08-01 07:00:00+00:00 | Sul | 7846.42199999 | br