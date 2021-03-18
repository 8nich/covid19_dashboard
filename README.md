# covid19_dashboard

covid19_dashboard is a tool to get data from different sources, formats the data and inserts the data into a mysql database. 
With grafana the data can be easily visualised. 

## Usage

1. Change the file: config_example.json to config.json and edit it with the credentials of your mysql server. 
2. Setup a Grafana Server or register on https://grafana.com/ for a free hosted one. 
3. Add your mysql server as datasource to your grafana instance. 
4. Import the file: Covd-19-xxx.json to your grafana instance. 
5. Change the datasource from the json file to your datasource. (Grafana will ask you to do so)
6. Dashboard is ready 
![grafik](https://user-images.githubusercontent.com/35539054/111668217-d2f69c00-8815-11eb-95fe-ea871c722606.png)
![grafik](https://user-images.githubusercontent.com/35539054/111668873-8495cd00-8816-11eb-89a0-3c97ca09bfdd.png)
![grafik](https://user-images.githubusercontent.com/35539054/111669040-ae4ef400-8816-11eb-974c-4539b7148c97.png)

## Datasource
- covid-tracker
  - covid-tracker API: https://coronavirus-tracker-api.herokuapp.com/all 

- bfs
  - mortality 2010 - 2019: https://www.bfs.admin.ch/bfsstatic/dam/assets/12607336/master
  - mortality 2020/2021: https://www.bfs.admin.ch/bfs/de/home/statistiken/gesundheit/gesundheitszustand/sterblichkeit-todesursachen.assetdetail.16006453.html (always newest link is taken
  - mortalityrate 1803 - 2019: https://www.pxweb.bfs.admin.ch/sq/c9604a82-ad70-4a42-8409-10cb0a9e9e4c
  - population 1860 - 2019: https://www.pxweb.bfs.admin.ch/sq/1fbb332f-952d-41ad-9b9a-0abd8d98183d

- bag
  - tested: https://www.bag.admin.ch/dam/bag/de/dokumente/mt/k-und-i/aktuelle-ausbrueche-pandemien/2019-nCoV/covid-19-basisdaten-labortests.xlsx.download.xlsx/Dashboard_3_COVID19_labtests_positivity.xlsx 

- openzh
  - covid19 dataset: https://raw.githubusercontent.com/openZH/covid_19/master/COVID19_Fallzahlen_CH_total_v2.csv 

- our world in data
  - owid-covid dataset: https://covid.ourworldindata.org/data/owid-covid-data.xlsx

- wikidata 
  - population data: https://query.wikidata.org/sparql
