# covid19_dashboard

covid19_dashboard is a tool to get data from different sources, formats the data and inserts the data into a mysql database. 
With grafana the data can be easily visualised. 

#Usage

1. change the file: config_example.json to config.json and edit it with the credentials of your mysql server. 
2. Setup a Grafana Server or Register on https://grafana.com/ for a free hosted one
3. Add a mysql datasource to your grafana instance
4. Import the file: Covd-19-xxx.json to your grafana instance
5. change the datasource from the json file to your datasource
6. Dashboard is ready 
