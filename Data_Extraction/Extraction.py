# Databricks notebook source
# MAGIC %md
# MAGIC ## Extrating Data

# COMMAND ----------

import requests

date = '2022-07-09'
base = 'BRL'
url = f'https://api.apilayer.com/exchangerates_data/{date}&base={base}'


headers= {
  'apikey': 'C6nwqtMj70rFn11HC5DUBBbZ1RY0G6RZ'
}

response = requests.request('GET', url, headers=headers,data = payload)

if response.status_code != 200:
    print('data not found')

result = response.text

# COMMAND ----------

print(result)

# COMMAND ----------


