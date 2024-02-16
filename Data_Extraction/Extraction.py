# Databricks notebook source
# MAGIC %md
# MAGIC ## Extrating Data

# COMMAND ----------

import requests

def extrat_data(date, base = 'BRL'):
  
  url = f'https://api.apilayer.com/exchangerates_data/{date}&base={base}'


  headers= {
    'apikey': 'C6nwqtMj70rFn11HC5DUBBbZ1RY0G6RZ'
  }

  params = {'base': base}
  response = requests.request('GET', url, headers = headers, params = params)

  if response.status_code != 200:
    raise Exception('Data not extrating.')

  return response.json()

# COMMAND ----------

extrat_data('2022-09-07', base = 'BRL')

# COMMAND ----------

year, month, day = extrat_data('2022-09-07', base = 'BRL')['date'].split('-')
print(year, month, day)

# COMMAND ----------

dbutils.fs.ls('dbfs:/')

# COMMAND ----------

path = f'/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/bronze/{year}/{month}/{day}'
path

# COMMAND ----------

def data_for_dataframe(data_json):
    data_tuple = [(coin, float(rate)) for coin, rate in data_json['rates'].items()]
    return data_tuple

# COMMAND ----------

response = data_for_dataframe(extrat_data('2022-09-07', base = 'BRL'))

# COMMAND ----------

spark.createDataFrame(response, schema = ['coin', 'rate']).show(n = 5)

# COMMAND ----------

df_conversions = spark.createDataFrame(response, schema = ['coin', 'rate'])

# COMMAND ----------

from pyspark.sql.functions import lit
df_conversions = df_conversions.withColumn('date', lit(f'{year}-{month}-{day}'))
df_conversions.limit(5).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Save DataFrame

# COMMAND ----------

# local file and type
file_local = path
file_type = 'parquet'

# mode
mode = 'overwrite'

# saving
df_conversions.write.format(file_type)\
    .mode(mode)\
    .save(file_local)

# COMMAND ----------


