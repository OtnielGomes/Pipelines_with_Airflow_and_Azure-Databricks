# Databricks notebook source
dbutils.widgets.text("data_execucao", "")
data_execucao = dbutils.widgets.get("data_execucao")


# COMMAND ----------

import requests
from pyspark.sql.functions import lit


# COMMAND ----------

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

def save_parquet(extracted_conversions):
    
    # Extracting date from records
    year, month, day = extracted_conversions['date'].split('-')

    # Defining the path 
    path = f'/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/bronze/{year}/{month}/{day}'

    # Saving data to a variable
    response = [(coin, float(rate)) for coin, rate in extracted_conversions['rates'].items()]

    # Creating DataFrame
    df_conversions = spark.createDataFrame(response, schema = ['coin', 'rate'])

    # Creating a date column
    df_conversions = df_conversions.withColumn('date', lit(f'{year}-{month}-{day}'))

    # Save DataFrame in parquet
   
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

extracted_conversions = extrat_data(data_execucao, base = 'BRL')
save_parquet(extracted_conversions)

# COMMAND ----------


