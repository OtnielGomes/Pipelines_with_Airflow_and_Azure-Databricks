# Databricks notebook source
# MAGIC %md
# MAGIC ## Extracting data

# COMMAND ----------

# Command used to receive a date from AirFlow
dbutils.widgets.text('execution_date', '')
execution_date = dbutils.widgets.get('execution_date')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports:

# COMMAND ----------

import requests # Library used to access the API responsible for extracting coin data 
from pyspark.sql.functions import lit # Function used to create a column with the dates of the day of data extraction


# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to extract data

# COMMAND ----------

def extrat_data(date, base = 'BRL'):
  
  url = f'https://api.apilayer.com/exchangerates_data/{date}&base={base}'


  headers= {
    'apikey': '*******************************'# Enter your API Key
  }

  params = {'base': base}
  response = requests.request('GET', url, headers = headers, params = params)

  if response.status_code != 200:
    raise Exception('Data not extrating.')

  return response.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function to save data in parquet

# COMMAND ----------

def save_parquet(extracted_conversions):
    
    # Extracting date from records
    year, month, day = extracted_conversions['date'].split('-')

    # Defining the path 
    path = f'/Workspace/Repos/otnielgomes/Pipelines_with_Airflow_and_Azure-Databricks/data/bronze/{year}/{month}/{day}'

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

# MAGIC %md
# MAGIC ## Performing functions

# COMMAND ----------

extracted_conversions = extrat_data(execution_date, base = 'BRL')
save_parquet(extracted_conversions)
