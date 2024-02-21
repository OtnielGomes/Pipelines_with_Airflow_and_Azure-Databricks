# Databricks notebook source
# MAGIC %md 
# MAGIC ## Filtering data

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Imports:

# COMMAND ----------

from pyspark.sql.functions import to_date, first, col, round # Functions Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading files

# COMMAND ----------

# File location and file type
file_location = 'dbfs:/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/bronze/*/*/*'
file_type = 'parquet'

# Read files
df_all_data = spark.read.format(file_type) \
    .load(file_location)

#df_all_data.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing and processing data

# COMMAND ----------

# Number of lines
#df_all_data.count() 

# COMMAND ----------

# Coins in dataset
#df_all_data \
    #.select('coin') \
    #.distinct() \
    #.display()

# COMMAND ----------

# Numbers of coins in dataset
#df_all_data \
    #.select('coin') \
    #.distinct() \
    #.count()

# COMMAND ----------

# Choosing currencies according to project request
coins = ['USD', 'EUR', 'GBP']

# COMMAND ----------

# Defined a new dataset with the chosen currencies
df_coin = df_all_data \
    .filter(df_all_data.coin.isin(coins))

#df_coin.limit(10).display()

# COMMAND ----------

# Coins in dataset
#df_coin \
    #.select('coin') \
    #.distinct() \
    #.display()

# COMMAND ----------

# Check the data type of the dataset
#df_coin.printSchema()

# COMMAND ----------

# Adjusting the data column schema
df_coin = df_coin = df_coin \
    .withColumn('date', to_date('date'))

# COMMAND ----------

# Check the data type of the dataset
#df_coin.printSchema()

# COMMAND ----------

# Creating columns for the selected currencies in the dataset
conversion_rate_results = df_coin \
    .groupBy('date') \
    .pivot('coin') \
    .agg(first('rate')) \
    .orderBy('date', ascending = False)

#conversion_rate_results.limit(5).display()

# COMMAND ----------

# Adjusting the conversion rate values to check the value in BRL that we will need to purchase the other currencies
# Creating new dataset
result_value_BRL = conversion_rate_results
#result_value_BRL.limit(5).display()

# COMMAND ----------

# Adjusting the conversion rate values to check the value in BRL that we will need to purchase the other currencies
# creating a FOR to convert the values
for coin in coins:
    result_value_BRL = result_value_BRL \
        .withColumn(coin, round(1 / col(coin), 4))

# COMMAND ----------

#result_value_BRL.limit(5).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Saving data

# COMMAND ----------

# joining dataset partitions
conversion_rate_results = conversion_rate_results.coalesce(1)
result_value_BRL = result_value_BRL.coalesce(1)

# COMMAND ----------

# Datset: conversion_rate_results
# File_location and Filetype
file_location = 'dbfs:/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/conversion_rate_results'
file_type = 'csv'

# Options CSV
first_row_is_header = 'True'
infer_schema = 'True'
delimiter = ','

# Mode
mode = 'overwrite'

# Writing the dataset
conversion_rate_results.write.format(file_type) \
    .mode(mode) \
    .option('header', first_row_is_header) \
    .option('inferSchema', infer_schema) \
    .option('sep', delimiter) \
    .save(file_location)

#display(dbutils.fs.ls(file_location))

# COMMAND ----------

# Datset: result_value_BRL
# File_location and Filetype
file_location = 'dbfs:/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL'
file_type = 'csv'

# Options CSV
first_row_is_header = 'True'
infer_schema = 'True'
delimiter = ','

# Mode
mode = 'overwrite'

# Writing the dataset
result_value_BRL.write.format(file_type) \
    .mode(mode) \
    .option('header', first_row_is_header) \
    .option('inferSchema', infer_schema) \
    .option('sep', delimiter) \
    .save(file_location)

#display(dbutils.fs.ls(file_location))
