# Databricks notebook source
# MAGIC %md
# MAGIC ## Instaling libraries:

# COMMAND ----------

# Installing libraries necessary to continue the project
# %pip install kaleido slack-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports:

# COMMAND ----------

from slack_sdk import WebClient
import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuring bot

# COMMAND ----------

# Configuring bot access
slack_token = '******************************'# Insert your slack token
client = WebClient(token = slack_token)

# COMMAND ----------

file_name = dbutils.fs.ls('dbfs:/Workspace/Repos/otnielgomes/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/')[-1].name

# COMMAND ----------

# !pwd

# COMMAND ----------

path = '../../../../../dbfs/Workspace/Repos/otnielgomes/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/' + file_name

# COMMAND ----------

sending_file_csv = client.files_upload_v2(
    channel = 'C06KUC6QU5P',
    title = 'File in CSV in BRL values',
    file = path,
    filename = 'BRL_values.csv',
    initial_comment = 'Attached is the CSV file'
)

# COMMAND ----------

df_BRL_values = ps.read_csv('dbfs:/Workspace/Repos/otnielgomes/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/')

# COMMAND ----------

# Creating directory to save the graphs
#!mkdir graphics

# COMMAND ----------

#!ls

# COMMAND ----------

#df_BRL_values.plot.line(x = 'date', y = 'USD')

# COMMAND ----------

# Creating function to generate graphs
for coin in df_BRL_values.columns[1 : ]:
    fig = df_BRL_values.plot.line(x = 'date', y = coin)
    fig.write_image(f'./graphics/{coin}.png')

# COMMAND ----------

# checking if files were saved
# !ls ./graphics

# COMMAND ----------

# Creating function to send graphs
def send_graphs(coin_graphs):
    sending_graphs = client.files_upload_v2(
        channel = 'C06KUC6QU5P',
        title = 'Coin graphs in BRL',
        file = f'./graphics/{coin_graphs}.png',
        file_name = 'Coin graphs',
        initial_comment = 'Line graphs',
    )

# COMMAND ----------

# Executing function to send graphs
for coin in df_BRL_values.columns[1: ]:
    send_graphs(coin)
