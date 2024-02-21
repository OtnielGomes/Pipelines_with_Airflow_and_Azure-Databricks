# Databricks notebook source
# MAGIC %md
# MAGIC ## Instaling libraries:

# COMMAND ----------

# Installing libraries necessary to continue the project
%pip install kaleido slack-sdk

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
slack_token = "xoxb-6655112072118-6661790749650-XtU19hKmzuVZTzGapB1A79WB"
client = WebClient(token = slack_token)

# COMMAND ----------

file_name = dbutils.fs.ls('dbfs:/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/')[-1].name

# COMMAND ----------

!pwd

# COMMAND ----------

path = '../../../../../dbfs/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/' + file_name

# COMMAND ----------

sending_file_csv = client.files_upload_v2(
    channel = 'C06KUC6QU5P',
    title = 'File in CSV in BRL values',
    file = path,
    filename = 'BRL_values.csv',
    initial_comment = 'Attached is the CSV file'
)

# COMMAND ----------

df_BRL_values = ps.read_csv('dbfs:/Workspace/Repos/202307063411@alunos.estacio.br/Pipelines_with_Airflow_and_Azure-Databricks/data/silver/result_value_BRL/').head()

# COMMAND ----------


