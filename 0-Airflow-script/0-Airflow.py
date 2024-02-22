# Databricks notebook source
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Execution-notebook-etl',
    start_date=datetime(2023, 12, 21),
    schedule_interval="0 9 * * *"
) as dag_execution_notebook_extract:
    
    extracting_data = DatabricksRunNowOperator(
        task_id='1-Extract-Data',
        databricks_conn_id ='databricks_default',
        job_id = 487579528000536,# Enter your JOB ID
        notebook_params={
            "execution_date": '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )
    
    transforming_data = DatabricksRunNowOperator(
        task_id='2-Transforming-data',
        databricks_conn_id ='databricks_default',
        job_id = 650119000030685 # Enter your JOB ID
    )

    sending_report = DatabricksRunNowOperator(
        task_id='3-Automating-report',
        databricks_conn_id ='databricks_default',
        job_id = 52127534448739 # Enter your JOB ID
    )
    
    extracting_data >> transforming_data >> sending_report
