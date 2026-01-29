# Databricks notebook source
# AUTO-GENERATED FROM MAGE
# Pipeline: query_result_bq_automation

# COMMAND ----------

dbutils.widgets.get("sheet_url",'')
dbutils.widgets.get("file_name",'')
dbutils.widgets.get("recepient_email",'')

# COMMAND ----------

sheet_url=dbutils.widgets.get("sheet_url")
file_name=dbutils.widgets.get("file_name")
recepient_email=dbutils.widgets.get("recepient_email")

# COMMAND ----------

import sys
import os

notebook_dir = os.path.dirname(os.path.abspath('__file__'))
mage_ai_path = os.path.join(
    notebook_dir,
    '/Volumes/dealshare_prod/default/mage-ai/mage-ai/mage-ai-master/'
)

print(f"Adding to sys.path: {mage_ai_path}")
if mage_ai_path not in sys.path:
    sys.path.insert(0, mage_ai_path)

import mage_ai
print(f"Successfully imported mage_ai from: {mage_ai.__file__}")

# COMMAND ----------

PIPELINE_NAME = "query_result_bq_automation"
print(f"-----------Starting pipeline: {PIPELINE_NAME}-----------")

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** query_loading_from_sheets
# MAGIC **Type:** data_loader

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
import pandas as pd
from mage_ai.io.google_sheets import GoogleSheets
from mage_ai.io.s3 import S3
import json
import uuid
from google.cloud import bigquery
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_big_query(*args, **kwargs):
    """
    Template for loading data from a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.
    """

    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    json_key_path = "default_repo/dealshare-d82f7-6ab33267157b.json"

    # Initialize BigQuery client
    client = bigquery.Client.from_service_account_json(json_key_path)


    query_doc = GoogleSheets.with_config(ConfigFileLoader(config_path, config_profile)).load(
        sheet_url=kwargs["sheet_url"],
        header_rows=0
    )
    temp_table_id=str(uuid.uuid4().hex)
    try:
        query = query_doc.iloc[0, 0]  # Extracts the first (and only) cell
        query = str(query)
        if query.endswith(";"):
                query = query[:-1]


        
        query = f"create table dealshare-d82f7.ims_analytics.temp_{temp_table_id} as {query};"
        print(query)

        # BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(query)
        query_job = client.query(query)

        while not query_job.done():
            print("Waiting for query to complete...")
            time.sleep(5)  
        
        query_job.result()

        print("Create Query completed!")

        chunk_size = 500000
        offset = 0
        df = pd.DataFrame()
        while True:
            fetch_query = f"SELECT * FROM dealshare-d82f7.ims_analytics.temp_{temp_table_id} LIMIT {chunk_size} OFFSET {offset};"
            # df_chunk = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(fetch_query)
            query_job = client.query(fetch_query)
            
            while not query_job.done():
                print("Waiting for query to complete...")
                time.sleep(5)  

            print("Chunk Fetch Query completed!")

            df_chunk = query_job.to_dataframe()
            if df_chunk.empty:
                print("No more data to fetch")
                break  
            df = pd.concat([df, df_chunk], ignore_index=True)
            offset += chunk_size 
        
        remove_temp=f"DROP TABLE dealshare-d82f7.ims_analytics.temp_{temp_table_id};"
        # BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(remove_temp)

        query_job = client.query(remove_temp)
        
        while not query_job.done():
            print("Waiting for query to complete...")
            time.sleep(5)  

        print("Drop Query completed!")

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)  # Convert to DataFrame if needed

        # df = df.drop_duplicates()
        print("Dataframe shape " +str(df.shape))

        return (df,"")
    except Exception as e:

        df=pd.DataFrame()
        return (df,str(e))


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# COMMAND ----------

query_loading_from_sheets = load_data_from_big_query(sheet_url=sheet_url)
print(query_loading_from_sheets)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** exporting_results_to_s3
# MAGIC **Type:** transformer

# COMMAND ----------

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_sheets import GoogleSheets
from mage_ai.io.s3 import S3
from os import path
import os
import pandas as pd
import json
import tempfile
import ast
import io
from email.message import EmailMessage
from email.utils import make_msgid
import smtplib
import boto3



@transformer
def transform(data,*args, **kwargs):
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    config_profile = 'AWS_creds'
    bucket_name = 'ds-mage-redshift'
    s3 = S3.with_config(ConfigFileLoader(config_path, config_profile))

    s3_client = boto3.client('s3', region_name='ap-south-1') 
    error_msg=data[1]
    data=data[0]
 
    if(len(data)!=0):

        chunk_size = 1_000_000  
        num_chunks = len(data) // chunk_size + (1 if len(data) % chunk_size else 0)
        download_link_list=[]

        def generate_presigned_url(bucket_name, object_key, expiration=10800):  # Expiration time in seconds (3 hours)
            return s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration
            )

        start = 0
        for i in range(num_chunks):
            # start = i * chunk_size
            end = start + chunk_size
            chunk_df = data.iloc[start:end]
            object_key = f'QueryResultExport/{kwargs["file_name"]}_{i+1}.csv'
            csv_data = chunk_df.to_csv(index=False)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                chunk_df.to_csv(temp_file.name, index=False)
                temp_file_path = temp_file.name  # Get the temp file path
                
            s3.export(temp_file_path, bucket_name, object_key, format='CSV')
            presigned_url = generate_presigned_url(bucket_name, object_key)
            download_link_list.append(presigned_url)

            print(f'Exported {kwargs["file_name"]}')
            os.remove(temp_file_path)
            start = end

        print(download_link_list)

        if kwargs['recepient_email'].endswith('@dealshare.in'):
            smtp_server = 'email-smtp.ap-south-1.amazonaws.com'
            smtp_port = 587
            username = 'AKIAWJUEKAUPQYSU56WK'
            password = 'BG26AmQGS9H6rfVxc+2spso/lQvZ+FL/xx9NE8/zhRGT'

            msg = EmailMessage()
            msg['Subject'] = "Query Result Link";
            msg['From'] = 'tech@dealshare.in'
            
            msg['To'] = kwargs['recepient_email'];

            content=""
            for link in download_link_list:
                content+=str(link)+" , "

            print(content)
            msg.set_content(content)


            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  
                server.login(username, password)
                server.send_message(msg)
            
        return download_link_list
    else:
        if kwargs['recepient_email'].endswith('@dealshare.in'):
            smtp_server = 'email-smtp.ap-south-1.amazonaws.com'
            smtp_port = 587
            username = 'AKIAWJUEKAUPQYSU56WK'
            password = 'BG26AmQGS9H6rfVxc+2spso/lQvZ+FL/xx9NE8/zhRGT'

            msg = EmailMessage()
            msg['Subject'] = "Query Result Link";
            msg['From'] = 'tech@dealshare.in'
            
            msg['To'] = kwargs['recepient_email'];

            content=f"Error: {error_msg}"

            print(content)
            msg.set_content(content)


            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  
                server.login(username, password)
                server.send_message(msg)  

        return "Encountered an error."

# COMMAND ----------

transform(query_loading_from_sheets, file_name=file_name, recepient_email=recepient_email)

# COMMAND ----------

print(f"-----------Pipeline {PIPELINE_NAME} completed successfully-----------")
