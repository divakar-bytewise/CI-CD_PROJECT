# Databricks notebook source
# AUTO-GENERATED FROM MAGE
# Pipeline: ims_sku_inventory_bq_std

# COMMAND ----------

dbutils.widgets.text("params","")

# COMMAND ----------

params_str = dbutils.widgets.get("params")

# COMMAND ----------

import json
params = json.loads(params_str)

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

PIPELINE_NAME = "ims_sku_inventory_bq_std"
print(f"-----------Starting pipeline: {PIPELINE_NAME}-----------")

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** bq_std_generic_last_modified
# MAGIC **Type:** data_loader

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
from datetime import datetime,timedelta
import pandas as pd
from datetime import datetime,timedelta
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_big_query(*args, **kwargs):
    query = f"SELECT max(modified_date) as dt FROM {kwargs['dataset']}.{kwargs['table_name']}"
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'
    dt = BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)['dt'][0]-timedelta(seconds=30)
    dt = dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt

# COMMAND ----------

last_modified_date = load_data_from_big_query(**params)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** sku_inventory_loader
# MAGIC **Type:** data_loader

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
import pandas as pd

def extract_second_part(value):
    if pd.notnull(value) and '-' in value:
        return value.split('-')[1]
    else:
        return 0

@data_loader
def load_data_from_mysql(last_modified_date, *args, **kwargs):
    query1 = f"""
        SELECT 
            id,
            created_at,
            inventory_count,
            location_id,
            updated_at as modified_date,
            sku_id,
            warehouse_id,
            sku_group_id,
            version,
            is_non_sellable,
            current_timestamp as _streaming_created_at,
            current_timestamp as _mage_created_at,
            current_timestamp as _mage_updated_at
        FROM sku_inventory si
        WHERE si.updated_at>'{last_modified_date}'
    """
    query2 = f"""SELECT deleted_id FROM deleted_rows_log WHERE delete_time>='{last_modified_date}' and table_name='sku_inventory'"""

    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'dealshare_delivery'

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data = loader.load(query1)
        del_id = loader.load(query2)
        data['sku_group_pk_id'] = data['sku_group_id'].apply(extract_second_part).astype(int)
        data['is_deleted'] = False
        del_id_str = ', '.join(del_id['deleted_id'].astype(str))
        return [data,del_id_str]

# COMMAND ----------

data = load_data_from_mysql(last_modified_date)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** ims_std_exporter_with_deletes
# MAGIC **Type:** data_exporter

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
import pandas as pd
from os import path
import time

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(lst, **kwargs):
    df = lst[0]
    del_id_str = lst[1]
    temp_table = f"dealshare-d82f7.{kwargs['dataset']}.temp_{kwargs['table_name']}"
    main_table = f"dealshare-d82f7.{kwargs['dataset']}.{kwargs['table_name']}"
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    for col in kwargs['datetime_columns']:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')

    insert_list = df.columns.tolist()
    update_list = [i for i in insert_list if i!=kwargs['id_column']]
    update_string = ', '.join([f'target.{col}=source.{col}' for col in update_list])
    insert_target_string = ', '.join(insert_list)
    insert_source_string = ', '.join([f'source.{col}' for col in insert_list])
    
    delete_str = (f"DELETE FROM {main_table} WHERE id in ({del_id_str});" if len(del_id_str)>0 else "")

    trunc_query = f"""TRUNCATE TABLE {temp_table}"""
    merge_query = f"""
        BEGIN TRANSACTION;
        MERGE INTO {main_table} AS target USING {temp_table} AS source 
        ON target.{kwargs['id_column']} = source.{kwargs['id_column']} 
        WHEN MATCHED THEN UPDATE SET {update_string} 
        WHEN NOT MATCHED THEN INSERT ({insert_target_string}) VALUES ({insert_source_string});
        TRUNCATE TABLE {temp_table};
        {delete_str}
        COMMIT TRANSACTION;
    """
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(trunc_query)
    time.sleep(2)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        temp_table,
        if_exists='append',
        # unique_conflict_method='UPDATE',
        # unique_constraints=['picklist_id']
    )
    time.sleep(2)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(merge_query)

# COMMAND ----------

export_data_to_big_query(data,**params)

# COMMAND ----------

print(f"-----------Pipeline {PIPELINE_NAME} completed successfully-----------")
