# Databricks notebook source
# AUTO-GENERATED FROM MAGE
# Pipeline: sku_location_prc_full_bq

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

PIPELINE_NAME = "sku_location_prc_full_bq"
print(f"-----------Starting pipeline: {PIPELINE_NAME}-----------")

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** extract_slp_items
# MAGIC **Type:** data_loader

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_mysql(*args, **kwargs):
    query = f'''SELECT * from sku_location_pricing'''
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'items'

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)

# COMMAND ----------

extract_slp_items = load_data_from_mysql()

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** `slp_to_bq`
# MAGIC **Type:** data_exporter

# COMMAND ----------

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    table_id = 'dealshare-d82f7.items_analytics.items_dim_sku_location_pricing'
    # query = f"""TRUNCATE TABLE {table_id}"""
    query = f""" 
        BEGIN TRANSACTION;
        TRUNCATE TABLE {table_id};
        COMMIT TRANSACTION;
        """
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(query)

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append'
    )

# COMMAND ----------

slp_to_bq=export_data_to_big_query(extract_slp_items)

# COMMAND ----------

print(f"-----------Pipeline {PIPELINE_NAME} completed successfully-----------")
