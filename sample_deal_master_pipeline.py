# Databricks notebook source
# AUTO-GENERATED FROM MAGE
# Pipeline: sample_deal_master_pipeline

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

PIPELINE_NAME = "sample_deal_master_pipeline"
print(f"-----------Starting pipeline: {PIPELINE_NAME}-----------")

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** halcyon_wind
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
def load_data(*args, **kwargs):

    query = f'''select po.id, pot.name from product_offer po
            inner join product_offer_translations pot on po.id = pot.offer_id
            where po.modified_date < pot.modified_date
            and po.end_Date > now()
            and po.offer_type in ('general','combo','single_combo_deals')
            and deals_type = 'normal' and pot.lang='en'
            ''';

    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'happy_offer'

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        result =  loader.load(query)

    return result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

# COMMAND ----------

halcyon_wind_out = load_data()

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** refined_bird
# MAGIC **Type:** data_exporter

# COMMAND ----------

import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    table_id = 'dealshare-d82f7.oms_analytics.sample_deal_master'
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    query = f"""TRUNCATE TABLE {table_id}"""

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(query)

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='append',
        # unique_conflict_method='UPDATE',
        # unique_constraints=['id']
    )

# COMMAND ----------

refined_bird=export_data_to_big_query(halcyon_wind_out)

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** intriguing_ancient
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
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    
    query = f"""UPDATE oms_analytics.deal_master dm
            SET deal_name = sdm.name
            FROM oms_analytics.sample_deal_master sdm
            WHERE dm.id = sdm.id
            AND dm.deal_name <> sdm.name
            """
    
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(query)

# COMMAND ----------

export_data(refined_bird)

# COMMAND ----------

print(f"-----------Pipeline {PIPELINE_NAME} completed successfully-----------")
