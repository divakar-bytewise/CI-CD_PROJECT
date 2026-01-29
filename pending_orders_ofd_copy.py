# Databricks notebook source
# AUTO-GENERATED FROM MAGE
# Pipeline: pending_orders_ofd_copy

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

PIPELINE_NAME = "pending_orders_ofd_copy"
print(f"-----------Starting pipeline: {PIPELINE_NAME}-----------")

# COMMAND ----------

# MAGIC %md
# MAGIC **Mage block:** pending_orders_bq
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
def export_data_to_big_query(*args, **kwargs) -> None:
    config_path = '/Volumes/dealshare_prod/default/io_config/io_config.yaml'
    config_profile = 'default'

    
    change_query = f""" 
        BEGIN TRANSACTION;

        DELETE FROM oms_analytics.pending_orders where pending_date = DATE(TIMESTAMP(CURRENT_DATETIME('Asia/Kolkata')));

        INSERT INTO oms_analytics.pending_orders (
        SELECT 
        DATE(TIMESTAMP(CURRENT_DATETIME('Asia/Kolkata'))) AS pending_date,
        order_id,
        user_address_id,
        billed_amount,
        cm.city_id,
        shipment_order_status,
        order_status,
        shipment_id,
        da.id                                                    AS allotment_id,
        da.created_at                                            AS allotment_date,
        da.admin_bts_date                                        AS bts_date,
        p.warehouse_id                                           AS pal_warehouse_id,
        w.warehouse_name                                         AS pal_warehouse_name,
        cm.city_name                                             AS city_name,
        cm.state_region                                          AS state_region,
        inventory_warehouse_id                                   AS inventory_warehouse_id
        FROM (
        SELECT 
        o.order_id AS order_id,
        o.user_address_id                                           AS user_address_id,
        o.billed_amount                                             AS billed_amount,
        o.city_id                                                   AS city_id,
        o.status                                                    AS order_status,
        o.warehouse_id                                              AS inventory_warehouse_id,
        o.shipment_item_status                                     AS shipment_order_status,
        o.shipment_id                                               AS shipment_id,
        o.allotment_id
        FROM oms_analytics.order_analytics o
        WHERE o.order_date_ < DATE(TIMESTAMP(CURRENT_DATETIME('Asia/Kolkata')))
        AND o.status NOT IN ('delivered', 'discarded', 'cancel', 'completed', 'pending')
        AND  lower(o.source) NOT IN ('minimart')) AS d
        LEFT JOIN ims_analytics.ims_dim_allotments da ON da.id = d.allotment_id
        LEFT JOIN oms_analytics.dim_user_address u ON u.id = d.user_address_id
        LEFT JOIN oms_analytics.dim_pal p ON p.pal_id = u.pal_id
        LEFT JOIN oms_analytics.dim_warehouse_data w ON w.warehouse_id = p.warehouse_id
        left join ( select * from oms_analytics.dim_state_city_pincode as cm, UNNEST(cm.city_details)) cm ON cm.city_id = d.city_id                            
        );

        COMMIT TRANSACTION;
    """
    

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(change_query)
    return 1

# COMMAND ----------

export_data_to_big_query()

# COMMAND ----------

print(f"-----------Pipeline {PIPELINE_NAME} completed successfully-----------")
