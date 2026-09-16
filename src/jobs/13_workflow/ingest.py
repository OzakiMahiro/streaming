# Databricks notebook source
# タスク1: 取り込み。bronze に数件書く

CATALOG = "tech_survey"
TABLE = f"{CATALOG}.bronze.job_orders"

spark.sql(f"CREATE OR REPLACE TABLE {TABLE} AS SELECT * FROM VALUES (1, 'laptop'), (2, 'monitor') AS t(order_id, product)")

print("ingest 完了:", spark.table(TABLE).count(), "件")
