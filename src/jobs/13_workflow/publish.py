# Databricks notebook source
# タスク3: 集計。transform が失敗すると、ここは実行されない

CATALOG = "tech_survey"
SOURCE = f"{CATALOG}.silver.job_orders"
TARGET = f"{CATALOG}.gold.job_summary"

spark.sql(f"CREATE OR REPLACE TABLE {TARGET} AS SELECT product, count(*) AS n FROM {SOURCE} GROUP BY product")

print("publish 完了")
