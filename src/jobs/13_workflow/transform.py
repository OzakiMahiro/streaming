# Databricks notebook source
# タスク2: 整形。ジョブパラメータ fail_transform が "true" のときだけ、わざと失敗する

dbutils.widgets.text("fail_transform", "false")
fail_transform = dbutils.widgets.get("fail_transform")

CATALOG = "tech_survey"
SOURCE = f"{CATALOG}.bronze.job_orders"
TARGET = f"{CATALOG}.silver.job_orders"

if fail_transform == "true":
    raise RuntimeError("わざと失敗させている (fail_transform=true)")

spark.sql(f"CREATE OR REPLACE TABLE {TARGET} AS SELECT * FROM {SOURCE}")

print("transform 完了:", spark.table(TARGET).count(), "件")
