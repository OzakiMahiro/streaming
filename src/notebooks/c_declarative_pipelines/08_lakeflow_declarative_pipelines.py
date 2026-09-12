# Databricks notebook source
# MAGIC %md
# MAGIC # 08. Lakeflow Declarative Pipelines (旧DLT)
# MAGIC
# MAGIC ## 調査目的
# MAGIC 宣言的パイプラインによるBronze/Silver/Gold管理を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - マテリアライズドビューとストリーミングテーブルの違いと使い分け
# MAGIC - パイプラインのトリガーモード (`triggered` / `continuous`)
# MAGIC - 既存のJobs (手動オーケストレーション) との使い分け
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - このプロジェクトのメダリオン構成をLDPに置き換えるべきか、どの層から置き換えるか
# MAGIC
# MAGIC ## Notes
# MAGIC このファイルはLakeflow Declarative Pipelines用のノートブックであり、
# MAGIC 通常のJobではなく `resources/pipelines/lakeflow_declarative_pipelines.pipeline.yml` から参照される想定。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

# TODO: import dlt (Lakeflow Declarative Pipelinesのライブラリ) をここに書く

# COMMAND ----------

# MAGIC %md
# MAGIC ## パイプライン定義

# COMMAND ----------

# TODO: @dlt.table / @dlt.view を使ったBronze/Silver/Gold定義をここに書く
