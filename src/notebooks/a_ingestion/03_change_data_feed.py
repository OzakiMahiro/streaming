# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Change Data Feed (CDF)
# MAGIC
# MAGIC ## 調査目的
# MAGIC CDFによるテーブル間の変更伝播方法を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `table_changes()` 関数と `_change_type` / `_commit_version` / `_commit_timestamp` 列
# MAGIC - CDF有効化 (`delta.enableChangeDataFeed`) によるストレージ・パフォーマンスへの影響
# MAGIC - CDFの保持期間とVACUUMとの関係
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - Silver→Gold の差分伝播にCDFを使うべきか、MERGEの都度全件比較と比べてどちらが良いか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG, SCHEMA_SILVER, SCHEMA_GOLD  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: CDFを使った差分読み取りコードをここに書く
