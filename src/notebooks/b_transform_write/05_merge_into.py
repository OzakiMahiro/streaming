# Databricks notebook source
# MAGIC %md
# MAGIC # 05. `MERGE INTO` によるUpsert
# MAGIC
# MAGIC ## 調査目的
# MAGIC Upsertパターンの実装方法とパフォーマンス特性を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `MERGE INTO` の基本構文 (`WHEN MATCHED` / `WHEN NOT MATCHED` / `WHEN NOT MATCHED BY SOURCE`)
# MAGIC - ソース側に重複行がある場合の挙動とエラー
# MAGIC - Z-ORDER / Liquid Clustering併用時のMERGE性能への影響
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - Silver層のUpsertに `MERGE INTO` をどう組み込むか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG, SCHEMA_SILVER  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: MERGE INTOによるUpsertコードをここに書く
