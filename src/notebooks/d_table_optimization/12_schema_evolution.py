# Databricks notebook source
# MAGIC %md
# MAGIC # 12. スキーマ進化
# MAGIC
# MAGIC ## 調査目的
# MAGIC スキーマ変更に対する安全な対応方法を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - 書き込み時の `mergeSchema` オプションの挙動
# MAGIC - Auto Loaderのスキーマ進化モード (`addNewColumns` / `rescue` / `failOnNewColumns` / `none`)
# MAGIC - 列の削除・型変更など破壊的スキーマ変更の検知方法
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 本番運用でスキーマ変更をどう検知し、誰に通知するか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG, SCHEMA_BRONZE  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: スキーマ進化の検証コードをここに書く
