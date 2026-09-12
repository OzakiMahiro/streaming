# Databricks notebook source
# MAGIC %md
# MAGIC # 11. VACUUM とタイムトラベル
# MAGIC
# MAGIC ## 調査目的
# MAGIC 古いファイルの削除とタイムトラベルの仕組みを理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `VACUUM` のデフォルト保持期間 (`delta.deletedFileRetentionDuration`) とタイムトラベル可能範囲の関係
# MAGIC - ストリーミング読み取り中のテーブルに対するVACUUMの影響
# MAGIC - `RESTORE` によるロールバックの挙動
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 保持期間をどう設定するか、CDFの保持期間との整合性

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG, SCHEMA_SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: VACUUM/タイムトラベルの検証コードをここに書く
