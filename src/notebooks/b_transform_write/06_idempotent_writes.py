# Databricks notebook source
# MAGIC %md
# MAGIC # 06. 冪等な書き込み
# MAGIC
# MAGIC ## 調査目的
# MAGIC 再実行しても結果が変わらない書き込み設計を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `foreachBatch` での `txnAppId` / `txnVersion` オプションによる重複書き込み防止
# MAGIC - MERGEキーの設計による重複排除
# MAGIC - at-least-once配信とexactly-once書き込みの違い
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - ジョブ再実行 (リトライ) 時に重複データが発生しない設計になっているか

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

# TODO: txnAppId/txnVersionを使った冪等書き込みコードをここに書く
