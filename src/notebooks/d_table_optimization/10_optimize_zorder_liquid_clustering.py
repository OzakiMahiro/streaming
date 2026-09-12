# Databricks notebook source
# MAGIC %md
# MAGIC # 10. OPTIMIZE / Z-ORDER / Liquid Clustering
# MAGIC
# MAGIC ## 調査目的
# MAGIC テーブルの読み取り性能最適化の手法を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `OPTIMIZE` の自動実行 (Auto Optimize / Predictive Optimization) の条件
# MAGIC - Z-ORDERとLiquid Clusteringの違いと使い分け
# MAGIC - 最適化の実行頻度とコストのトレードオフ
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 日次更新後、どのタイミングで最適化を実行するか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG, SCHEMA_GOLD  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: OPTIMIZE/Z-ORDER/Liquid Clusteringの検証コードをここに書く
