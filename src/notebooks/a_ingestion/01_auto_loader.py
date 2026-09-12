# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Auto Loader (`cloudFiles`)
# MAGIC
# MAGIC ## 調査目的
# MAGIC Auto Loaderによる増分ファイル取り込みの挙動を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - ファイル検知方式: directory listing mode と file notification mode の違い
# MAGIC - スキーマ推論とスキーマ進化 (`mergeSchema`, `cloudFiles.schemaEvolutionMode`)
# MAGIC - チェックポイント (RocksDBベースのファイルステート管理) の仕組み
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - Bronze層への取り込みでどのオプションが必要か
# MAGIC - 大量ファイル・高頻度更新時のコスト

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import VOLUME_LANDING_PATH, VOLUME_CHECKPOINT_PATH  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: Auto Loaderでの取り込みコードをここに書く
