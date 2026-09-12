# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Structured Streaming 基礎
# MAGIC
# MAGIC ## 調査目的
# MAGIC トリガーの種類とマイクロバッチの仕組みを理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `processingTime` / `availableNow` / `continuous` トリガーの違い
# MAGIC - マイクロバッチ実行のライフサイクルとチェックポイントの役割
# MAGIC - 出力モード (`append` / `update` / `complete`) の使い分け
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 日次バッチ的な運用には `availableNow` が向くか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import VOLUME_CHECKPOINT_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: 各トリガーモードでの動作確認コードをここに書く
