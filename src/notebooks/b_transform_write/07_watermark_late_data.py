# Databricks notebook source
# MAGIC %md
# MAGIC # 07. ウォーターマークと遅延データ処理
# MAGIC
# MAGIC ## 調査目的
# MAGIC ストリーミング集計における遅延データの扱いを理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `withWatermark` の指定方法とステート保持期間の関係
# MAGIC - ウォーターマークを超えて到着した遅延データが破棄される条件
# MAGIC - ステート管理によるメモリ・ストレージコスト
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 日次集計にウォーターマークが必要か、必要ならどの程度の許容遅延にするか

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import VOLUME_CHECKPOINT_PATH  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: withWatermarkを使ったストリーミング集計コードをここに書く
