# Databricks notebook source
# MAGIC %md
# MAGIC # 04. `replaceWhere` によるパーティション部分置換
# MAGIC
# MAGIC ## 調査目的
# MAGIC パーティション単位の冪等な部分上書きを理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `replaceWhere` に指定できる条件の制約 (パーティション列以外を使った場合の挙動)
# MAGIC - 動的パーティション上書き (`partitionOverwriteMode=dynamic`) との違い
# MAGIC - 同じパーティションに対する再実行時の安全性 (冪等性の確認)
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 日次パーティションの再処理 (バックフィル) にどう組み込むか

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

# TODO: replaceWhereによる部分上書きコードをここに書く
