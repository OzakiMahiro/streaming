# Databricks notebook source
# MAGIC %md
# MAGIC # 14. Structured Streamingのメトリクス監視
# MAGIC
# MAGIC ## 調査目的
# MAGIC ストリーミングクエリの状態監視方法を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `StreamingQueryListener` によるメトリクス取得
# MAGIC - `StreamingQuery.lastProgress` / `recentProgress` に含まれる指標 (処理レート・遅延など)
# MAGIC - 外部監視基盤 (Datadog/Prometheus等) への連携方法
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - 本番運用での監視ダッシュボード・アラート設計

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

# TODO: StreamingQueryListenerを使った監視コードをここに書く
