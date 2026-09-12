# Databricks notebook source
# MAGIC %md
# MAGIC # 13. Databricks Workflows/Jobs
# MAGIC
# MAGIC ## 調査目的
# MAGIC タスク間の依存関係とリトライ設計を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - タスク間の依存指定 (`depends_on`) と並列実行の制御
# MAGIC - リトライポリシー (`max_retries` / `min_retry_interval_millis`) の設定
# MAGIC - 失敗時の通知 (メール/Slack Webhook) の設定方法
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - メダリオン層 (Bronze→Silver→Gold) をJobsのタスクとして繋ぐ場合の依存設計

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import sys
import os

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import CATALOG  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 検証コード

# COMMAND ----------

# TODO: Jobs APIやdatabricks-sdkを使った動作確認コードをここに書く
