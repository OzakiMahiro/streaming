# Databricks notebook source
# MAGIC %md
# MAGIC # 09. データ品質 (Expectations)
# MAGIC
# MAGIC ## 調査目的
# MAGIC LDPにおけるデータ品質チェックの方法を理解する
# MAGIC
# MAGIC ## 確認したいこと
# MAGIC - `expect` / `expect_or_drop` / `expect_or_fail` の違いと挙動
# MAGIC - 複数条件をまとめる `expect_all` 系関数
# MAGIC - 違反件数・違反率をイベントログ/メトリクスでどう確認するか
# MAGIC
# MAGIC ## 参考にする観点
# MAGIC - Bronze/Silver/Goldのどの層にどのexpectationレベルを適用するか
# MAGIC
# MAGIC ## Notes
# MAGIC このファイルは08と同じLDPパイプラインの一部として動く想定。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

# TODO: import dlt をここに書く

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations定義

# COMMAND ----------

# TODO: @dlt.expect系デコレータを使ったデータ品質チェックをここに書く
