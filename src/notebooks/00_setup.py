# Databricks notebook source
# MAGIC %md
# MAGIC # 00. セットアップ
# MAGIC
# MAGIC 各トピックの調査に必要な土台を用意する。他のノートブックを動かす前に、まずこれを1回実行する。
# MAGIC
# MAGIC ## やること
# MAGIC Unity Catalogのカタログ・スキーマ・Volumeを作成する。
# MAGIC
# MAGIC ## 注意
# MAGIC すべて `IF NOT EXISTS` なので何度実行してもよい。
# MAGIC 取り込むサンプルデータは各トピックのノートブックが自分のサブディレクトリに用意するため、
# MAGIC ここでは作成しない。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from streaming.common.config import (
    CATALOG,
    SCHEMA_BRONZE,
    SCHEMA_GOLD,
    SCHEMA_LDP,
    SCHEMA_OPS,
    SCHEMA_SILVER,
    VOLUME_CHECKPOINT_PATH,
    VOLUME_LANDING_PATH,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## カタログ・スキーマ・Volumeの作成
# MAGIC
# MAGIC メダリオン各層のスキーマ (bronze/silver/gold) に加えて、以下2つを作る。
# MAGIC
# MAGIC - `ops`: 取り込み元ファイルとチェックポイント用のVolumeを置く
# MAGIC - `ldp`: Lakeflow Declarative Pipelinesの出力先 - Jobs方式のテーブルと衝突させないため分けている

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

for schema in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD, SCHEMA_OPS, SCHEMA_LDP):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_OPS}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA_OPS}.checkpoints")

print(f"取り込み元のベースパス: {VOLUME_LANDING_PATH}")
print(f"チェックポイントのベースパス: {VOLUME_CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 確認

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA_OPS}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ここまで成功していれば `01_auto_loader` から順に進められる。
# MAGIC 各ノートブックは `landing/<トピック名>/` に自分の取り込み元データを作るため、互いに干渉しない。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 後片付け (任意)
# MAGIC
# MAGIC 調査を最初からやり直したい場合のみ、下のセルのコメントを外して実行する。
# MAGIC カタログ配下のテーブル・Volume・ファイルがすべて削除されるので注意。

# COMMAND ----------

# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
