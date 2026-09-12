# Databricks notebook source
# MAGIC %md
# MAGIC # 00. セットアップ
# MAGIC
# MAGIC 各トピックの調査に必要な土台を用意する。他のノートブックを動かす前に、まずこれを1回実行する。
# MAGIC
# MAGIC ## やること
# MAGIC - Unity Catalogのカタログ/スキーマ/Volumeを作成
# MAGIC - Auto Loaderの取り込み元となるサンプルデータ (注文イベントのJSON) をlanding Volumeに配置
# MAGIC
# MAGIC ## 注意
# MAGIC カタログ/スキーマ/Volumeの作成は `IF NOT EXISTS` なので何度実行してもよい。
# MAGIC 一方、サンプルデータ投入セルは実行するたびに新しいファイルが増える
# MAGIC (増分ファイルの到着を再現するための挙動なので、意図的にそうしている)。

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
from streaming.common.sample_data import generate_orders, write_batch

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

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## サンプルデータの投入
# MAGIC
# MAGIC 注文イベントをJSON Lines形式で1ファイル書き出す。
# MAGIC このセルを複数回実行すると、そのたびに新しいファイルがlanding Volumeに増える。
# MAGIC Auto Loaderやストリーミングの増分処理を試すときは、ストリーム実行中にここを再実行するとよい。

# COMMAND ----------

records = generate_orders(n_records=200, late_ratio=0.1, seed=None)
file_path = write_batch(VOLUME_LANDING_PATH, records)

print(f"書き出したファイル: {file_path}")
print(f"レコード数: {len(records)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 確認

# COMMAND ----------

display(dbutils.fs.ls(VOLUME_LANDING_PATH))

# COMMAND ----------

display(spark.read.json(VOLUME_LANDING_PATH).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 後片付け (任意)
# MAGIC
# MAGIC 調査を最初からやり直したい場合のみ、下のセルのコメントを外して実行する。
# MAGIC カタログ配下のテーブル・Volume・ファイルがすべて削除されるので注意。

# COMMAND ----------

# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
