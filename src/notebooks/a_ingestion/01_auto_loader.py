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
# MAGIC
# MAGIC ## 前提
# MAGIC `00_setup` を実行済みで、カタログとVolumeが作成されていること。
# MAGIC 取り込むデータはこのノートブックが自分のサブディレクトリに用意するため、
# MAGIC 何度実行しても同じ件数になる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セットアップ

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../.."))

from streaming.common.config import (
    CATALOG,
    SCHEMA_BRONZE,
    VOLUME_CHECKPOINT_PATH,
    VOLUME_LANDING_PATH,
)
from streaming.common.sample_data import generate_orders, write_batch

TOPIC = "01_auto_loader"

# 他のノートブックと干渉しないよう、取り込み元とチェックポイントはトピックごとに分ける
LANDING_PATH = f"{VOLUME_LANDING_PATH}/{TOPIC}"
CHECKPOINT_PATH = f"{VOLUME_CHECKPOINT_PATH}/{TOPIC}"

# 推論したスキーマの保存先 - チェックポイントとは別に指定する
SCHEMA_PATH = f"{CHECKPOINT_PATH}/_schema"

TABLE_BRONZE = f"{CATALOG}.{SCHEMA_BRONZE}.orders_raw"

print(f"取り込み元: {LANDING_PATH}")
print(f"書き込み先: {TABLE_BRONZE}")
print(f"チェックポイント: {CHECKPOINT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. リセットと初期データ投入
# MAGIC
# MAGIC 何度実行しても同じ結果になるよう、テーブル・チェックポイント・取り込み元を作り直してから始める。
# MAGIC 消すのはこのノートブック専用のサブディレクトリだけなので、他のトピックには影響しない。

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE_BRONZE}")
dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
dbutils.fs.rm(LANDING_PATH, recurse=True)

initial_file = write_batch(LANDING_PATH, generate_orders(n_records=200, late_ratio=0.1))
print(f"初期データ: {initial_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 基本の取り込み
# MAGIC
# MAGIC Auto Loaderは `spark.readStream.format("cloudFiles")` で使う。主要なオプションは以下。
# MAGIC
# MAGIC | オプション | 役割 |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format` | 取り込むファイル形式 - ここではJSON |
# MAGIC | `cloudFiles.schemaLocation` | 推論したスキーマの保存先 - 次回実行時はここを参照する |
# MAGIC | `cloudFiles.inferColumnTypes` | JSONの型を推論する - 未指定だと全列が文字列になる |
# MAGIC | `cloudFiles.schemaEvolutionMode` | 新しい列が現れたときの挙動 - 既定は `addNewColumns` |
# MAGIC
# MAGIC トリガーは `availableNow` を使う。未処理ファイルをすべて処理したら自動で停止するため、
# MAGIC ノートブックでの検証や日次バッチ的な運用に向く
# MAGIC (常時起動の `processingTime` との違いは `02_structured_streaming_basics` で扱う)。

# COMMAND ----------

def ingest_once(schema_evolution_mode: str = "rescue") -> int:
    """
    取り込み元の未処理ファイルをAuto Loaderで取り込み、処理件数を返す。

    Parameters
    ----------
    schema_evolution_mode : str, default "rescue"
        新しい列を検知したときの挙動。
        `addNewColumns` / `rescue` / `failOnNewColumns` / `none` のいずれか。
        既定値をDatabricksの既定 (`addNewColumns`) ではなく `rescue` にしているのは、
        ノートブックを通しで実行できるようにするため (理由は5章を参照)。

    Returns
    -------
    int
        このバッチで処理された行数。

    Notes
    -----
    `availableNow` トリガーのため、未処理ファイルを処理し終えるとクエリは自動終了する。
    `awaitTermination` で終了を待ってから進捗を読み取る。
    """
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
        .load(LANDING_PATH)
    )

    query = (
        stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TABLE_BRONZE)
    )
    query.awaitTermination()

    return sum(progress["numInputRows"] for progress in query.recentProgress)


processed = ingest_once()
print(f"処理件数: {processed}  (初期データの200件)")

# COMMAND ----------

display(spark.table(TABLE_BRONZE).limit(10))

# COMMAND ----------

print(f"テーブル全体の行数: {spark.table(TABLE_BRONZE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 増分取り込みの確認
# MAGIC
# MAGIC 新しいファイルを1つ追加してから、まったく同じコードを再実行する。
# MAGIC Auto Loaderはチェックポイントに処理済みファイルを記録しているため、
# MAGIC 既存ファイルは読み直さず、新しいファイルだけを処理する。

# COMMAND ----------

new_file = write_batch(LANDING_PATH, generate_orders(n_records=50))
print(f"追加したファイル: {new_file}")

# COMMAND ----------

processed = ingest_once()
print(f"処理件数: {processed}  (50なら増分取り込みが効いている)")
print(f"テーブル全体の行数: {spark.table(TABLE_BRONZE).count()}  (200 + 50 = 250)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. スキーマ推論の結果を見る
# MAGIC
# MAGIC `cloudFiles.schemaLocation` に推論結果が保存されている。
# MAGIC 次回以降の実行ではここを読むため、毎回全ファイルをサンプリングし直すことはない。

# COMMAND ----------

display(dbutils.fs.ls(SCHEMA_PATH))

# COMMAND ----------

spark.table(TABLE_BRONZE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC `inferColumnTypes` を有効にしているため、`amount` はDOUBLE、`quantity` はLONGとして推論される。
# MAGIC 無効の場合はすべてSTRINGになる。
# MAGIC
# MAGIC もう1つ注目すべきは `_rescued_data` 列。推論済みスキーマに収まらなかったデータはここに退避されるため、
# MAGIC 想定外のデータが来ても取り込み自体は失敗しない。次章で実際に中身を入れて確かめる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. スキーマ進化: `rescue` モード
# MAGIC
# MAGIC 既存スキーマに無い `campaign_code` 列を含むファイルを投入する。
# MAGIC `rescue` モードではテーブルのスキーマは変わらず、新しい列は `_rescued_data` にJSON文字列として退避される。

# COMMAND ----------

records = generate_orders(n_records=10)
for record in records:
    record["campaign_code"] = "AUTUMN_SALE"

evolved_file = write_batch(LANDING_PATH, records)
print(f"新しい列を含むファイル: {evolved_file}")

# COMMAND ----------

processed = ingest_once(schema_evolution_mode="rescue")
print(f"処理件数: {processed}  (10件)")
print(f"テーブル全体の行数: {spark.table(TABLE_BRONZE).count()}  (250 + 10 = 260)")

# COMMAND ----------

display(
    spark.table(TABLE_BRONZE)
    .select("order_id", "_rescued_data")
    .where("_rescued_data IS NOT NULL")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC `campaign_code` がテーブルの列にはならず、`_rescued_data` の中にJSONとして入っていることを確認する。
# MAGIC データを落とさずに取り込みを継続できる一方、新しい列を使いたい場合は自分でパースする必要がある。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. スキーマ進化: `addNewColumns` (既定) の挙動
# MAGIC
# MAGIC | モード | 新しい列が現れたとき |
# MAGIC |---|---|
# MAGIC | `addNewColumns` (既定) | スキーマに列を追加して**ストリームを失敗させる** - 再実行すると新しいスキーマで継続する |
# MAGIC | `rescue` | スキーマは変えず `_rescued_data` に退避する - 失敗しない |
# MAGIC | `failOnNewColumns` | 失敗する - スキーマを更新しないので手動対応が必要 |
# MAGIC | `none` | 新しい列を無視する |
# MAGIC
# MAGIC 既定の `addNewColumns` は「一度失敗してから、再実行で新しいスキーマになる」という挙動をとる。
# MAGIC ジョブのリトライ設定が再実行を肩代わりする前提の設計だが、知らないと「なぜか失敗する」ように見える。
# MAGIC
# MAGIC ### なぜ下のセルをコメントアウトしているか
# MAGIC
# MAGIC ストリームが失敗すると、Pythonの `try/except` で例外を捕捉しても、
# MAGIC ノートブック終了時にDatabricksが `Some streams terminated before this command could finish!`
# MAGIC を投げてノートブック全体を失敗扱いにする。
# MAGIC そのためジョブとして通しで実行できなくなる。挙動を自分の目で見たいときだけ、対話的に実行すること。
# MAGIC また、書き込み先テーブルのスキーマも変わるため、実行後は0章のリセットからやり直すこと。

# COMMAND ----------

# records = generate_orders(n_records=10)
# for record in records:
#     record["channel"] = "mobile_app"
# write_batch(LANDING_PATH, records)
#
# # 1回目: スキーマを更新した上で失敗する
# ingest_once(schema_evolution_mode="addNewColumns")

# COMMAND ----------

# # 2回目: 更新済みのスキーマで処理が進み、channel列が増える
# ingest_once(schema_evolution_mode="addNewColumns")
# spark.table(TABLE_BRONZE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ファイル検知方式 (directory listing / file notification)
# MAGIC
# MAGIC Auto Loaderが新しいファイルを知る方法は2つある。
# MAGIC
# MAGIC | 方式 | 仕組み | 向いている場面 |
# MAGIC |---|---|---|
# MAGIC | directory listing (既定) | ストレージのファイル一覧を取得して差分を判定する | ファイル数が中規模まで - 追加設定が不要 |
# MAGIC | file notification | クラウドの通知サービス (SNS/SQS, Event Grid等) 経由で到着を受け取る | ディレクトリ内のファイルが非常に多い場合 |
# MAGIC
# MAGIC directory listingはファイル数に比例してリスト取得のコストが増えるため、ディレクトリが肥大化すると遅くなる。
# MAGIC file notificationは `cloudFiles.useNotifications` で有効化するが、クラウド側のリソース作成権限が必要になる。
# MAGIC
# MAGIC 今回のような小規模な検証では既定のdirectory listingで十分。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 分かったこと
# MAGIC
# MAGIC TODO: 実行結果を踏まえて記入する
# MAGIC
# MAGIC - Bronze層への取り込みで必要なオプション:
# MAGIC - スキーマ進化モードの選択:
# MAGIC - コスト面で気をつける点:
