from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LANDING = "/Volumes/tech_survey/ops/landing/15_cdc_stores"


# 置いてあるスナップショットのうち、最新の deem_date の分だけを取り出す
# create_auto_cdc_from_snapshot_flow は「最新のスナップショットを持つテーブル」を入力に取る
@dp.materialized_view(comment="最新の deem_date のスナップショットだけを取り出したもの")
def stores_latest_snapshot() -> DataFrame:
    """
    landing にある全スナップショットから、最新の版だけを返す。

    Returns
    -------
    DataFrame
        最新の `deem_date` に属する行だけを持つDataFrame。

    Notes
    -----
    最大値を取るのに `first()` のようなアクションを使わず、結合で解決している。
    フロー関数の中でアクションを走らせると、解析の段階で実行されてしまうため。
    """
    snapshots = spark.read.json(LANDING)
    latest = snapshots.select(F.max("deem_date").alias("_latest"))

    return snapshots.join(latest, snapshots["deem_date"] == latest["_latest"]).drop("_latest")


dp.create_streaming_table("cdc_stores_history", comment="店舗マスタの履歴 (スナップショットから構築)")

# スナップショット同士を比較して、変化点だけを履歴にする
dp.create_auto_cdc_from_snapshot_flow(
    target="cdc_stores_history",
    source="stores_latest_snapshot",
    keys=["store_id"],
    stored_as_scd_type=2,
    # deem_date は版ごとに必ず変わるので、比較対象から外す
    # 外さないと「中身は同じなのに版が違う」だけで履歴行ができてしまう
    track_history_except_column_list=["deem_date"],
)
