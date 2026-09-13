from pyspark import pipelines as dp

# 取り込み元。08専用のフォルダを使う
LANDING = "/Volumes/tech_survey/ops/landing/08_ldp"


# readStream で読むと「ストリーミングテーブル」になる。届いたぶんだけ追記される
# schemaLocation も checkpointLocation も指定していない - パイプラインが自分で持つ
@dp.table(comment="landing に届いた注文JSONをそのまま取り込む")
def bronze_orders():
    """landing に届いた注文JSONをそのまま取り込む。

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        注文データ
    """
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(LANDING)
