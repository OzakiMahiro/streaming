from pyspark import pipelines as dp
from pyspark.sql import DataFrame

LANDING = "/Volumes/tech_survey/ops/landing/15_cdc_orders"


# 変更イベントをそのまま取り込む。ここでは何も判断しない
@dp.table(comment="注文の変更イベントを無加工で取り込む")
def cdc_orders_changes() -> DataFrame:
    """
    landing に届いた変更イベントのJSONを取り込む。

    Returns
    -------
    DataFrame
        変更イベントのストリーミングDataFrame。

    Notes
    -----
    1行が1つの操作を表す。`op` 列に "u" (更新) と "d" (削除) が入る。
    どれが最新かの判断は、この後の Auto CDC が `event_time` を見て行う。
    """
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(LANDING)
