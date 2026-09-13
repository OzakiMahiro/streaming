from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# 条件は quality_drop.py とまったく同じ。違うのはデコレータだけ
RULES = {
    "positive_amount": "amount > 0",
    "product_not_null": "product IS NOT NULL",
}


# expect_all = 警告のみ。違反した行もテーブルに残る
@dp.table(comment="違反を記録するだけで、行はすべて残す")
@dp.expect_all(RULES)
def quality_warn() -> DataFrame:
    """
    bronze の全行をそのまま通し、条件違反を記録だけする。

    Returns
    -------
    DataFrame
        bronze と同じ行を持つストリーミングDataFrame。

    Notes
    -----
    違反した行も残るため、件数は bronze と一致する。
    違反があったかどうかはイベントログを見ないと分からない。
    """
    return spark.readStream.table("quality_bronze")
