from pyspark import pipelines as dp
from pyspark.sql import DataFrame

# 条件は quality_warn.py とまったく同じ
RULES = {
    "positive_amount": "amount > 0",
    "product_not_null": "product IS NOT NULL",
}


# expect_all_or_drop = 違反した行を書き込む前に捨てる
@dp.table(comment="違反した行を落として、きれいなものだけ残す")
@dp.expect_all_or_drop(RULES)
def quality_drop() -> DataFrame:
    """
    bronze のうち、条件を満たす行だけを残す。

    Returns
    -------
    DataFrame
        条件違反を除いたストリーミングDataFrame。

    Notes
    -----
    捨てた行はどこにも残らない。中身を後から追う必要があるなら、
    bronze を残しておくか、警告にして別テーブルへ分ける。
    """
    return spark.readStream.table("quality_bronze")
