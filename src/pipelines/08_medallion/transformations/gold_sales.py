from pyspark import pipelines as dp
from pyspark.sql import functions as F


# readStream ではなく read で読むと「マテリアライズドビュー」になる。毎回まとめて計算し直される
@dp.table(comment="商品ごとの売上合計")
def gold_sales():
    return (
        spark.read.table("silver_orders")
        .groupBy("product")
        .agg(
            F.count("*").alias("order_count"),
            F.sum("amount").alias("total_amount"),
        )
    )
