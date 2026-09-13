from pyspark import pipelines as dp
from pyspark.sql import functions as F


# bronze を readStream で読むので、これもストリーミングテーブルになる
@dp.table(comment="型を整えて、金額が正の注文だけを残す")
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")  # 同じパイプライン内のテーブルは名前だけで参照する
        .withColumn("event_time", F.col("event_time").cast("timestamp"))  # JSONに型が無いのでキャストする
        .withColumn("amount", F.col("amount").cast("int"))
        .filter(F.col("amount") > 0)
    )
