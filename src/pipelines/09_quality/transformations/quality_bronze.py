from pyspark import pipelines as dp
from pyspark.sql import DataFrame

LANDING = "/Volumes/tech_survey/ops/landing/09_quality"


# 生データはそのまま取り込む。品質チェックはこの後の層で行う
@dp.table(comment="汚れたデータを含む注文をそのまま取り込む")
def quality_bronze() -> DataFrame:
    """
    landing に届いた注文JSONを、加工せずに取り込む。

    Returns
    -------
    DataFrame
        取り込んだ注文のストリーミングDataFrame。

    Notes
    -----
    ここで品質チェックを行わないのは、上流が壊れたときに
    取り込み自体が止まり、元データごと失われるのを避けるため。
    """
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load(LANDING)
