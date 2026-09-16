from pyspark import pipelines as dp
from pyspark.sql.functions import expr

dp.create_streaming_table("cdc_orders_history", comment="注文の状態の履歴を持つ (SCD Type 2)")

# ソースも条件も上と同じ。違いは stored_as_scd_type だけ
dp.create_auto_cdc_flow(
    target="cdc_orders_history",
    source="cdc_orders_changes",
    keys=["order_id"],
    sequence_by="event_time",
    stored_as_scd_type=2,  # 履歴を残す。__START_AT / __END_AT が付く
    apply_as_deletes=expr("op = 'd'"),
    except_column_list=["op"],
)
