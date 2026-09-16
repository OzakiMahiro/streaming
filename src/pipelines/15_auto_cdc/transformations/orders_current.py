from pyspark import pipelines as dp
from pyspark.sql.functions import expr

# 流し込み先を先に作る。中身は下の flow が入れる
dp.create_streaming_table("cdc_orders_current", comment="注文の現在の状態だけを持つ (SCD Type 1)")

# SCD Type 1 = 上書き。同じ order_id は最新の1行だけが残る
dp.create_auto_cdc_flow(
    target="cdc_orders_current",
    source="cdc_orders_changes",
    keys=["order_id"],
    sequence_by="event_time",  # どちらが新しいかの判断。順序が前後しても正しくなる
    stored_as_scd_type=1,
    apply_as_deletes=expr("op = 'd'"),  # この条件に当たる行は「削除」として扱う
    except_column_list=["op"],  # op は運搬用の列なので、結果には残さない
)
