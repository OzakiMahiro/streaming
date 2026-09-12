"""
全ノートブック共通の設定値を管理するモジュール。

Notes
-----
Unity Catalogのカタログ名・スキーマ名・Volumeパスなどをここに集約する。
各ノートブックからは `sys.path` 経由でこのモジュールをimportして使う想定。
ここで定義したカタログ/スキーマ/Volumeは `src/notebooks/00_setup.py` で作成する。
"""

from dataclasses import dataclass

CATALOG: str = "tech_survey"

# メダリオン各層のスキーマ
SCHEMA_BRONZE: str = "bronze"
SCHEMA_SILVER: str = "silver"
SCHEMA_GOLD: str = "gold"

# 運用用スキーマ: 取り込み元ファイルやチェックポイントなど、テーブル以外の置き場所
SCHEMA_OPS: str = "ops"

# LDP (Lakeflow Declarative Pipelines) の出力先スキーマ - Jobs方式との比較のため分けている
SCHEMA_LDP: str = "ldp"

VOLUME_LANDING_PATH: str = f"/Volumes/{CATALOG}/{SCHEMA_OPS}/landing"
VOLUME_CHECKPOINT_PATH: str = f"/Volumes/{CATALOG}/{SCHEMA_OPS}/checkpoints"


@dataclass(frozen=True)
class TableLocation:
    """
    3階層 (catalog.schema.table) のテーブル所在地を表す。

    Parameters
    ----------
    catalog : str
        カタログ名。
    schema : str
        スキーマ名。
    table : str
        テーブル名。
    """

    catalog: str
    schema: str
    table: str

    @property
    def full_name(self) -> str:
        """
        `catalog.schema.table` 形式の完全修飾名を返す。

        Returns
        -------
        str
            完全修飾テーブル名。
        """
        return f"{self.catalog}.{self.schema}.{self.table}"
