"""
全ノートブック共通の設定値を管理するモジュール。

Notes
-----
Unity Catalogのカタログ名・スキーマ名・Volumeパスなどをここに集約する。
各ノートブックからは `sys.path` 経由でこのモジュールをimportして使う想定。
値は環境依存のため、実際に使う前にTODOを埋めること。
"""

from dataclasses import dataclass


# TODO: 実際に使用するUnity Catalogのカタログ名に置き換える
CATALOG: str = "TODO_catalog"

# TODO: メダリオン各層のスキーマ名に置き換える
SCHEMA_BRONZE: str = "bronze"
SCHEMA_SILVER: str = "silver"
SCHEMA_GOLD: str = "gold"

# TODO: Auto Loaderのソース/チェックポイント用Volumeパスに置き換える
VOLUME_LANDING_PATH: str = "/Volumes/TODO_catalog/TODO_schema/landing"
VOLUME_CHECKPOINT_PATH: str = "/Volumes/TODO_catalog/TODO_schema/checkpoints"


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
        raise NotImplementedError
