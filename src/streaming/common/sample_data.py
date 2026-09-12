"""
調査用のサンプルデータ (注文イベント) を生成するモジュール。

Notes
-----
`write_batch` は1回の呼び出しにつき1ファイルをVolumeに書き出す。
繰り返し呼ぶことで「新しいファイルが順次到着する」状況を再現でき、
Auto Loaderやストリーミングの増分処理の検証に使える。

生成されるレコードは以下の用途を想定している。

- `order_date`: パーティション列 - replaceWhereによる部分置換の検証に使う
- `event_time`: イベント時刻 - ウォーターマークと遅延データの検証に使う
- `order_id` + `status`: 同一注文の状態遷移 - MERGEによるUpsertの検証に使う
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

JST = timezone(timedelta(hours=9))

PRODUCTS: tuple[str, ...] = ("laptop", "monitor", "keyboard", "mouse", "headset")
STATUSES: tuple[str, ...] = ("placed", "paid", "shipped", "cancelled")


def generate_orders(
    n_records: int = 100,
    late_ratio: float = 0.0,
    seed: int | None = None,
) -> list[dict[str, Any]]:
    """
    注文イベントのレコードを生成する。

    Parameters
    ----------
    n_records : int, default 100
        生成するレコード数。
    late_ratio : float, default 0.0
        遅延データの割合 (0.0〜1.0)。
        該当するレコードの `event_time` は2〜48時間前の時刻になる。
    seed : int or None, default None
        乱数シード。再現性が必要な場合に指定する。

    Returns
    -------
    list of dict
        生成された注文イベントのレコード。

    Examples
    --------
    >>> records = generate_orders(10, late_ratio=0.2, seed=42)
    >>> len(records)
    10
    """
    rng = random.Random(seed)
    now = datetime.now(JST)
    records: list[dict[str, Any]] = []

    for _ in range(n_records):
        if rng.random() < late_ratio:
            event_time = now - timedelta(hours=rng.uniform(2, 48))
        else:
            event_time = now - timedelta(seconds=rng.uniform(0, 600))

        quantity = rng.randint(1, 5)
        unit_price = round(rng.uniform(1000, 200000), 2)
        records.append(
            {
                "order_id": str(uuid.uuid4()),
                "customer_id": f"C{rng.randint(1, 500):04d}",
                "product": rng.choice(PRODUCTS),
                "quantity": quantity,
                "amount": round(unit_price * quantity, 2),
                "status": rng.choice(STATUSES),
                "event_time": event_time.isoformat(),
                "order_date": event_time.strftime("%Y-%m-%d"),
            }
        )

    return records


def write_batch(
    volume_path: str,
    records: list[dict[str, Any]],
) -> str:
    """
    レコードをJSON Lines形式の1ファイルとしてVolumeに書き出す。

    Parameters
    ----------
    volume_path : str
        書き出し先のVolumeパス (例: `/Volumes/tech_survey/ops/landing`)。
    records : list of dict
        書き出すレコード。

    Returns
    -------
    str
        書き出したファイルのフルパス。
    """
    os.makedirs(volume_path, exist_ok=True)
    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(volume_path, f"orders_{timestamp}_{uuid.uuid4().hex[:8]}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    return file_path
