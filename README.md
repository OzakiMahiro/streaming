# streaming

Databricks の streaming / バッチ更新まわりを手を動かして学ぶためのリポジトリ。
メダリオンアーキテクチャにおける日次更新ETL戦略を検討することを目標に、5カテゴリ・14トピックを順に調べる。

各トピックは Jupyter ノートブック (`.ipynb`) 1本で完結する。
ローカルの VS Code でセルを1つずつ実行し、処理自体は databricks-connect 経由で Databricks 側で動く。

## トピック一覧

- [00. セットアップ](src/notebooks/00_setup.ipynb) — カタログ / スキーマ / Volume を作る

### A. 取り込み

- [01. Auto Loader (`cloudFiles`)](src/notebooks/a_ingestion/01_auto_loader.ipynb) — 増分取り込み、チェックポイント、スキーマ推論
- [02. トリガー](src/notebooks/a_ingestion/02_structured_streaming_basics.ipynb) — `availableNow` と `once`、サーバーレスの制約
- [03. Change Data Feed (CDF) によるテーブル間伝播](src/notebooks/a_ingestion/03_change_data_feed.ipynb)

### B. 変換・書き込み

- 04. `replaceWhere` によるパーティション部分置換
- 05. `MERGE INTO` によるUpsert
- 06. 冪等な書き込み
- 07. ウォーターマークと遅延データ処理

### C. 宣言的パイプライン

- 08. Lakeflow Declarative Pipelines (旧DLT)
- 09. データ品質 (Expectations)

### D. テーブル最適化・運用

- 10. OPTIMIZE / Z-ORDER / Liquid Clustering
- 11. VACUUM とタイムトラベル
- 12. スキーマ進化

### E. オーケストレーション・監視

- 13. Databricks Workflows / Jobs
- 14. Structured Streaming のメトリクス監視

リンクが付いているものが作成済み。それ以外はこれから作る。

各調査を踏まえた全体設計方針は [docs/etl_strategy.md](docs/etl_strategy.md) にまとめる。

## セットアップ

### 1. ローカル環境

この環境は Databricks のサーバーレスと**バージョンを揃えて**ある。
ローカルと Databricks 側で挙動が食い違うのを防ぐため、Python やライブラリのバージョンは自分で選ばず、
`databricks environments setup-local` に決めてもらう。

```sh
uv sync
```

環境を作り直す場合や、サーバーレスのバージョンが上がった場合は次を実行する。

```sh
databricks environments setup-local --profile <プロファイル名> --serverless-version <N>
uv python pin 3.12   # 解決されたPythonバージョンに合わせる
```

### 2. 認証

`~/.databrickscfg` にプロファイルが必要。ノートブックは `profile="free"` を指定しているので、
別名のプロファイルを使う場合は各ノートブックの `DatabricksSession` の行を書き換える。

```sh
databricks auth describe --profile free
```

### 3. 入れ物を作る

[00_setup.ipynb](src/notebooks/00_setup.ipynb) を実行して、カタログ・スキーマ・Volume を作る。1回だけでよい。

## ノートブックの動かし方

1. VS Code で `.ipynb` を開く
2. カーネルにこのプロジェクトの `.venv` (Python 3.12) を選ぶ
3. 上から順にセルを実行する

各ノートブックは冒頭でテーブルと取り込み元データをリセットするので、何度実行しても同じ結果になる。
取り込み元は `landing/<トピック名>/` のようにトピックごとに分かれていて、互いに干渉しない。

### `display()` について

VS Code の Databricks 拡張機能が有効だと、`display(df)` が表として描画される。
拡張機能が IPython に HTML フォーマッタを登録していて、内部で `df.limit(20).toPandas()` を呼んでいる。
拡張機能なしの素の Jupyter では表にならないので、その場合は `df.show()` を使う。

## この環境の制約

Databricks Free Edition は**サーバーレス専用**で、クラシックコンピュートを作れない。
そのため終わらないトリガー (`processingTime`、無指定) は使えず、`availableNow` と `once` だけが使える。
詳細は [02](src/notebooks/a_ingestion/02_structured_streaming_basics.ipynb) で扱う。

## ディレクトリ構成

```
streaming/
├── databricks.yml       # VS CodeのDatabricks拡張機能が参照する。08/09のパイプラインでも使う
├── docs/
│   └── etl_strategy.md  # 全体設計方針
├── resources/
│   └── pipelines/       # Lakeflow Declarative Pipelines の定義 (08, 09で使う)
└── src/
    └── notebooks/       # 調査用ノートブック
```
