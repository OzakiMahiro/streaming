# streaming

Databricks の streaming / バッチ更新まわりを手を動かして学ぶためのリポジトリ。
メダリオンアーキテクチャにおける日次更新ETL戦略を検討することを目標に、5カテゴリ・15トピックを順に調べる。

各トピックは Jupyter ノートブック (`.ipynb`) 1本で完結する。
ローカルの VS Code でセルを1つずつ実行し、処理自体は databricks-connect 経由で Databricks 側で動く。

## トピック一覧

- [00. セットアップ](src/notebooks/00_setup.ipynb) — カタログ / スキーマ / Volume を作る

### A. 取り込み

- [01. Auto Loader (`cloudFiles`)](src/notebooks/a_ingestion/01_auto_loader.ipynb) — 増分取り込み、チェックポイント、スキーマ推論
- [02. トリガー](src/notebooks/a_ingestion/02_structured_streaming_basics.ipynb) — `availableNow` と `once`、サーバーレスの制約
- [03. Change Data Feed (CDF) によるテーブル間伝播](src/notebooks/a_ingestion/03_change_data_feed.ipynb)

### B. 変換・書き込み

- [04. 選択的上書き](src/notebooks/b_transform_write/04_replace_where.ipynb) — `replaceWhere` と `replaceUsing`、Liquid Clustering
- [05. mergeInto](src/notebooks/b_transform_write/05_merge_into.ipynb) — 列単位の更新、`whenNotMatchedBySource`、SQLとの対応
- [06. 冪等な書き込み](src/notebooks/b_transform_write/06_idempotent_writes.ipynb) — `txnAppId` / `txnVersion`、`foreachBatch`
- [07. ウォーターマークと遅延データ処理](src/notebooks/b_transform_write/07_watermark_late_data.ipynb)

### C. 宣言的パイプライン

- [08. Lakeflow Declarative Pipelines](src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb) — ストリーミングテーブルとマテリアライズドビュー、DABでのデプロイ
- [09. データ品質 (Expectations)](src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb) — `expect` / `drop` / `fail` の使い分け、イベントログ
- [15. Auto CDC](src/notebooks/c_declarative_pipelines/15_auto_cdc.ipynb) — 変更を当てる、スナップショットを畳む (`14` の後に追加)

### D. テーブル最適化・運用

- [10. OPTIMIZE と Liquid Clustering](src/notebooks/d_table_optimization/10_optimize_clustering.ipynb) — スモールファイル問題、Z-ORDER との違い
- [11. タイムトラベルと VACUUM](src/notebooks/d_table_optimization/11_vacuum_time_travel.ipynb) — 過去を読む / 戻す、保持期間の決め方
- [12. スキーマ進化](src/notebooks/d_table_optimization/12_schema_evolution.ipynb) — Auto Loader でのスキーマ推論と進化

### E. オーケストレーション・監視

- [13. Workflows / Jobs](src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb) — タスクの依存、リトライ、LDP との使い分け
- [14. ストリーミングの監視](src/notebooks/e_orchestration_monitoring/14_streaming_metrics.ipynb) — `recentProgress`、未処理量の見方

番号順に読むことを想定している。後の回は前の回で確かめたことを前提にしている。

### X. 総括
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

このリポジトリの検証は **Databricks Free Edition** で行っている。

Free Edition は**サーバーレス専用**で、クラシックコンピュートを作れない。
そのため終わらないトリガー (`processingTime`、無指定) は使えず、`availableNow` と `once` だけが使える。
詳細は [02](src/notebooks/a_ingestion/02_structured_streaming_basics.ipynb) で扱う。

このほか、サーバー側でPythonを実行する仕組みが動かない
(`foreachBatch`、Python UDF が使えない。[06](src/notebooks/b_transform_write/06_idempotent_writes.ipynb) で扱う)。

**実際に設計しているシステムは Databricks on AWS の有償版** なので、これらの制約は当てはまらない。
どこが変わるかは [docs/etl_strategy.md](docs/etl_strategy.md) の「検証環境と本番環境の差」にまとめてある。

## ディレクトリ構成

```
streaming/
├── databricks.yml       # DABの定義。VS Codeの拡張機能も参照する
├── docs/
│   └── etl_strategy.md  # 全体設計方針。15本を踏まえた判断の記録
├── resources/           # Databricks側に作るものの定義 (DABが読む)
│   ├── jobs/            # ジョブ (13)
│   └── pipelines/       # Lakeflow Declarative Pipelines (08, 09, 15)
└── src/
    ├── notebooks/       # 調査用ノートブック。ローカルのVS Codeで実行する
    ├── jobs/            # ジョブのタスク (13)。Databricks側で動く
    └── pipelines/       # パイプラインのソース (08, 09, 15)。Databricks側で動く
```

`src/notebooks/` だけがローカルで動く。
`src/jobs/` と `src/pipelines/` は **Databricks側で動くコード** で、
`databricks bundle deploy` でワークスペースに配置してから使う。
そのため `.ipynb` ではなく `.py` で置いてある。
