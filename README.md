# streaming

Databricksのstreaming/バッチ更新まわりの技術調査リポジトリ。メダリオンアーキテクチャにおける日次更新ETL戦略を検討するため、以下5カテゴリ・14トピックをDatabricksノートブック形式で調査する。

DAB (Databricks Asset Bundles) を前提に `src` レイアウトで構成している。

## トピック一覧

### セットアップ

- [00. セットアップ](src/notebooks/00_setup.py) — カタログ/スキーマ/Volumeの作成とサンプルデータ投入

### A. 取り込み

- [01. Auto Loader (`cloudFiles`)](src/notebooks/a_ingestion/01_auto_loader.py)
- [02. Structured Streaming 基礎](src/notebooks/a_ingestion/02_structured_streaming_basics.py)
- [03. Change Data Feed (CDF)](src/notebooks/a_ingestion/03_change_data_feed.py)

### B. 変換・書き込み

- [04. `replaceWhere` によるパーティション部分置換](src/notebooks/b_transform_write/04_replace_where.py)
- [05. `MERGE INTO` によるUpsert](src/notebooks/b_transform_write/05_merge_into.py)
- [06. 冪等な書き込み](src/notebooks/b_transform_write/06_idempotent_writes.py)
- [07. ウォーターマークと遅延データ処理](src/notebooks/b_transform_write/07_watermark_late_data.py)

### C. 宣言的パイプライン

- [08. Lakeflow Declarative Pipelines (旧DLT)](src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.py)
- [09. データ品質 (Expectations)](src/notebooks/c_declarative_pipelines/09_data_quality_expectations.py)

### D. テーブル最適化・運用

- [10. OPTIMIZE / Z-ORDER / Liquid Clustering](src/notebooks/d_table_optimization/10_optimize_zorder_liquid_clustering.py)
- [11. VACUUM とタイムトラベル](src/notebooks/d_table_optimization/11_vacuum_time_travel.py)
- [12. スキーマ進化](src/notebooks/d_table_optimization/12_schema_evolution.py)

### E. オーケストレーション・監視

- [13. Databricks Workflows/Jobs](src/notebooks/e_orchestration_monitoring/13_workflows_jobs.py)
- [14. Structured Streamingのメトリクス監視](src/notebooks/e_orchestration_monitoring/14_streaming_metrics_monitoring.py)

各調査を踏まえた全体設計方針は [docs/etl_strategy.md](docs/etl_strategy.md) にまとめる。

## セットアップ

### 1. ローカル環境

```sh
uv sync
```

カタログ名やスキーマ名を変えたい場合は `src/streaming/common/config.py` を編集する。
変更した場合は `resources/pipelines/lakeflow_declarative_pipelines.pipeline.yml` の `catalog` / `schema` も合わせる。

### 2. Databricksへの初回デプロイ

`databricks.yml` は `~/.databrickscfg` の `free` プロファイルを参照している。別のプロファイルを使う場合は書き換える。

初回は以下の順番で実行する。

```sh
# 1. ノートブックをワークスペースに同期する
#    カタログが未作成のためパイプライン作成だけエラーになるが、この時点では想定どおり
databricks bundle deploy --target dev

# 2. ワークスペース上で 00_setup ノートブックを実行する
#    カタログ/スキーマ/Volumeとサンプルデータが作成される
#    /Workspace/Users/<ユーザー名>/.bundle/streaming/dev/files/src/notebooks/00_setup

# 3. 再デプロイする。カタログができているのでパイプラインも作成される
databricks bundle deploy --target dev
```

> **なぜ初回だけデプロイが2回必要か**
>
> パイプライン定義は `catalog: tech_survey` を参照するが、このカタログを作るのは `00_setup.py` である。
> 一方で `00_setup.py` をワークスペースで実行するには、先にファイルを同期しておく必要がある。
> この循環のため、初回のみ「同期 → セットアップ実行 → 再デプロイ」の順を踏む。
> 2回目以降は `databricks bundle deploy --target dev` の1回で完結する。

手順2をCLIで行う場合は、一度きりのジョブとして実行する。

```sh
databricks jobs submit --json '{
  "run_name": "streaming-00-setup",
  "tasks": [{
    "task_key": "setup",
    "notebook_task": {
      "notebook_path": "/Workspace/Users/<ユーザー名>/.bundle/streaming/dev/files/src/notebooks/00_setup"
    }
  }]
}'
```

## ディレクトリ構成

```
streaming/
├── databricks.yml              # DABバンドル定義
├── docs/
│   └── etl_strategy.md         # 全体設計方針
├── resources/
│   ├── jobs/                   # 手動オーケストレーション用Job定義
│   └── pipelines/              # Lakeflow Declarative Pipelines定義
├── src/
│   ├── notebooks/              # 調査用ノートブック (トピック別)
│   └── streaming/               # 共通Pythonパッケージ
└── tests/
```

## 2回目以降のデプロイ

初回セットアップさえ済んでいれば、以降は1回のデプロイで完結する。

```sh
databricks bundle validate
databricks bundle deploy --target dev
```

デプロイすると、ノートブックは以下に同期される。ブラウザで開けばNotebook UIとして実行できる。

```
/Workspace/Users/<ユーザー名>/.bundle/streaming/dev/files/src/notebooks/
```

devターゲットではJob/Pipelineに `[dev <ユーザー名>]` のプレフィックスが付き、開発用としてマークされる。
作成したリソースをまとめて削除したい場合は `databricks bundle destroy --target dev` を使う
(Unity Catalogのカタログ/スキーマ/Volumeはバンドル管理外なので、`00_setup.py` 末尾の後片付けセルで消す)。
