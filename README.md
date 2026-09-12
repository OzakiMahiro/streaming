# streaming

Databricksのstreaming/バッチ更新まわりの技術調査リポジトリ。メダリオンアーキテクチャにおける日次更新ETL戦略を検討するため、以下5カテゴリ・14トピックをDatabricksノートブック形式で調査する。

DAB (Databricks Asset Bundles) を前提に `src` レイアウトで構成している。

## トピック一覧

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

```sh
uv sync
```

`src/streaming/common/config.py` のカタログ名・スキーマ名・Volumeパスを自分の環境に合わせて設定する。

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

## デプロイ

```sh
databricks bundle validate
databricks bundle deploy --target dev
```
