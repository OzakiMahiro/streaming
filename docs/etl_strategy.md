# メダリオンアーキテクチャ 日次更新ETL戦略

各トピックの調査 (`src/notebooks/`) を踏まえて、このプロジェクトとしての設計判断をまとめる。詳細な技術調査は各ノートブックの `%md` セルを参照。

## TODO

- [ ] メダリオン各層 (Bronze / Silver / Gold) の更新方式をそれぞれ決める
- [ ] Jobs方式 (手動オーケストレーション) と LDP (Lakeflow Declarative Pipelines) 方式のどちらを採用するか、層ごとに整理する
- [ ] 日次更新のトリガー戦略 (`availableNow` トリガー vs スケジュールJob) を決める
- [ ] 冪等性の設計方針 (`replaceWhere` / `MERGE` / `txnAppId`+`txnVersion` のどれをどこで使うか) をまとめる
- [ ] スキーマ進化・データ品質チェックの方針をまとめる
- [ ] テーブル最適化 (OPTIMIZE/VACUUM) の実行タイミングを決める

## Jobs方式 vs LDP方式の比較

TODO

## 冪等性の設計方針

TODO

## 参考資料

TODO
