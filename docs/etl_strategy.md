# deem_date モデルの日次更新ETL戦略

14本のノートブックで確かめたことを踏まえて、**このデータモデルでETLを組むなら何を選ぶか** を決めた文書。

技術の解説はしない。「どういう選択肢があって、なぜこれを選んだか」だけを書く。
個々の挙動の根拠は各ノートブックにあるので、番号で参照する。

## 前提

### 確定していること

| | 内容 |
|---|---|
| 源泉 | お客さんの **基幹システム** |
| バージョン列 | 全テーブルが `deem_date` を持つ。日付型だが、意味は **バージョン** |
| マスタの届き方 | `deem_date` ごとに **全件**。**変更がなくても** 全件入ってくる |
| 再送 | 同じ `deem_date` が2回届くことがある。その場合は **その `deem_date` の分を全て置き換える** |
| 鮮度 | 日次 |
| 環境 | **Databricks on AWS (有償版)** |
| 監視 | エラー時にメールと電話で知らせる仕組みを作る予定 |

### 未確定のこと

| | 内容 |
|---|---|
| **取り込み方式** | 基幹からどう受け取るか。ファイル配置か、Databricksの取り込み機能か、他か |

これが現時点で **いちばん大きい未決事項**。1章で扱う。

### こちらで置いた仮定

| | 仮定 |
|---|---|
| 遅延 | `deem_date` が版を表すので、遅れて届いても版は変わらない |
| 再送の単位 | 時系列マスタの再送でも、その `deem_date` の **全 `business_date` 分** が来る |

## データの3類型

同じ `deem_date` を持つが、意味が違う3種類がある。

| 類型 | 持ち方 | `deem_date` の意味 |
|---|---|---|
| トランザクション (時系列) | `business_date` = `deem_date` | ほぼ意味のない列。冗長 |
| マスタ (非時系列) | `deem_date` のみ | 版そのもの。例: 店舗マスタ |
| マスタ (時系列) | `business_date` × `deem_date` | **バイテンポラル**。「その版の時点で、その業務日をどう捉えていたか」 |

3つ目が特殊に見えるが、**扱いは統一できる**。
再送時の置き換え単位を `deem_date` に揃えれば、3類型とも同じ処理で書ける。

トランザクションの `deem_date` は冗長だが、**捨てないほうがよい**。
全テーブルで置き換え単位が `deem_date` に統一されることに価値がある。
処理を類型ごとに分岐させずに済む。

## このモデルをどう捉えるか

`deem_date` ごとに全件を持つ形は、一般に **スナップショット方式** と呼ばれる。
履歴を残す目的は SCD Type 2 と同じだが、機構が違う。

| | SCD Type 2 | この方式 |
|---|---|---|
| いつ行を足すか | 変化したときだけ | 毎回全件 |
| 期間の表し方 | `__START_AT` / `__END_AT` の区間 | `deem_date` 単体 |
| 容量 | 小さい | 大きい |
| ある時点の参照 | 区間の比較が要る | **`WHERE deem_date = X` だけ** |
| 変化点の抽出 | 自明 | 隣接する `deem_date` の差分が要る |

**「変更がなくても毎回入ってくる」のは無駄ではなく、参照の素直さを買っている。**
マスタとファクトの結合が `ON m.store_id = f.store_id AND m.deem_date = f.deem_date` で済む。
Type 2 だと区間の比較が結合条件に入り込む。

以降の判断はすべて、この捉え方が前提になる。

## 更新方式の対応表

判断の全体像。軸は「**入力に何が来て、何をしたいか**」。

| やりたいこと | Jobs (手続き) | LDP (宣言) | 検証状況 |
|---|---|---|---|
| 追記する | `.mode("append")` | `@dp.table` + `readStream` | 両方あり ([`01`](../src/notebooks/a_ingestion/01_auto_loader.ipynb) [`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb)) |
| 全件作り直す | `CREATE OR REPLACE TABLE AS SELECT` | マテリアライズドビュー (`read`) | 両方あり ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb)) |
| **範囲を入れ替える** | **`replaceWhere`** | REPLACE WHERE フロー | Jobs ([`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb)) / LDP未検証 |
| キーで行を丸ごと置換 | `replaceUsing` | — (Auto CDCに吸収) | Jobs ([`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb)) |
| **キーで行ごとに更新** | **`mergeInto`** | **`create_auto_cdc_flow`** | Jobs ([`05`](../src/notebooks/b_transform_write/05_merge_into.ipynb)) / LDP ([`15`](../src/notebooks/c_declarative_pipelines/15_auto_cdc.ipynb)) |
| スナップショットから履歴を作る | 無い (自分で書く) | **`create_auto_cdc_from_snapshot_flow`** | LDPのみ ([`15`](../src/notebooks/c_declarative_pipelines/15_auto_cdc.ipynb)) |

上から下へ行くほど、**入力が「全件」から「差分」に寄り**、**出力が「状態」から「履歴」に寄る**。

- 上2行 … 入力は全件。突き合わせをしない
- 中2行 … キーで突き合わせる
- 下2行 … 履歴の構築が絡む

`deem_date` モデルが `replaceWhere` (3行目) に落ちたのは、
**入力が「その版の全件」で、キーの突き合わせが要らない** から。
基幹が差分だけをくれる形に変われば、5行目に移動することになる。

なお `create_auto_cdc_from_snapshot_flow` (6行目) は `mergeInto` の対応物ではない。
入力が変更イベントではなく **スナップショットの列** で、Jobs 側に相当する機能が無い。

### 横断する要素

表に入らないものが2つある。どちらも「Jobsなら自分で持つ、LDPなら付いてくる」という構造。

| | Jobs | LDP |
|---|---|---|
| 冪等性 | 自分で担保する。`txnAppId` / `txnVersion` ([`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb)) または上書き | エンジンが持つ |
| 品質チェック | 自前の検証クエリ ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb) のタスク) | expectations ([`09`](../src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb)) |

ただし **LDPのREPLACE WHEREフローには expectations を併用できない** ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb))。
3行目をLDPでやると、右下の利点が1つ消える。2章で「ジョブ中心」を選んだ理由のひとつ。

## 決定サマリ

| 項目 | 決めたこと | 根拠 |
|---|---|---|
| 取り込み | **基幹が全件ファイルを吐く形を第一候補**。未確定 | 1章 |
| 全体構成 | ジョブ中心のバッチETL | [`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) [`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb) [`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb) |
| Bronze | 追記のみ。置き換えない | [`01`](../src/notebooks/a_ingestion/01_auto_loader.ipynb) [`12`](../src/notebooks/d_table_optimization/12_schema_evolution.ipynb) |
| Silver | **`deem_date` 単位の `replaceWhere`** | [`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) |
| 版の判別 | `_metadata.file_modification_time` を Bronze で1列持つ | [`12`](../src/notebooks/d_table_optimization/12_schema_evolution.ipynb) |
| Gold | 毎回作り直す | [`07`](../src/notebooks/b_transform_write/07_watermark_late_data.ipynb) |
| 冪等性 | `replaceWhere` と上書きで担保する | [`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) [`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb) |
| 品質チェック | Silver の手前に検証タスクを置く | [`09`](../src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb) [`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb) |
| 最適化 | Predictive Optimization に任せる | [`10`](../src/notebooks/d_table_optimization/10_optimize_clustering.ipynb) |
| 保持期間 | `VACUUM` は既定の7日から始める | [`11`](../src/notebooks/d_table_optimization/11_vacuum_time_travel.ipynb) |
| 監視 | `on_failure` の webhook + 未処理量 + 検証結果 | [`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb) [`14`](../src/notebooks/e_orchestration_monitoring/14_streaming_metrics.ipynb) |

---

## 1. 取り込み — 未確定。判断軸は「誰がスナップショットを作るか」

**現状**: 基幹からどう受け取るかが決まっていない。

**判断軸**: `deem_date` ごとの全件スナップショットを **誰が作るか**。
このモデルの前提は「版ごとに全件が揃っていること」なので、そこが確保できるかで決まる。

| 方式 | スナップショットを作るのは | 相性 |
|---|---|---|
| 基幹が全件ファイルを出力 → S3 → Auto Loader | **基幹側** | **◎** |
| JDBC で日次に全件 SELECT | Databricks側 | ○ |
| Lakeflow Connect (データベース CDC) | Databricks側で組み立てる | △ |
| Lakeflow Connect (クエリベース) | カーソルによる増分 | △ |
| Lakehouse Federation | 作らない (都度クエリ) | × |

**第一候補**: 基幹が `deem_date` 付きの全件ファイルを吐き、S3 に置く。Auto Loader で取り込む。

理由は、**このモデルに変換を挟まないこと**。
`deem_date` モデルは「版ごとの全件」が前提で、基幹側がその形で出せるなら、
Databricks側は受け取るだけで済む。[`01`](../src/notebooks/a_ingestion/01_auto_loader.ipynb) で確かめた形がそのまま使える。

**CDC系が △ な理由**: Lakeflow Connect のデータベースコネクタは **変更イベント** を運ぶ仕組み。
「注文1が更新された」という形で届く。そこから「版ごとの全件」を組み立て直す処理が要る。
変換が挟まれば、そこが障害点になり、検証対象にもなる。
スナップショットが前提のモデルに、変更ベースの取り込みを合わせるのは遠回りになる。

**Federation が × な理由**: コピーせず都度クエリする仕組みなので、**履歴が残らない**。
`deem_date` ごとの版を保持する、というモデルの根幹と合わない。

**確認したいこと**:

- 基幹システムは全件ファイルを出力できるか。できるならこの論点は終わる
- できない場合、基幹の種類は何か (SQL Server は Lakeflow Connect が GA、Oracle はクエリベースのPublic Preview)
- 基幹はオンプレか。オンプレで Lakeflow Connect を使う場合、**AWS Direct Connect が必要**

## 2. 全体構成 — ジョブ中心

**決定**: ジョブのタスクとして、バッチ処理を順に並べる。LDPは現時点では採らない。

**理由**: このモデルの中核の操作が **`deem_date` 単位の全置換** だから。
これは [`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) で確かめた `replaceWhere` そのもので、バッチ書き込みのオプションになる。

LDPにも同等の仕組み (REPLACE WHERE フロー) があり、
`@dp.table(replace_where=...)` と書けることまでは確認した ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb))。
ただし2点が引っかかる。

- **このリポジトリで動かしていない**
- **expectations を併用できない** 制約がある ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb) で確認)

[`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) で実際に動かした方法が使える以上、まずそちらを採る。

**却下した案**: LDP 中心。
素直なメダリオン構成なら LDP のほうが書く量は減る ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb))。
ただしそれは **追記で流れていく形** に強いのであって、
版ごとの全置換が中核にあるこのモデルでは利点が薄れる。

なお、1章で Lakeflow Connect を選ぶ場合、**取り込み部分は自動的にLDPになる**
(Lakeflow Connect はLDPの上で動く)。その場合は構成を見直す。

## 3. Bronze — 追記のみ。置き換えない

**決定**: 届いたものを追記する。同じ `deem_date` が2回来ても、**両方そのまま残す**。

**理由**: Bronze で置き換えると、**「再送があった」という事実自体が消える**。
なぜあの日のデータが変わったのかを後から追う手がかりが無くなる。

技術的にも、Auto Loader の書き込みはストリーミングなので追記しかできない。
`replaceWhere` はバッチ書き込みのオプションで、ここでは使えない。

**版の判別**: Bronze で1列だけ足す。

```python
.withColumn("_file_time", F.col("_metadata.file_modification_time"))
```

同じ `deem_date` の行が複数あるとき、どれが新しい版かを判断するために要る。
`_metadata` はファイル系ソースに最初から付いている隠し列なので、自分で時刻を作る必要はない。
`current_timestamp()` よりファイル側の時刻のほうが正確で、取り込みが遅れても順序が狂わない。

ファイル以外の取り込み方式になった場合は、相当する情報を別に確保する必要がある。

**型とスキーマ**: 型変換も品質チェックもしない。

Auto Loader は既定で全列を文字列として推論する ([`12`](../src/notebooks/d_table_optimization/12_schema_evolution.ipynb))。
型を決めないので、上流が想定外の値を入れても取り込みが止まらない。
`cloudFiles.schemaEvolutionMode` は既定 (`addNewColumns`) のまま。
列が増えると1回落ちるが、それが **基幹側の変更に気づく機会** になる。
落ちたぶんはジョブのリトライで吸収する ([`12`](../src/notebooks/d_table_optimization/12_schema_evolution.ipynb) [`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb))。

## 4. Silver — `deem_date` 単位で置き換える

**決定**: その `deem_date` の最新版だけを取り出し、`replaceWhere` で入れ替える。

```python
(
    latest.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"deem_date = '{target}'")
    .saveAsTable(SILVER)
)
```

**理由**: 運用の要件が `replaceWhere` の性質と一致している。

[`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) で確かめたとおり、`replaceWhere` は **範囲内で渡さなかった行を消す**。
普通はこれが事故のもとになるが、このモデルでは **それが正しい動作** になる。
2回目のマスタに含まれていない店舗は、その `deem_date` から消えるべきだから。

「渡したものがその範囲の全てである」という前提が成り立っているときだけ使える手で、
ここはまさにその条件を満たしている。

**3類型すべてこの形で書ける。**
時系列マスタ (`business_date` × `deem_date`) でも、置き換え単位は `deem_date` のまま。
その版の全 `business_date` 分がまとめて入れ替わる。

**却下した案**: キーで突き合わせる方式 ([`05`](../src/notebooks/b_transform_write/05_merge_into.ipynb) の `mergeInto`、LDPの Auto CDC)。
これらは **渡さなかった行を残す** 仕組みなので、逆の性質になる。
2回目に含まれない店舗が消えず、要件を満たさない。

## 5. Gold — 毎回作り直す

**決定**: `CREATE OR REPLACE TABLE ... AS SELECT` で、対象範囲を作り直す。

**理由**: 作り直しなので冪等で、遅れて届いたデータも次の実行で反映される。

[`07`](../src/notebooks/b_transform_write/07_watermark_late_data.ipynb) でウォーターマークを扱ったが、**このモデルでは出番がない**。
遅延が問題になるのは「イベント時刻で区切って集計する」場合で、
`deem_date` という明示的な版がある以上、時間で区切る必要がない。

## 6. 冪等性 — 上書きで担保する

**決定**: `replaceWhere` と `CREATE OR REPLACE` で担保する。`txnAppId` / `txnVersion` は使わない。

[`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb) で見た3つの手段のうち、**書き込む中身の側から冪等性を作る** ものを選んでいる。

| 手段 | 使うか | 理由 |
|---|---|---|
| `replaceWhere` ([`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb)) | **使う** | 範囲を宣言して入れ替える。このモデルそのもの |
| `mergeInto` ([`05`](../src/notebooks/b_transform_write/05_merge_into.ipynb)) | 使わない | 渡さなかった行が残る。要件と逆 |
| `txnAppId` / `txnVersion` ([`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb)) | 使わない | 追記のみの場合の手段。番号の管理が事故のもとになる |

ジョブのタスクは何度でも実行されうる。失敗したジョブを再実行すると、
**成功済みのタスクも最初から動く** ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb))。
Bronze の追記だけは Auto Loader のチェックポイントが重複を防ぐ ([`01`](../src/notebooks/a_ingestion/01_auto_loader.ipynb))。
それ以外は上書きなので、何度実行しても同じ結果になる。

## 7. 品質チェック — Silver の手前にタスクを置く

**決定**: Bronze から Silver へ進む前に、検証タスクを1つ挟む。

**理由**: LDPを使わないので [`09`](../src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb) の expectations は使えない。あれはLDP専用の仕組み。
代わりにジョブのタスクとして検証クエリを実行する ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb))。

このモデル特有の観点として、次を見る。

- **件数が極端に少なくないか**。マスタが全件のはずなのに欠けていないか
- **`deem_date` が想定どおり1種類か**。複数混ざっていたら取り込み側の異常
- **キーの重複がないか**。`store_id` × `deem_date` で一意のはず

方針は [`09`](../src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb) と同じにする。

- **Bronze では弾かない**。基幹側が壊れた瞬間に取り込みが止まると、元ファイルが消えていれば戻せない
- **業務上あり得ない値だけ止める**。1件の異常で日次処理全体を止めない

## 8. テーブル運用

**最適化**: Predictive Optimization に任せる。`OPTIMIZE` を打つタスクは作らない ([`10`](../src/notebooks/d_table_optimization/10_optimize_clustering.ipynb))。

**クラスタリング**: Silver / Gold は `CLUSTER BY (deem_date)` で作る ([`04`](../src/notebooks/b_transform_write/04_replace_where.ipynb) [`10`](../src/notebooks/d_table_optimization/10_optimize_clustering.ipynb))。

`deem_date` は絞り込みにも `replaceWhere` にも使う列なので、寄せておく価値がある。
パーティションではなく Liquid Clustering にするのは、**後からキーを変えられる** ため。
時系列マスタは `CLUSTER BY (deem_date, business_date)` を検討する。

**保持期間**: `VACUUM` は既定の7日から始める ([`11`](../src/notebooks/d_table_optimization/11_vacuum_time_travel.ipynb))。

ただしこのモデルは **`deem_date` 自体が履歴を持っている**。
タイムトラベルに頼らなくても、過去の版はテーブルの中に残る。
`VACUUM` の保持期間が必要になるのは「誤った処理を流して上書きしてしまった」場合に限られる。
日次でチェックする前提なら7日で足りる。

## 9. 監視

エラー時にメールと電話で知らせる仕組みを作る予定、と聞いている。それを前提に置く。

| 何を | どこで | 通知 |
|---|---|---|
| ジョブの失敗 | ジョブの `on_failure` ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb)) | **webhook** 経由で電話まで飛ばす |
| 検証タスクの失敗 | 7章の検証クエリ | 同上 |
| 取り込みの遅れ | 未処理量 ([`14`](../src/notebooks/e_orchestration_monitoring/14_streaming_metrics.ipynb)) | メール程度でよい |

**電話まで飛ばすなら `email_notifications` では足りない。**
`webhook_notifications` を使って、PagerDuty のような通知基盤に投げる形になる。
ジョブ定義に書ける ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb))。

**電話を鳴らす対象は絞る。** 夜間に起こす価値があるのは、
「朝までに直さないと業務が止まる」ものだけ。
取り込みの遅れは、翌朝メールで気づけば間に合うことが多い。

`on_success` は付けない。成功のたびに通知すると読まれなくなる ([`13`](../src/notebooks/e_orchestration_monitoring/13_workflows_jobs.ipynb))。

**このモデルで特に怖いのは、同じ `deem_date` の再送に気づかないこと。**
Bronze に両方残しておけば、後から「2回来ていた」と分かる。3章の判断はここにも効く。

監視が要る理由は共通している。
[`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb) の `txnVersion`、[`07`](../src/notebooks/b_transform_write/07_watermark_late_data.ipynb) のウォーターマーク、[`09`](../src/notebooks/c_declarative_pipelines/09_data_quality_expectations.ipynb) の expectations、[`12`](../src/notebooks/d_table_optimization/12_schema_evolution.ipynb) の `rescue` は、
すべて **エラーを出さずに黙って進む**。見に行く仕組みがなければ気づけない。

## 10. 検証環境と本番環境の差

このリポジトリの14本は **Databricks Free Edition** で動かした。
本番は **Databricks on AWS 有償版** なので、検証中に当たった制約のいくつかは当てはまらない。

| 検証環境での制約 | 本番では | 影響 |
|---|---|---|
| サーバーレス専用。`availableNow` と `once` しか使えない ([`02`](../src/notebooks/a_ingestion/02_structured_streaming_basics.ipynb)) | **クラシックコンピュートが使える** | 継続実行のストリームも組める。日次なら影響なし |
| サーバー側でPythonが実行できない ([`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb) [`14`](../src/notebooks/e_orchestration_monitoring/14_streaming_metrics.ipynb)) | **使えるはず** | `foreachBatch`、Python UDF、`StreamingQueryListener` が選択肢に戻る |
| `retentionDurationCheck` を変更できない ([`11`](../src/notebooks/d_table_optimization/11_vacuum_time_travel.ipynb)) | **変更できるはず** | `VACUUM ... RETAIN` が自由に使える |

**この差は、判断の一部を見直す材料になる。**

特に `foreachBatch` が使えるなら、[`06`](../src/notebooks/b_transform_write/06_idempotent_writes.ipynb) で扱った「自分で書き込む」設計が選べる。
ただし **このモデルでは使う理由が薄い**。
中核が `replaceWhere` による全置換で、ストリームの中で書き込みを制御する必要がないため。

一方、[`14`](../src/notebooks/e_orchestration_monitoring/14_streaming_metrics.ipynb) の `StreamingQueryListener` は本番で価値がある。
取り込みのメトリクスを継続的に集める手段になる。

## 11. まだ確かめていないこと

判断の根拠が弱い部分。必要になったら調べる。

- **取り込み方式** … 1章のとおり未確定。ここが決まらないと3章も確定しない
- **`_metadata.file_modification_time`** … 3章で採用したが、このリポジトリで動かしていない
- **LDPの REPLACE WHERE フロー** … 構文はドキュメントで確認したが動かしていない ([`08`](../src/notebooks/c_declarative_pipelines/08_lakeflow_declarative_pipelines.ipynb))。
  2章の判断を見直す材料になる
- **`create_auto_cdc_from_snapshot_flow`** … スナップショットの列から SCD Type 2 を作る仕組み。
  容量が問題になったときの選択肢 (12章)
- **有償版での挙動** … 10章の「本番では使えるはず」は未検証

## 12. 前提が変わったら

**基幹が全件を出せないと分かったら**

- 1章の第一候補が消える。CDC系で取り込み、**スナップショットをこちら側で組み立てる** ことになる
- 組み立てる処理が新しく必要になり、そこが検証対象になる
- この場合は、そもそも `deem_date` モデルを維持するかから考え直す価値がある

**上流が差分だけをくれるようになったら**

- 4章が根本から変わる。`replaceWhere` は「渡さなかった行が消える」ので使えない
- [`05`](../src/notebooks/b_transform_write/05_merge_into.ipynb) の `mergeInto` か、LDPの Auto CDC に切り替える
- **この変更は基幹側の仕様変更で起きる**。気づかずに `replaceWhere` のままだと、
  差分に含まれない行が静かに消える。7章の検証で件数を見ているのは、ここを拾うためでもある

**容量が問題になったら**

- スナップショットを保つのをやめ、SCD Type 2 に畳む選択肢がある
- `create_auto_cdc_from_snapshot_flow` が、連続するスナップショットを比較して変化点だけを取り出す
- 代わりに **結合の素直さを失う**。参照のたびに区間の比較が入る
