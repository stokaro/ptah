<p align="center"><img src="docs/site/src/assets/logo.svg" alt="Ptah のマーク。濃色の角丸正方形の上に、空色の二層と琥珀色の冠石が重なる図" width="72" height="72"></p>

<h1 align="center">Ptah</h1>

<p align="center">Ptah（プタハ）は、スキーマと永続的な推論状態を対象とするオープンソースのデータベース変更管理ツールです。</p>

<p align="center"><a href="README.md">English</a> · <strong>日本語</strong></p>

<p align="center">
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-unit-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-unit-tests.yml?branch=master&label=tests&logo=github" alt="master ブランチにおける単体テストワークフローの状態"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-integration-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-integration-tests.yml?branch=master&label=integration&logo=github" alt="master ブランチにおける統合テストワークフローの状態"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/capability-matrix.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/capability-matrix.yml?branch=master&label=databases&logo=github" alt="master ブランチにおける機能マトリクスワークフローの状態。宣言されたすべてのデータベースリリースラインを検査する"></a>
  <a href="https://github.com/stokaro/ptah/releases/latest"><img src="https://img.shields.io/github/v/release/stokaro/ptah?label=release&logo=github" alt="公開済みの最新リリースタグ"></a>
  <a href="https://pkg.go.dev/ptah.run"><img src="https://pkg.go.dev/badge/ptah.run.svg" alt="ptah.run の Go パッケージリファレンス"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/go.mod"><img src="https://img.shields.io/github/go-mod/go-version/stokaro/ptah?label=go&logo=go&logoColor=white" alt="go.mod が宣言する Go のバージョン"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/LICENSE"><img src="https://img.shields.io/github/license/stokaro/ptah?label=license&color=blue" alt="MIT と表示されるライセンスバッジ"></a>
</p>

<p align="center"><a href="#インストール">インストール</a> · <a href="https://docs.ptah.run/edge/start/quick-start/">クイックスタート</a> · <a href="https://docs.ptah.run/edge/inference/overview/">推論マイグレーション</a> · <a href="https://docs.ptah.run/edge/">ドキュメント</a> · <a href="https://docs.ptah.run/edge/databases/support-matrix/">データベース対応状況</a></p>

<p align="center">
  <a href="https://docs.ptah.run/edge/databases/postgresql/" title="PostgreSQL"><img src="docs/assets/engines/postgresql.svg" alt="PostgreSQL" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/mysql/" title="MySQL"><img src="docs/assets/engines/mysql.svg" alt="MySQL" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/mysql/" title="MariaDB"><img src="docs/assets/engines/mariadb.svg" alt="MariaDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/sqlite/" title="SQLite"><img src="docs/assets/engines/sqlite.svg" alt="SQLite" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/sqlserver/" title="SQL Server"><img src="docs/assets/engines/sqlserver.svg" alt="SQL Server" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/clickhouse/" title="ClickHouse"><img src="docs/assets/engines/clickhouse.svg" alt="ClickHouse" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="CockroachDB"><img src="docs/assets/engines/cockroachdb.svg" alt="CockroachDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="YugabyteDB"><img src="docs/assets/engines/yugabytedb.svg" alt="YugabyteDB" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/oracle/" title="Oracle"><img src="docs/assets/engines/oracle.svg" alt="Oracle" height="32" width="32"></a>
  &nbsp;&nbsp;
  <a href="https://docs.ptah.run/edge/databases/distributed/" title="Spanner"><img src="docs/assets/engines/spanner.svg" alt="Spanner" height="32" width="32"></a>
</p>

Ptah はスキーマと永続的な推論状態の両方について、データベースの変更を管理します。
スキーマについては、目標スキーマと稼働中のデータベースを比較し、バージョン管理された
マイグレーションを書き出すか、承認済みの計画を直接適用します。推論状態については、
稼働中の世代の横に候補世代を構築し、外部の埋め込みエンドポイントを呼び出し、結果を
検証したうえで、ロールバック経路を残したまま利用側を切り替えます。

コマンドラインインターフェースは Go ツールチェーンなしで動作し、同じ計画コンポーネントを
Go パッケージとしても利用できます。

## スキーマの変更

<p align="center"><img src="docs/site/src/assets/product-journeys.svg" alt="スキーマソースと稼働中のデータベースからレビュー可能な計画が生成され、それがバージョン管理されたマイグレーションファイルになるか、直接適用される。推論仕様とソース行からは候補世代が生成され、切り替え前に検証される。その間も稼働中の世代はロールバックのために残る" width="1000"></p>

どちらのワークフローも同じ比較モデルと計画モデルを使います。違いは、SQL が実行される前に
バージョン管理下のレビュー対象成果物になるかどうかだけです。

## 永続的な推論状態

Ptah が担うのはマイグレーションの進行であり、推論そのものは実行しません。ソース行を読み、
外部エンドポイントを呼び出し、候補世代を自分で書き込みます。検証と切り替えが終わるまで、
稼働中の世代には手を触れません。

<p align="center"><img src="docs/site/src/assets/inference-generation-lifecycle.svg" alt="稼働中の推論世代がクエリに応答し続ける間に、Ptah が候補世代の準備、バックフィル、追いつき、インデックス作成、検証を行う。切り替えによって検証済みの候補が稼働状態になり、保持された前世代へロールバックできる。世代の廃棄は別の破壊的な操作である" width="1000"></p>

[推論マイグレーションガイド](https://docs.ptah.run/edge/inference/overview/)には、仕様、
並行変更への追いつき、評価、承認、ロールバック、廃棄が書かれています。

> [!NOTE]
> Ptah は GA 前です。ネイティブのコマンドツリーと公開 Go API はまだ変更される可能性が
> あります。

## インストール

インストーラーは Linux、macOS、Windows それぞれの現行リリースを選び、チェックサムを
検証し、`ptah`、`ptah-compat`、`ptah-ls` をホームディレクトリ配下に配置します。

```bash
curl -fsSL https://ptah.run/install.sh | sh
```

PowerShell の場合:

```powershell
irm https://ptah.run/install.ps1 | iex
```

[インストールガイド](https://docs.ptah.run/edge/start/install/)では、バージョンの固定、
署名の検証、実行せずにダウンロードする方法、ソースからのビルドを扱っています。

## SQLite で Ptah を試す

次の目標スキーマを `schema.sql` として保存します。

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);
```

SQL を生成し、使い捨てのデータベースに適用し、データベースがファイルと一致することを
確認します。

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
ptah schema drift --db-url "sqlite://app.db" --schema-file schema.sql
```

出力には次の行が含まれます。

```text
CREATE TABLE "users" (
```

出力には次の行が含まれます。

```text
Schema apply completed successfully.
```

出力には次の行が含まれます。

```text
No schema drift detected.
```

最後のコマンドは、データベースがファイルと一致するときに 0 で終了します。これが CI の
ゲートとして使える理由です。確認が済んだら `app.db` と `schema.sql` を削除してください。

> [!CAUTION]
> `--auto-approve` は確認プロンプトを省略します。直接のスキーマ変更は、目標スキーマが
> 宣言していないオブジェクトを削除することがあります。ここで使えるのは `app.db` が
> 使い捨てだからです。

期待される出力と検証手順を含む完全なワークフローは、
[直接スキーマ変更のチュートリアル](https://docs.ptah.run/edge/start/quick-start-direct/)
または
[バージョン管理マイグレーションのチュートリアル](https://docs.ptah.run/edge/start/quick-start-migrations/)
にあります。

## スキーマ変更の反映方法を選ぶ

| ワークフロー | 適している場面 | 最初のコマンド |
| --- | --- | --- |
| バージョン管理マイグレーション | SQL ファイルをコードレビューとデプロイ履歴に残したいとき | `ptah migrations generate` |
| 直接のスキーマ変更 | 目標スキーマが正であり、計画を今レビューして適用したいとき | `ptah schema plan` |

スキーマソースには SQL、YAML、HCL、DBML、Go アノテーション、外部ローダー、稼働中の
データベースを使えます。データベースと機能の対応範囲はエンジンごとに異なります。対象を
確かめるには
[対応マトリクス](https://docs.ptah.run/edge/databases/support-matrix/)と
`ptah db capabilities --db-url <url>` を使ってください。

## ドキュメントを読む

- [ワークフローを選ぶ](https://docs.ptah.run/edge/start/choose-a-workflow/)では、
  バージョン管理マイグレーションと直接のスキーマ変更を比較します。
- [稼働中のデータベースを調べる](https://docs.ptah.run/edge/direct/inspect/)、
  [比較してドリフトを検出する](https://docs.ptah.run/edge/direct/compare-and-drift/)。
- [マイグレーションの整合性を検証する](https://docs.ptah.run/edge/versioned/integrity-and-safety/)、
  [マイグレーションとスキーマをテストする](https://docs.ptah.run/edge/testing/migrations-and-schema/)。
- スキーマを[可視化する](https://docs.ptah.run/edge/schema/visualize/)、
  [エクスポートする](https://docs.ptah.run/edge/schema/export/)。
- 外部エンドポイントが埋め込みを計算する間に
  [永続的な推論状態を移行する](https://docs.ptah.run/edge/inference/overview/)。
- [ネイティブコマンドを調べる](https://docs.ptah.run/edge/reference/native-commands/)、
  [失敗を診断する](https://docs.ptah.run/edge/operate/troubleshooting/)。

サイトのソースは [`docs/site`](docs/site) にあります。読者向けサイトの外にある
コントリビューター向け文書と実装文書の索引は
[`docs/README.md`](docs/README.md) です。

## Go パッケージと Atlas 互換性

Go プロジェクトは、文書化されたパッケージを組み込み、アノテーション付きの構造体を
スキーマソースとして使い、エディタ支援のために `ptah-ls` を実行できます。
[公開 API 台帳](https://docs.ptah.run/edge/extend/public-api/)、
[再利用可能なコンポーネント](https://docs.ptah.run/edge/extend/components/)、
[Go アノテーション](https://docs.ptah.run/edge/schema/go-annotations/)から始めてください。

`ptah-compat` は別のバイナリで、Atlas 互換のコマンド面を提供します。ネイティブの `ptah`
コマンドツリーは Atlas のコマンドパスを使いません。Ptah は Atlas との完全な同等性を
主張しません。測定された対応範囲と相違点は
[互換性の概要](https://docs.ptah.run/edge/atlas/overview/)と
[適合性の測定結果](https://docs.ptah.run/edge/atlas/conformance/)に書かれています。

## ライセンスとサポート

Ptah は独立したクリーンルーム実装であり、[MIT ライセンス](LICENSE)で公開されています。
Atlas のソースコードは使っておらず、Ariga との提携も推奨も受けていません。出所に関する
方針は[ライセンス境界](https://docs.ptah.run/edge/atlas/license-boundary/)に記録して
います。

上に並ぶデータベースの名称は、Ptah が対応するエンジンを示すためだけのものです。各名称は
それぞれの所有者に帰属し、いずれの所有者も Ptah を推奨・後援していません。各ファイルの
出所と適用ライセンスは [NOTICE](NOTICE) に書かれています。

質問とアイデアの共有は
[Discussions](https://github.com/stokaro/ptah/discussions) で、バグ報告と機能要望は
[issue トラッカー](https://github.com/stokaro/ptah/issues) でお願いします。
報告を実行可能にする条件と、変更が通すべきものは
[CONTRIBUTING.md](CONTRIBUTING.md) にあります。参加については
[行動規範](CODE_OF_CONDUCT.md)が適用されます。商用のお問い合わせは
`ask <at> stokaro.com` までお願いします。
