<p align="center"><img src="docs/site/src/assets/logo.svg" alt="The Ptah mark: an amber capstone above two sky-blue courses on a dark rounded square" width="72" height="72"></p>

<h1 align="center">Ptah</h1>

<p align="center">Open-source database change management for schemas and persistent inference state.</p>

<p align="center">
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-unit-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-unit-tests.yml?branch=master&label=tests&logo=github" alt="Status of the unit test workflow on the master branch"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/go-integration-tests.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/go-integration-tests.yml?branch=master&label=integration&logo=github" alt="Status of the integration test workflow on the master branch"></a>
  <a href="https://github.com/stokaro/ptah/actions/workflows/capability-matrix.yml?query=branch%3Amaster"><img src="https://img.shields.io/github/actions/workflow/status/stokaro/ptah/capability-matrix.yml?branch=master&label=databases&logo=github" alt="Status of the capability matrix workflow on the master branch, which probes every declared database release line"></a>
  <a href="https://github.com/stokaro/ptah/releases/latest"><img src="https://img.shields.io/github/v/release/stokaro/ptah?label=release&logo=github" alt="The latest published release tag"></a>
  <a href="https://pkg.go.dev/ptah.run"><img src="https://pkg.go.dev/badge/ptah.run.svg" alt="The Go package reference for ptah.run"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/go.mod"><img src="https://img.shields.io/github/go-mod/go-version/stokaro/ptah?label=go&logo=go&logoColor=white" alt="The Go version declared in go.mod"></a>
  <a href="https://github.com/stokaro/ptah/blob/master/LICENSE"><img src="https://img.shields.io/github/license/stokaro/ptah?label=license&color=blue" alt="The license badge, reading MIT"></a>
</p>

<p align="center"><a href="#install">Install</a> · <a href="https://docs.ptah.run/edge/start/quick-start/">Quick start</a> · <a href="https://docs.ptah.run/edge/inference/overview/">Inference migrations</a> · <a href="https://docs.ptah.run/edge/">Documentation</a> · <a href="https://docs.ptah.run/edge/databases/support-matrix/">Database support</a></p>

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

Ptah manages database change across schemas and persistent inference state. For
schemas, it compares a desired schema with a live database and either writes
versioned migrations or applies an approved plan directly. For inference state,
it builds a candidate generation beside the active one, calls an external
embedding endpoint, verifies the result, and switches consumers with a rollback
path.

The command-line interface runs without a Go toolchain, and the same planning
components are available as Go packages.

## Schema changes

<p align="center"><img src="docs/site/src/assets/product-journeys.svg" alt="Schema sources and a live database produce a reviewable plan that either becomes versioned migration files or is applied directly. Inference specifications and source rows produce a candidate generation that is verified before cutover while the active generation remains available for rollback." width="1000"></p>

Both workflows use the same comparison and planning model. The difference is
whether SQL becomes a reviewed artifact in version control before it runs.

## Persistent inference state

Ptah orchestrates the migration; it does not run inference. It reads source
rows, calls the external endpoint, and writes the candidate generation itself,
leaving the active generation untouched until verification and cutover.

<p align="center"><img src="docs/site/src/assets/inference-generation-lifecycle.svg" alt="The active inference generation continues serving queries while Ptah prepares, backfills, catches up, indexes, and verifies a candidate. Cutover makes the verified candidate active; rollback can restore the retained previous generation, and retirement is separate and destructive." width="1000"></p>

The [inference migrations guide](https://docs.ptah.run/edge/inference/overview/)
covers the specification, concurrent-change catch-up, evaluation, approvals,
rollback, and retirement.

> [!NOTE]
> Ptah is pre-GA. The native command tree and public Go API can still change.

## Install

The installer selects the current release for Linux, macOS, or Windows, verifies
its checksum, and installs `ptah`, `ptah-compat`, and `ptah-ls` under your home
directory.

```bash
curl -fsSL https://ptah.run/install.sh | sh
```

In PowerShell:

```powershell
irm https://ptah.run/install.ps1 | iex
```

The [installation guide](https://docs.ptah.run/edge/start/install/)
covers version pinning, signature verification, download-without-execution,
and building from source.

<!-- ptah:readme-example -->
## Try Ptah with SQLite

Save this desired schema as `schema.sql`:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE
);
```

Render the SQL, apply it to a throwaway database, and check that the database
matches the file:

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
ptah schema drift --db-url "sqlite://app.db" --schema-file schema.sql
```

Expected output includes:

```text
CREATE TABLE "users" (
```

Expected output includes:

```text
Schema apply completed successfully.
```

Expected output includes:

```text
No schema drift detected.
```

The last command exits 0 when the database matches the file, which is what makes
it usable as a CI gate. Remove `app.db` and `schema.sql` when you are done.

> [!CAUTION]
> `--auto-approve` skips the confirmation prompt. A direct schema change can
> drop objects that the desired schema does not declare. Use it here only
> because `app.db` is disposable.

For a complete workflow with expected output and verification, use the
[direct schema changes tutorial](https://docs.ptah.run/edge/start/quick-start-direct/)
or the
[versioned migrations tutorial](https://docs.ptah.run/edge/start/quick-start-migrations/).

## Choose how schema changes land

| Workflow | Use it when | Start with |
| --- | --- | --- |
| Versioned migrations | SQL files belong in code review and deployment history | `ptah migrations generate` |
| Direct schema changes | The desired schema is authoritative and you want to review and apply the plan now | `ptah schema plan` |

Schema sources can come from SQL, YAML, HCL, DBML, Go annotations, external
loaders, or a live database. Database and feature coverage vary by engine; use
the [support matrix](https://docs.ptah.run/edge/databases/support-matrix/)
and `ptah db capabilities --db-url <url>` for the concrete target.

## Explore the documentation

- [Choose a workflow](https://docs.ptah.run/edge/start/choose-a-workflow/)
  to compare versioned migrations with direct schema changes.
- [Inspect a live database](https://docs.ptah.run/edge/direct/inspect/)
  or [compare and detect drift](https://docs.ptah.run/edge/direct/compare-and-drift/).
- [Validate migration integrity](https://docs.ptah.run/edge/versioned/integrity-and-safety/)
  or [test migrations and schemas](https://docs.ptah.run/edge/testing/migrations-and-schema/).
- [Visualize](https://docs.ptah.run/edge/schema/visualize/)
  or [export](https://docs.ptah.run/edge/schema/export/) a schema.
- [Migrate persistent inference state](https://docs.ptah.run/edge/inference/overview/)
  while an external endpoint computes embeddings.
- [Look up native commands](https://docs.ptah.run/edge/reference/native-commands/)
  or [diagnose a failure](https://docs.ptah.run/edge/operate/troubleshooting/).

The site source lives in [`docs/site`](docs/site). [`docs/README.md`](docs/README.md)
indexes contributor and implementation documents outside the reader site.

## Go packages and Atlas compatibility

Go projects can embed the documented packages, use annotated structs as schema
sources, and run `ptah-ls` for editor support. Start with the
[public API ledger](https://docs.ptah.run/edge/extend/public-api/),
[reusable components](https://docs.ptah.run/edge/extend/components/),
or [Go annotations](https://docs.ptah.run/edge/schema/go-annotations/).

The separate `ptah-compat` binary exposes an Atlas-compatible command surface;
the native `ptah` command tree does not use Atlas command paths. Ptah does not
claim full Atlas parity. The
[compatibility overview](https://docs.ptah.run/edge/atlas/overview/)
and [conformance results](https://docs.ptah.run/edge/atlas/conformance/)
state the measured coverage and differences.

## License and help

Ptah is an independent clean-room implementation published under the
[MIT license](LICENSE). It does not use Atlas source code and is not affiliated
with or endorsed by Ariga. The
[license boundary](https://docs.ptah.run/edge/atlas/license-boundary/)
records the provenance policy.

The database marks above identify the engines Ptah supports and nothing more.
Each belongs to its owner, and none of those owners endorses or sponsors Ptah.
[NOTICE](NOTICE) says where each file came from and under which license.

For questions and bug reports, open an issue in
[stokaro/ptah](https://github.com/stokaro/ptah/issues).
