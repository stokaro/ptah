# Ptah

Ptah helps you inspect, define, compare, visualize, test, and change database
schemas. Use versioned migrations, declarative schema changes, or both, across
supported databases.

The CLI runs on its own and needs no Go toolchain. The
[database support matrix](docs/site/src/content/docs/databases/support-matrix.md)
lists every engine Ptah supports, the dialect name and URL scheme each one uses,
and how deep the support goes.

## Two ways to change a schema

| Workflow | You write | Entry commands |
| --- | --- | --- |
| Versioned migrations | Numbered SQL files kept in version control | `ptah migrations generate`, `ptah migrations up` |
| Declarative schema changes | The schema you want, as SQL, YAML, HCL, or DBML | `ptah schema plan`, `ptah schema apply` |

The two combine. One command reads a declarative schema source, compares it with
a live database, and writes the difference as a reversible migration pair:

```bash
ptah migrations generate --db-url "sqlite://app.db" --schema-file schema.sql \
  --migrations-dir ./migrations --name create_users
```

[Choose a workflow](docs/site/src/content/docs/start/choose-a-workflow.md)
compares them in detail.

## What Ptah does

Ptah is published under the MIT license. Migration integrity, deterministic
planning, drift detection, approvals, advisory locking on the engines that
provide it, recovery, testing, and artifact distribution are all part of the same
open-source project. Deterministic means that the same inputs produce the same
SQL on every run.

| Capability | Native commands |
| --- | --- |
| Migration integrity | `ptah migrations hash`, `ptah migrations validate`, and `--verify-sum` on `up`, `down`, `ls`, `show`, `status` and `push` |
| Deterministic planning | `ptah schema render`, `ptah schema plan`, `ptah migrations plan` |
| Drift detection | `ptah schema drift`, `ptah schema compare` |
| Approvals | `ptah schema approve`, `ptah schema verify-approval` |
| Advisory locking | `--lock-timeout` on `ptah schema apply`, `--migration-lock-timeout` on `migrations up`, `down`, `baseline` and `checkpoint` |
| Recovery | `ptah migrations down`, `ptah migrations repair`, `ptah migrations rebase`, `ptah migrations baseline` |
| Testing | `ptah schema test`, `ptah migrations test` |
| Artifact distribution | `ptah oci`, `ptah schema push`, `ptah schema pull`, `ptah migrations push`, `ptah migrations pull` |
| Documentation and diagrams | `ptah schema export --to markdown`, `ptah schema export --to html` |

Not every engine provides every capability. Advisory locking is the clearest
example: PostgreSQL, YugabyteDB, MySQL, MariaDB and SQL Server take a lock, and
on SQLite, ClickHouse, CockroachDB and Spanner the migration runs without one.
Run `ptah db capabilities --db-url <url>` to see what Ptah resolves for one
server.

## Install

The released archives carry `ptah`, `ptah-compat`, and `ptah-ls` for Linux,
macOS, and Windows on both `amd64` and `arm64`. On Linux and macOS:

```bash
set -euo pipefail

VERSION="$(curl -sSL https://api.github.com/repos/stokaro/ptah/releases/latest \
  | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p')"
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
[ "$ARCH" = "x86_64" ] && ARCH=amd64
[ "$ARCH" = "aarch64" ] && ARCH=arm64
ARCHIVE="ptah_${VERSION#v}_${OS}_${ARCH}.tar.gz"

cd "$(mktemp -d)"
curl -sSLO "https://github.com/stokaro/ptah/releases/download/${VERSION}/${ARCHIVE}"
curl -sSLO "https://github.com/stokaro/ptah/releases/download/${VERSION}/checksums.txt"
if command -v sha256sum >/dev/null; then
  sha256sum --ignore-missing -c checksums.txt
else
  shasum -a 256 --ignore-missing -c checksums.txt
fi

tar -xzf "$ARCHIVE"
sudo install -m 0755 ptah ptah-compat ptah-ls /usr/local/bin/
ptah version
```

`set -euo pipefail` is what makes the checksum a gate: without it a failed
comparison is followed by the install anyway. The archive has no top-level
directory, which is why the binaries are installed one by one instead of
unpacked straight into `/usr/local/bin`. Stock macOS ships `shasum` rather than
`sha256sum`, which is what the branch above selects between.

The [install guide](docs/site/src/content/docs/start/install.md) covers
installing through the Go toolchain and building from a checkout.

To install through the Go toolchain instead:

```bash
go install go.5x5.cz/ptah/cmd/ptah@latest
go install go.5x5.cz/ptah/cmd/ptah-compat@latest
go install go.5x5.cz/ptah/cmd/ptah-ls@latest
```

To build from a checkout:

```bash
GOWORK=off go build -o ./bin/ptah ./cmd/ptah
./bin/ptah version
```

Ptah is pre-GA. The native command tree can still change.

## A minimal example

Save the schema you want as `schema.sql`. Any of the supported source formats
works here; this one is plain SQL:

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
```

See the SQL Ptah would run against SQLite:

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
```

Expected output includes:

```sql
-- Statement 1/1
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL UNIQUE,
  "created_at" TEXT NOT NULL
);
```

Ptah writes the payload to standard output and progress messages to standard
error, so the block above is what a pipe receives.

Apply it to a database:

```bash
ptah schema apply --db-url "sqlite://app.db" --schema-file schema.sql --auto-approve
```

Expected output includes:

```text
Planned schema changes:
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL UNIQUE,
  "created_at" TEXT NOT NULL
);
Auto-approval enabled; applying schema changes.
Schema apply completed successfully.
```

Confirm that the database matches the file. This command exits 0 when they agree
and 1 when they do not, so it also works as a CI gate:

```bash
ptah schema drift --db-url "sqlite://app.db" --schema-file schema.sql
```

Expected output includes:

```text
No schema drift detected.
```

Remove the database when you are done:

```bash
rm app.db
```

## Documentation

| I want to | Read |
| --- | --- |
| Install the CLI | [Install](docs/site/src/content/docs/start/install.md) |
| Run my first migration | [Quick start](docs/site/src/content/docs/start/quick-start.mdx) |
| Decide between the two workflows | [Choose a workflow](docs/site/src/content/docs/start/choose-a-workflow.md) |
| Bring a database Ptah did not create under management | [Adopt an existing database](docs/site/src/content/docs/start/adopt-an-existing-database.mdx) |
| Generate, apply, verify, and roll back migrations | [Versioned migrations](docs/site/src/content/docs/versioned/overview.md) |
| Inspect a live database and apply a desired schema to it | [Inspect](docs/site/src/content/docs/direct/inspect.md), [Compare and drift](docs/site/src/content/docs/direct/compare-and-drift.md), [Apply](docs/site/src/content/docs/direct/apply.md) |
| Write the schema in SQL, YAML, HCL, DBML, or an external loader | [Work with a schema source](docs/site/src/content/docs/schema/work-with-a-source.mdx) |
| Document or visualize a schema | [Export a schema](docs/site/src/content/docs/schema/export.md), [Visualize a schema](docs/site/src/content/docs/schema/visualize.md) |
| Test schemas and migrations, and gate a pull request | [Test migrations and schemas](docs/site/src/content/docs/testing/migrations-and-schema.md), [CI](docs/site/src/content/docs/testing/ci.md) |
| Publish or consume artifacts through an OCI registry | [OCI registry artifacts](docs/site/src/content/docs/operate/oci-registry.md) |
| Check engine support and dialect differences | [Database support matrix](docs/site/src/content/docs/databases/support-matrix.md), [Capabilities](docs/site/src/content/docs/reference/capabilities.mdx) |
| Look up a command, a format, or an exit code | [Native commands](docs/site/src/content/docs/reference/native-commands.md), [Exit codes](docs/site/src/content/docs/reference/exit-codes.md) |
| Diagnose a failure | [Troubleshooting](docs/site/src/content/docs/operate/troubleshooting.md) |

The site source lives in [`docs/site`](docs/site) and is built with Astro and
Starlight.

## Go integration

The CLI needs no Go toolchain. Go projects can also embed Ptah through its
documented Go packages, use Go annotations as one of the schema sources, and run
the `ptah-ls` language server for annotation support in an editor.

- [Public Go API](docs/site/src/content/docs/extend/public-api.md)
- [Reusable components](docs/site/src/content/docs/extend/components.md)
- [Go annotations](docs/site/src/content/docs/schema/go-annotations.md)

## Atlas compatibility

A separate `ptah-compat` binary presents an Atlas-compatible command surface for
scripts written against the Atlas CLI, invoked as `ptah-compat <command> ...`.
The native `ptah` binary has no Atlas command paths; its migration verbs live
under `ptah migrations`. The two binaries share capabilities, not command lines.

Ptah does not claim full Atlas parity. The evidence lives in the separate
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository. The
[Atlas compatibility overview](docs/site/src/content/docs/atlas/overview.md)
explains what the surface covers and where it differs, and
[Conformance](docs/site/src/content/docs/atlas/conformance.mdx) summarizes the
current measurements.

## License and provenance

Ptah is published under the [MIT license](LICENSE). It is an independent
implementation that does not use Atlas source code and is not affiliated with or
endorsed by Ariga. The
[license boundary](docs/site/src/content/docs/atlas/license-boundary.md) page
records the provenance policy and its legal basis. `ptah license` prints the
license and attribution from the binary itself.

## The name

Ptah is the Egyptian god of architects and craftsmen. The name also spells the
four stages a schema change moves through: **Parse** a schema source,
**Transform** it into statements for one dialect, **Apply** the change, and
**Harmonize** the database with the schema you declared.

## Repository references

The site is the reader-facing entry point. These files carry contributor or
implementation detail beyond it:

- [OCI registry artifacts](docs/oci_registry.md)
- [Project configuration](docs/project_config.md)
- [Atlas project config subset](docs/atlas_project_config.md)
- [HCL schema](docs/atlas_hcl_schema.md)
- [YAML schema](docs/yaml_schema.md)
- [Capabilities](docs/capabilities.md)
- [Declarative database testing](docs/testing.md)
- [Exit codes](docs/exit_codes.md)
- [GitHub Action](docs/github_action.md)
- [System design](docs/system_design.md)

Runnable examples: [schema visualization](examples/viz),
[embedded migrator](examples/migrator), and
[annotation parser](examples/annotation_parser).

## Build the documentation site

```bash
cd docs/site
npm ci
npm run build
```

For versioned output, set `DOCS_VERSION`:

```bash
DOCS_VERSION=edge npm run build
```
