# Ptah GitHub Action

Ptah ships a Marketplace GitHub Action as `stokaro/ptah-action@v1` and keeps
the source composite action at `.github/actions/ptah`. The action generates a
migration plan for a pull request, evaluates the safety verdict, optionally
lints a migration directory, and updates one sticky pull request comment. It
also writes a dedicated `Ptah destructive-change verdict` check run from the
machine-readable safety report.

For repository use:

```yaml
name: Ptah

on:
  pull_request:

permissions:
  checks: write
  contents: read
  issues: write
  pull-requests: write

jobs:
  ptah:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: stokaro/ptah-action@v1
        with:
          dir: ./internal/models
          db-url: ${{ secrets.PTAH_DATABASE_URL }}
          dialect: postgres
          migration-dir: ./migrations
          lint: "true"
          comment: "true"
```

Use `stokaro/ptah/.github/actions/ptah@master` only when validating changes to
the in-repository composite action before they are published to
`stokaro/ptah-action`.

The action downloads the requested Ptah release binary by default. Use
`version` to pin a release tag, or `binary-path` when a workflow builds Ptah
from source before invoking the action. If a release asset is not available yet,
the installer falls back to `go install ptah.run/cmd/ptah@...`
using `master` for `version: latest`. Source fallback uses direct module fetch
to avoid stale Go module proxy results for moving refs.

The `checks: write` permission is needed for the destructive-change check run.
The `issues: write` and `pull-requests: write` permissions are needed for
sticky pull request comments. On forked pull requests without write-scoped
tokens, the action skips the comment or check-run write and still completes the
local Ptah validation path.

## Inputs

| Input | Default | Description |
| --- | --- | --- |
| `version` | `latest` | Ptah release tag to download. |
| `binary-path` | empty | Existing Ptah binary path. Skips release download. |
| `setup-go` | `true` | Set up the Go toolchain before running Ptah. |
| `go-version` | empty | Go version passed to `actions/setup-go`. Empty reads the toolchain from `go-version-file` instead. |
| `go-version-file` | `go.mod` | Go module file the toolchain is read from, relative to the calling repository. |
| `dir` | `.` | Root directory scanned for Go schema entities. |
| `db-url` | required | Target database URL used to read the current schema. |
| `dialect` | empty | Dialect passed to `ptah migrations lint`. |
| `migration-dir` | `migrations` | Migration directory passed to lint. |
| `schemas` | empty | Comma-separated database schemas to inspect. |
| `comment` | `true` | Whether to write a sticky PR comment. |
| `lint` | `true` | Whether to run `ptah migrations lint`. |
| `lint-fail-on` | `error` | Lint failure threshold: `error`, `any`, or `none`. |
| `allow-destructive` | `false` | Allows destructive plans after review. |
| `output-dir` | temporary | Directory for generated reports. |

## Outputs

| Output | Description |
| --- | --- |
| `plan-path` | Text migration plan report. |
| `safety-path` | JSON safety report. |
| `safety-error-path` | Text stderr captured from the safety report command. |
| `lint-path` | JSON lint report. |
| `lint-error-path` | Text stderr captured from the lint command. |
| `destructive` | `true`, `false`, or `unknown`. |

## Behavior

The action sets up Go by default, downloads the selected Ptah release unless
`binary-path` is provided, and then runs:

```bash
ptah migrations plan --report text
ptah migrations plan --report json --check-destructive
ptah migrations lint --format json
```

The text plan is always generated without `--check-destructive` so the pull
request comment still contains the SQL reviewers need to inspect. The separate
JSON safety command controls the destructive-change failure gate. When
`allow-destructive` is `false`, destructive plans fail the job after the comment
is posted. Lint failures also fail the job after the comment is posted.

The action requires a database URL. Use a disposable database for pull request
workflows. For SQLite smoke tests, `sqlite:///${{ runner.temp }}/ptah.db` is
enough; for PostgreSQL, MySQL, MariaDB, SQL Server, CockroachDB, or YugabyteDB,
start the database as a service container or provide a secret URL.

## Where the action lives

`stokaro/ptah-action` is a published copy of `.github/actions/ptah` in this
repository, not a second place to edit. `publish-action.yml` mirrors the
directory there on a push to master that touches it, and moves the `v1` tag so
a workflow pinning `stokaro/ptah-action@v1` receives the change.

`scripts/check-action-publishable.sh` runs on every pull request and refuses a
tree that could not be published: a missing `README.md` or `LICENSE`, a script
`action.yml` names but does not ship, or a path reaching outside the directory,
which exists here and does not once published.

Publishing authenticates as a GitHub App holding `Contents: Read and write`,
read from the `PUBLISH_APP_ID` variable and the `PUBLISH_APP_KEY` secret. The
workflow narrows the token it mints to `stokaro/ptah-action` alone, so a key
that leaks cannot write to the repository that stores it.

An app beats a personal token for a job that runs unattended. It is not tied to
whoever created it, so it survives that person's access changing, and the token
expires in an hour rather than on a date somebody picked. The variable is named
for publishing rather than for this action because the Homebrew formula and the
site redeploy write into a sibling repository the same way, and one app narrowed
per run is the credential the three should share.

Without them a push reports in its summary that the tree is publishable and was
not published; a manual dispatch fails instead, because someone asking for a
publish should not be told about it in a summary nobody opens.

The action's desired-schema inputs mirror the CLI's source selectors: `dir`
becomes `--root-dir`, `schema-file` becomes `--schema-file` and takes SQL,
YAML, HCL, DBML or `oci://` values, and `schema-cmd` with `schema-format`
becomes the external-loader pair. `dir` and `schema-file` accept one value per
line and compose. A run selecting no Go source installs no Go toolchain.
