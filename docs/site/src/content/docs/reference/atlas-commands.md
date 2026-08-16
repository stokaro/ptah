---
title: Atlas-compatible commands
description: Per-command status for every ptah-compat verb, with Atlas differences and known gaps.
---

This page is the lookup reference for the Atlas-compatible surface: what each
`ptah-compat <command>` does, where it differs from Atlas, and which inputs
fail explicitly. Usage, flags, and worked examples live on
[Atlas migrate commands](../../atlas/migrate-commands/) and
[Atlas schema commands](../../atlas/schema-commands/); the surfaces and
translation model are on the
[Atlas compatibility overview](../../atlas/overview/). Native verbs are on
[Native commands](../native-commands/).

The Atlas-compatible commands are hosted by the separate `ptah-compat` binary,
a drop-in replacement for scripts that need Atlas-style root commands; the main
`ptah` binary has no Atlas command paths. The invocations on this page are
written as `ptah-compat <command> ...`, the name the binary ships under. Each
verb section names its native `ptah` twin.

## Utility commands

| Command | Behavior |
| --- | --- |
| `ptah-compat version` | Prints Ptah build information. |
| `ptah-compat license` | Prints Ptah's MIT license and the license-clean Atlas compatibility notice. |
| `ptah-compat completion <shell>` | Generates shell completion output for the Atlas-compatible command tree under the invoked executable name. |

## Migrate commands

### `ptah-compat migrate apply`

Applies Atlas-format migration directories with Atlas-compatible apply flags
and Atlas revision bookkeeping by default. With `--env`, reads `env.url`,
`migration`, and `format.migrate.apply` from `atlas.hcl`.

Executes every Atlas OSS directory format selected by `migration.format` or a
`?format=` directory URL query; non-`atlas` formats are converted in memory to
up-only migrations.

Honors the `-- atlas:checkpoint` file directive: a fresh database applies
only the latest checkpoint plus post-checkpoint migrations, and a database
that already applied pre-checkpoint history silently skips the checkpoint,
matching measured Atlas behavior.

**Fails before the target database is opened:** unknown formats, two Flyway
files with the same exact revision identity, goose/dbmate files missing their
up directive, colliding versions, an Atlas directory that fails `atlas.sum` verification, and
an Atlas directory that carries no `atlas.sum` at all while holding at least one
top-level `.sql` file — both checksum refusals are byte-identical to
`ptah-compat migrate validate` and nothing is applied. A directory with no
top-level `.sql` file reports `No migration files to execute` and exits `0`.

Direct Flyway apply records each source version token byte for byte, including
dotted, dot-prefixed, padded, nonnumeric, baseline, token-ending-`R`, and empty
repeatable tokens. `--baseline` and the extended `--to-version` address those
exact tokens; the numeric projection controls execution order only. One
repeatable migration can own the empty token and remains settled after its body
is edited and rehashed. Two repeatables collide and are refused before the
target opens. Applied opaque history remains readable after its source file is
removed, without recreating pending work; its exact token still protects
source order. The known `.atlas_cloud_identifier` bookkeeping row remains
metadata rather than becoming a migration.

In `--format '{{ json . }}'` output, an exact empty current or target identity
is emitted as an explicit empty `Current` or `Target` member. Pending and
applied file records likewise keep an explicit empty `Version`. A genuinely
absent current or target state omits that member.

The revision table must distinguish every exact source token under its
configured version collation. Ptah-created MySQL and MariaDB tables use
`utf8mb4_bin`, and SQL Server tables use `Latin1_General_100_BIN2`. An existing
table whose collation aliases two covered tokens, such as `A` and `a`, is
refused before migration SQL runs rather than applying one body and failing on
the second primary-key write.

Atlas CE accepts a Ptah-written exact-token history. Ptah refuses a CE-written
row when its different per-revision checksum encoding cannot be verified. A
same-token `V2`/`B2` row is also refused when CE's ordinary applied type cannot
prove which byte-identical source ran; Ptah's own executed-baseline marker still
renders as `applied` and does not create `--baseline` boundary semantics. A
successful explicit `--baseline 2` that selects `B2` writes the ordinary
baseline type plus a durable source-baseline marker, settling that exact
identity on this and later runs. Baselining `V2` does not authorize a `B2`
introduced later with the same token. A Ptah `migrate set` of an executed
baseline renders as `manually set` while retaining a separate settled-baseline
marker; CE can still read the row.

The scan is top-level-only, matching what `atlas.sum` covers and what Atlas CE
reads. A `.sql` file in a subdirectory, or a top-level `.SQL`, is not a
migration and is not executed; each one is named on stderr as declined, which
Atlas CE does not do — see [#976](https://github.com/stokaro/ptah/issues/976).

A leading `-- atlas:txmode file` or `-- atlas:txmode none` header overrides
global `file` or `none` for that migration. File-level `all`, unknown values,
duplicates, and explicit file modes under global `all` fail before the affected
body or revision row changes. The directive belongs to the initial line-comment
header; a blank line after the header is accepted but not required. Txtar
`migration.sql` and `down.sql` sections carry independent modes; a mode before
the `-- atlas:txtar` marker is rejected as an unsafe archive classification.

Directories in an external tool's format are gated on the `atlas.sum` the source
directory carries, verified before the source layout is parsed and before the
database is opened. The covered file set is Atlas's for that layout, so a
golang-migrate down file and a Flyway undo file are not covered, and a layout
that carries no `atlas.sum` and whose covered set is empty is not a checksum
error. What executes is what the verified checksum covers, for every layout.

**Rejected on this verb, matching Atlas OSS:** `--dir-format`.

`--to-version` bounds the run at a migration version: every pending migration up
to and including it runs, and nothing above it does. The bound is enforced where
the apply plan is built, inside the migration lock, so a concurrent writer
cannot turn it into a different set of migrations; a version the directory does
not carry is refused rather than rounded to a neighbor, and the bound cannot be
combined with the amount argument, because the two select different prefixes and
neither outranks the other. The pinned community binary does not register the
flag — Atlas's published CLI reference does, which is what makes this a
Pro-surface addition rather than a CE parity row.

```bash
ptah-compat migrate apply --url "$DB" --dir file://migrations --to-version 20240101000002
```

**`--lock-name`** replaces the name of the session advisory lock that
serializes migration runs (`ptah_migrate` by default). Runs serialize only
against other runs naming the same lock. A lock another process holds makes the
run wait, bounded by `--lock-timeout`; an elapsed timeout fails the run before
any migration executes. An empty value is refused rather than silently falling
back to the default. On a dialect with no advisory-lock semantics the run
prints a stderr note naming the lock it did not acquire.

**`--skip-lock`** takes no lock at all, so a lock another process holds is
ignored rather than waited on and concurrent runs can interleave. It cannot be
combined with `--lock-name`.

Pre-migration checks — `-- +ptah check` directives and Atlas txtar
`checks.sql` / `checks/*.sql` sections — are enforced here as they are natively.
Atlas registers no `--skip-checks` on `migrate apply` (measured: CE v1.2.0
answers with `unknown flag`, and Atlas's own help surface registers it only
on `migrate down`), so the emergency bypass is the `PTAH_SKIP_CHECKS`
environment variable rather than a flag this surface must not grow:

```bash
PTAH_SKIP_CHECKS=1 ptah-compat migrate apply --url "$DB" --dir file://migrations
```

It reads like every other boolean `PTAH_*` variable — unset enforces the checks,
a valid boolean is honored, and anything else, an exported empty value included,
fails the run before a migration is applied. It warns on stderr while active and
bypasses checks only: `atlas.sum` verification and revision bookkeeping are
unaffected. See
[Pre-migration checks](../../versioned/integrity-and-safety/) and
[Boolean environment variables](../configuration/#boolean-environment-variables).

Native twin: [`ptah migrations up`](../native-commands/).

### `ptah-compat migrate status`

Reports Atlas-format migration status with Atlas revision-table metadata and
Atlas-format migration directories by default. Supports `--dir-format atlas`,
`--revisions-schema`, and Atlas Go-template `--format` output over `.Env`,
`.Available`, `.Applied`, `.Pending`, `.Current`, `.Next`, `.Status`, and — on
a half-applied migration — `.Count`, `.Total`, `.SQL`, and `.Error`.

For a direct Flyway directory, available, applied, pending, current, and next
versions use the exact source tokens. Numeric keys remain internal ordering
values and never replace those identities in the report. Current follows the
pinned binary's textual maximum over applied source tokens; numeric high-water
continues to govern execution internally. In JSON templates, a present empty
current identity and each empty file or revision identity remain explicit
empty members rather than disappearing through `omitempty`.

The default (no `--format`) report mirrors the Atlas shape, because this is the
verb pipelines parse:

```text
Migration Status: PENDING
  -- Current Version: No migration applied yet
  -- Next Version:    20260721120000
  -- Executed Files:  0
  -- Pending Files:   2
```

`Executed Files` counts revision rows and `Pending Files` counts directory files
not yet recorded. A half-applied migration annotates the first three lines
(`(1 statements applied)`, `(1 statements left)`, `(last one partially)`) and
adds a `Last migration attempt had errors:` block naming the failing statement.

Native twin: [`ptah migrations status`](../native-commands/), which keeps its
own block — only the compatibility surface is a contract with an existing
pipeline.

### `ptah-compat migrate hash`

Writes `atlas.sum` for the migration directory. `--dir-format` defaults to
`atlas`, so the compatibility path writes `atlas.sum` by default, and the
atlas layout forwards to `ptah migrations hash`. A successful compatibility
hash is silent, matching Atlas CE; inspect or commit the resulting `atlas.sum`
instead of relying on a progress message.

When `migration.dir` is a `data.template_dir` URL, the checksum belongs to the
immutable rendered view. The command remains silent and does not create an
`atlas.sum` beside the source templates.

### `ptah-compat migrate validate`

Silently verifies `atlas.sum` on success. Missing or mismatched checksum files
use Atlas-compatible exit-1 stdout/stderr diagnostics, and `--dev-url` cleans
the dev database and replays the migration directory to validate SQL
execution. Native `ptah migrations validate` keeps its own banner and exit
contract.

A `data.template_dir` URL is validated through its rendered snapshot; the
source templates are not modified.

### Source directory layouts on the verbs that read a directory

`hash`, `validate`, `lint`, `status`, and `set` read a migration directory
written by another tool. The layout is selected with either spelling Atlas
accepts, and `hash` and `validate` agree on the resulting `atlas.sum`:

```bash
ptah-compat migrate hash --dir "file://migrations?format=goose"
ptah-compat migrate hash --dir file://migrations --dir-format goose
```

Accepted values are `atlas` (the default), `golang-migrate`, `goose`, `flyway`,
`liquibase`, and `dbmate`. `migration.format` in `atlas.hcl` selects the same
thing under `--env`. When the query and the flag disagree, the query wins: an
empty `?format=` selects the atlas layout whatever the flag says.

Each layout covers a different set of source files, matching Atlas:

| Layout | Files the checksum covers |
| --- | --- |
| `atlas`, `goose`, `liquibase`, `dbmate` | every top-level `*.sql`, ordered by name |
| `golang-migrate` | every top-level `*.up.sql`. The down file of a pair is not covered, so editing it is invisible to `validate` |
| `flyway` | the whole tree: `V` and `B` files, then `R` repeatables last. `U` undo files are dropped, and everything at or below the highest baseline is squashed |

Format names are matched exactly, so `GOOSE` and `" goose "` are unknown
formats rather than `goose`. A directory with nothing for the layout to cover —
an empty directory, or a `golang-migrate` directory holding only a down file —
hashes to the empty-set checksum and validates clean.

An empty `--dir-format`, a query parameter other than `format`, and a repeated
`format` parameter are all accepted and read exactly as Atlas reads them: the
empty value and the unknown key select the atlas layout, and a repeated key
takes the first value.

`format` is the only query key that selects anything. A key that selected
nothing is named on standard error and the run continues, on each of the eight
verbs that accepts a `--dir` query — `apply`, `diff`, `hash`, `lint`, `new`,
`set`, `status` and `validate`:

```text
note: atlas migrate apply --dir: ignoring migration directory URL query key
"fromat". Only ?format= selects the directory layout. Set
PTAH_STRICT_DIR_QUERY=1 to refuse an unrecognized key instead.
```

The exit code and everything on standard output are unchanged, so a script
reading either sees exactly what Atlas produces. The note exists because
dropping the key is what Atlas does and saying nothing about it is not: a
misspelled `?fromat=goose` selects no layout on either tool, so the directory is
read as the atlas layout while you believe it is being read as Goose. Set
[`PTAH_STRICT_DIR_QUERY=1`](../../atlas/overview/#the-variables) to make that a
refusal instead.

`checkpoint`, `down`, `edit`, `rebase`, `rm` and `test` register `--dir` too and
refuse any query on it — `migration directory URL query parameters are not
supported for this command` — so neither the note nor the variable applies
there. The pinned community binary answers `unknown flag: --dir` on all six, so
this is stricter than a CLI with no contract on those verbs rather than a parity
gap.

Inputs that stay refused where Atlas CE exits 0, all of them loudly:

- a semicolon in the query, such as `?format=flyway;x=1`, which Atlas drops
  whole and reads as the atlas layout;
- a query on a `--dir` that a later `--dir` overrides;
- a stray positional argument, including one after `--`.

None of them can produce a wrong checksum. They are tracked in
[#990](https://github.com/stokaro/ptah/issues/990); the query rules are shared
with `migrate apply`, so relaxing one widens what the integrity gate accepts.

`migrate apply` registers no `--dir-format` at all, matching Atlas. It gates a
directory read through `?format=` over the same per-layout file set `hash`
writes, so what `migrate hash` writes is what `migrate apply` verifies.

`migrate new` and `migrate diff` both write the selected layout, gating the
directory over that layout's covered file set first. `migrate diff` composes
each layout's own files: a forward and a rollback file for `golang-migrate`
(`.up.sql` / `.down.sql`) and `flyway` (`V…` / `U…`), both halves under
directives in one file for `goose` and `dbmate`, and a changeset carrying
`--rollback:` lines for `liquibase`. `atlas.sum` is written over that layout's
covered file set, so the community binary's own `migrate validate` reads back
what Ptah wrote.

The generated SQL is Ptah's renderer's, on every layout including `atlas` — the
layout is what these follow, not the DDL text. On `liquibase`, Ptah writes ONE
changeset carrying the whole migration and all of its `--rollback:` lines, where
the community binary writes one changeset per statement; rolling the migration
back is exact either way, but Ptah does not offer per-statement rollback there,
because pairing each forward statement with a reverse statement would be a guess
about a reverse plan that is computed for the run as a whole.

The rollback half is planned against the state the migration starts from, not
the state it produces, so a forward migration that DROPS a table rolls back into
the CREATE TABLE that puts it back. That re-created table carries its own
primary key and its single-column foreign keys, and the rollback does not repeat
them; a rollback that did is refused by the server outright. A CHECK constraint
is not in the table body at all, so it is restored by its own statement.

Two differences survive that round trip, on this verb and on
`ptah migrations generate` alike. The restored primary key takes the server's
default name rather than the name it had, because the table body has nowhere to
put one. A column that was UNIQUE comes back both from the table body and from
the named constraint, so the restored table holds two unique constraints where
it held one. Neither stops the rollback from applying; both mean a
`schema diff` immediately after a rollback can report work to do.

### `ptah-compat migrate lint`

Runs Ptah migration linting with Atlas `--dir-format` defaulting to `atlas`.
It reads every Atlas source layout under both spellings, so a directory
another tool wrote can be linted without importing it first:

```bash
ptah-compat migrate lint --dir "file://migrations?format=golang-migrate" \
  --dev-url "sqlite://dev.db" --latest 1
```

The checksum step covers the file set that layout's `atlas.sum` covers, not the
Atlas one: on a `golang-migrate` directory, editing the covered `*.up.sql`
fails the lint and editing the uncovered `*.down.sql` does not, and a Flyway
`sub/V2__nested.sql` is covered.

Flyway lint reports exact source tokens in its migration detail lines. A single
repeatable uses a blank version detail and the generic analysis header, matching
the pinned community binary. If replay of an exact empty identity fails, the
error names it as `""` rather than leaving an anonymous gap in the diagnostic;
numeric and nonempty identities retain their ordinary spelling.

| Flag | Behavior |
| --- | --- |
| `--latest N` | Positive N selects the latest revision keys and remains exclusive with `--git-base`. Zero disables latest selection and configured `lint.latest`; explicit or configured Git may still select. With neither, the command returns `--latest or --git-base is required`. |
| `--git-base`, `--git-dir` | Map to native changeset linting. `--git-base` is the alternative to `--latest`. Changed Atlas repeatable files are selected by `R` or `<number>R`, not by a lossy numeric version. |
| `--dev-url` | Required. Infers the lint dialect, and cleans and replays migrations on directly connectable dev databases. |
| `--format` | Atlas Go-template output over `.Env`, `.Steps`, and `.Files`. The default is Atlas's migration-analysis text report. |

Both requirements are the community binary's, reproduced word for word — a run
missing `--dev-url` answers `required flag(s) "dev-url" not set` and one naming
no changeset answers `--latest or --git-base is required`, each at exit 1. Either
selector may come from the selected `atlas.hcl` env instead of the command line.
`PTAH_ATLAS_LINT_WITHOUT_DEV_URL=1` runs the analysis with no dev database, which
Ptah can do and that binary cannot.

A `docker://` dev database is provisioned: the container is started, used and
removed by the command. Atlas web reports remain an explicit gap.
Native twin: [`ptah migrations lint`](../native-commands/).

### `ptah-compat migrate new`

Creates a skeleton migration and updates `atlas.sum`; the native equivalent is
`ptah migrations create`. Every Atlas source layout is supported under both
spellings, and the created files follow the selected tool's convention:

| Layout | Files created |
| --- | --- |
| `atlas` | `<version>_<name>.sql`, empty |
| `golang-migrate` | `<version>_<name>.up.sql` and `.down.sql`, both empty |
| `flyway` | `V<version>__<name>.sql` and `U<version>__<name>.sql`, both empty |
| `goose` | `<version>_<name>.sql` holding `-- +goose Up` / `-- +goose Down` |
| `dbmate` | `<version>_<name>.sql` holding `-- migrate:up` / `-- migrate:down` |
| `liquibase` | `<version>_<name>.sql` holding `--liquibase formatted sql` |

`atlas.sum` is rewritten over the set the selected layout covers, so
`golang-migrate` and `flyway` create two files and cover one. On `atlas`,
`--edit` opens the created file in `$VISUAL`/`$EDITOR` before `atlas.sum` is
refreshed; on every other layout it is refused, as it is by Atlas.

For a project `migration.dir` backed by `data.template_dir`, the new root SQL
file and `atlas.sum` are synchronized to the template source directory. The
existing templates remain unchanged and the command stays silent.

A migration name is required on a non-`atlas` layout. Atlas accepts an omitted
name and writes the version alone, but such a file is one Ptah's own
`migrate apply` cannot read back on `golang-migrate`, `goose`, `liquibase` and
`dbmate`, so it is not created.

A migration name may not contain a path separator on this verb or on
`migrate diff`: the name becomes part of the file name, so a `/` in it selects a
directory that is not there. The run is refused and nothing is written, matching
the community binary's refusal of the same input
([#1231](https://github.com/stokaro/ptah/issues/1231)). Nothing else about a
name is refused — a space, a backslash and `..` are accepted, as they are there.

The directory's existing `atlas.sum` is verified first — over the selected
layout's covered file set — with the same output `migrate apply` and
`migrate validate` produce, and nothing is created when the check fails; see
[Which verbs enforce `atlas.sum`](../../atlas/migrate-commands/#which-verbs-enforce-atlassum).
An unrecognized `--dir` query key is ignored here and named on standard error,
as it is on the other seven verbs that accept a `--dir` query — `apply`,
`diff`, `hash`, `lint`, `set`, `status` and `validate`. It is not ignored on
`checkpoint`, `down`, `edit`, `rebase`, `rm` or `test`: those refuse a `--dir`
query outright, as the shared rules above record.

`--dir` must name a scheme on `migrate new`, `diff`, `hash`, `validate`,
`status`, and `lint`: `--dir migrations` is refused on those consumers with
`missing scheme for dir url. Did you mean "file://migrations"?` and creates
nothing. The stderr line ends with the bytes `20 0a`: one ASCII space followed
by the line feed. The same applies to its `PTAH_DIR` twin. A directory selected
by `atlas.hcl` `migration.dir` still accepts a bare path
([#1186](https://github.com/stokaro/ptah/issues/1186)).

Omitted entirely, `--dir` defaults to `file://migrations`, so
`ptah-compat migrate new add_users` creates `./migrations` and writes into it
([#1241](https://github.com/stokaro/ptah/issues/1241)). Missing parents are
created too: `--dir file://db/migrations` creates `db` and `db/migrations`. A
path component that exists and is not a directory is still refused, and nothing
is written. See
[The `--dir` default](../../atlas/migrate-commands/#the---dir-default) for how
the default ranks against `PTAH_DIR`, `PTAH_MIGRATIONS_DIR` and `atlas.hcl`.

### `ptah-compat migrate set [version]`

Moves Atlas revision history to the positional version without executing
migration SQL, with Atlas revision-table metadata and Atlas-format migration
directories by default. With `--env`, reads `env.url`, `migration.dir`, and
`migration.revisions_schema` from `atlas.hcl`; explicit `--dir`, `--url`, and
`--revisions-schema` flags keep CLI precedence. Flyway operands match exact
source tokens byte for byte, so `01` and `1` name different migrations. An
explicit empty positional operand selects a single repeatable's empty token.
The success summary renders that identity as `""`, both on the current-version
line and beside the changed revision, so the present token is not an anonymous
gap in operator output.

When the directory no longer owns an applied identity, metadata moves only if
the stored row preserves enough Flyway role information to order it against the
target. A known retired baseline stays before versioned migrations regardless
of its token. A known retired versioned row uses numeric component order
against a versioned target, but uses Flyway's raw-token squash cut against a
surviving baseline target: for example, B2 keeps retired V10 because `"10"`
sorts before `"2"`. The same raw-token order applies when both migrations are
known baselines: selecting B3 keeps retired B2, while selecting B10 removes
retired B20.

A single repeatable target follows retired versioned history even
though its exact identity is empty. This is deliberately safer
than Atlas CE v1.3.0: CE exits `0` but deletes the retired versioned row,
while Ptah exits `0` and keeps both revision identities. Rows that do not
distinguish those roles, including ordinary Atlas CE applied rows, refuse
before any revision row changes instead of guessing from token bytes.

Native twin:
[`ptah migrations set`](../native-commands/).

### `ptah-compat migrate down`

Forwards to `ptah migrations down` with mapped Atlas flags.

| Flag | Behavior |
| --- | --- |
| `--dev-url` | Replays and verifies the rollback plan on the dev database before the target is touched (native `--shadow-db`). |
| `--format` | Flag or `PTAH_FORMAT`; renders an Atlas Go-template report. Real and dry-run rollbacks are non-interactive. |
| `--revision-format` | Defaults to the `atlas` table layout, like `migrate set`. The native `ptah` pass-through selects the `ptah` layout. Both retain recoverable failed-down state. |

Because the forward defaults to the Atlas revision-table layout, a bare invocation
reverts the revisions `ptah-compat migrate apply` wrote.

A failed rollback stays dirty even with the Atlas layout. Resume it with
`ptah migrations repair --dir-format atlas --revision-format atlas`, using the
same database, directory, revision schema, version, and required
`--resume-from` statement as the failed compat run.

The command starts a real rollback without reading stdin, matching Atlas. It
does not accept the native `--confirm` flag. Review `--url`, `--dir`, and
`--to-version` before running it. Native `ptah migrations down` keeps its
interactive confirmation.

The registry-bound `--to-tag`, `--skip-checks`, and `--plan` flags are recorded
waivers that fail loudly with their rationale. `--to-tag` and `--plan` are also
settable through their `PTAH_<FLAG>` twins, and refusing them is the point:
setting `PTAH_TO_TAG` is a request for a capability Ptah lacks, and discarding
it would leave an empty rollback target that reverts the whole history.

`--skip-checks` is the single exception, and it is explicit-only. `migrate
apply` reads `PTAH_SKIP_CHECKS` as its pre-migration check bypass, so on this
verb the variable is not a request for Atlas Cloud down checks; it neither
refuses a rollback nor appears as an `[env: ...]` suffix in `--help`.

### `ptah-compat migrate diff`

Verifies the directory's `atlas.sum`, replays the selected migration layout on
`--dev-url`, diffs it against `--to`, and writes new migration files in that
layout. `atlas.sum` updates only after every file was written; a failed write
rolls the whole generation back.

For a project `migration.dir` backed by `data.template_dir`, replay uses the
immutable rendered snapshot while publication writes the new root SQL files and
`atlas.sum` to the template source directory. Existing templates are not
rewritten, and a successful writing run is silent so the backing host path does
not leak through the virtual migration URL.

The checksum refusal comes first — before the dev database is connected to and
before `--to` and `--dev-url` are required at all, which is the order Atlas uses
— so nothing is created on a directory it refuses. A directory that has never
been hashed and already holds a migration is refused; one that does not exist
yet, or holds no top-level `*.sql`, is not, which is how a project's first
migration gets written. An unrecognized `--dir` query key is ignored;
`?format=` and `--dir-format` select any of the six writable layouts. The
directory is verified over that layout's covered file set before the dev
database is opened.

Goose carries a whole-file `-- +goose NO TRANSACTION` directive when either the
forward or exact reverse plan requires no-transaction execution. The directive
governs both sections. golang-migrate, Flyway, dbmate, and Liquibase remain
fail-closed for those plans because their safe transaction metadata has not
been proven. The Atlas layout remains forward-only and carries `-- atlas:txmode
none` on its own file when required.

Both spellings of the layout are read the way the other verbs that accept a
`--dir` query read them. The value is matched verbatim, so `--dir-format ATLAS`
and `--dir-format " atlas "` are rejected rather than coerced, and an explicit
`?format=` outranks `--dir-format` — `--dir "file://migrations?format=atlas"
--dir-format golang-migrate` writes the Atlas-layout migration. An
unrecognized query key selects no layout, so `--dir-format` still decides
there.

The verb takes at most one positional, the migration name, and a second one is
refused with `accepts at most 1 arg(s), received 2`
([#1231](https://github.com/stokaro/ptah/issues/1231)). The name may not contain
a path separator, checked where the file would be written: a diff that finds no
changes writes nothing, never reaches the name, and still exits 0 — which is
what the community binary does.

**Desired state (`--to`)** accepts one of: local `.hcl`, `.yaml`, `.yml`, or
`.sql` files; one directly connectable database URL; one local Atlas migration
directory; or one `env://` reference into the evaluated `atlas.hcl`
environment. Source kinds cannot be mixed, the database source must use the
`--dev-url` dialect, and a desired database must not identify the same database
as `--dev-url`.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Atlas-hidden; prints the generated SQL instead of writing files. |
| `--format` | Renders generated SQL with `sql` and `.MarshalSQL` templates. The default is Atlas-style two-space indentation. |
| `--schema`/`-s` | Narrows the current and desired schemas used for comparison and output. |
| `--edit` | Opens the generated migrations in `$VISUAL`/`$EDITOR` before `atlas.sum` is finalized. |
| `--env` | Reads `env.schema.src`, `env.dev`, `migration.dir`, `format.migrate.diff`, and supported `diff` policy from `atlas.hcl`. |

`--schema` narrows comparison only: migration replay and cleanup still own the
complete [dev database realm](../../concepts/database-urls-and-dev-databases/).

**Concurrent indexes.** With `diff.concurrent_index.create`, new indexes are
planned as `CREATE INDEX CONCURRENTLY` and their files are tagged with the
Atlas `-- atlas:txmode none` directive, splitting mixed plans into a
transactional file followed by a concurrent-index file. Unsplittable mixes are
refused.

**`--lock-timeout`** bounds waiting for both Ptah's local migration-directory
lock and the exclusive dev-database lock:

- PostgreSQL, YugabyteDB, MySQL, MariaDB, and SQL Server use session advisory
  locks;
- SQLite, ClickHouse, and CockroachDB use an operating-system lock keyed by
  normalized database identity;
- dialects without a safe dev-database lock fail before cleanup.

Cross-host ClickHouse and CockroachDB replay is unsupported.

**`--qualifier`** prefixes every object in the generated statements with a
custom schema qualifier on PostgreSQL-family, MySQL, and MariaDB dev databases.
Invalid values, unsupported dialects, multi-schema plans, and statement kinds
Ptah cannot re-qualify yet (for example enum types) fail explicitly before any
file or checksum is written.

A `docker://` dev database is provisioned: the container is started, used and
removed by the command.
Native twin: [`ptah migrations generate`](../native-commands/).

### `ptah-compat migrate import`

Imports local `file://` migration directories from `atlas`, `golang-migrate`,
`goose`, `flyway`, `liquibase`, or `dbmate` format into a separate Atlas
single-file directory and writes `atlas.sum`. Flyway repeatable migrations are
converted to one-time versioned Atlas files rather than emitted with an `R`
suffix, so the imported directory remains stable under Ptah's revision model.

A conventional Liquibase formatted-SQL name such as `changelog.sql` makes the
importer parse every covered SQL file and emit one numeric Atlas file per
changeset, in lexical file and changeset appearance order. Versions are padded
to the digit width of the final version so lexical checksum order stays numeric.
Headerless or malformed members refuse the whole import before destination
creation.

A successful compatibility import is silent; inspect the destination directory
and its `atlas.sum` instead of relying on a progress message. Failures are still
reported on stderr. The native `ptah migrations import` converts the same source
formats into Ptah-native migrations instead, and reports what it wrote.

`--from` and `--to` are resolved by the same rules as every other verb's
`--dir`. Both require a scheme, the source layout comes from `--from`'s
`?format=` query or from `--dir-format` with the query winning, and the value is
matched verbatim — `FLYWAY` and `" flyway "` are refused. An empty `?format=`
selects the Atlas layout and outranks `--dir-format`, which makes the import a
no-op that is refused rather than performed.

Refusals are answered in Atlas's order: the source scheme, then the layout
value, then whether the source directory exists, then whether it is already in
the Atlas layout, then the target scheme. A source directory that is not there
is reported as missing rather than as a layout conflict.

### `ptah-compat migrate checkpoint [name]`

Forwards to `ptah migrations checkpoint`, replaying the migration directory on
the `--dev-url` dev database and writing a cumulative-schema checkpoint.

| Argument | Maps to |
| --- | --- |
| `--dir` | The native migrations directory. |
| Positional name (optional) | The checkpoint description, used as the file-name stem. |
| `--dir-format=atlas` | Writes the Atlas single-file checkpoint (default). |
| `--dir-format=ptah` | Writes the ptah reversible checkpoint pair. |
| `-s, --schema` | The native `--schemas` allow-list; repeat the flag or pass one comma-separated value. |
| `--qualifier` | The native `--qualifier`: prefixes every object the checkpoint creates with a schema qualifier. |
| `--lock-timeout` | The native `--migration-lock-timeout`, bounding the wait for the dev database's migration lock during the replay. |
| `--edit` | The native `--edit`: opens the written checkpoint files in `$VISUAL`, then `$EDITOR`, and refreshes the directory checksum afterwards. |

`--edit` refuses before it replays anything when the session could not finish:
with no editor configured, and when standard input is not a terminal. The second
refusal is the one that matters in CI, where an interactive editor launched
without a terminal does not fail but waits. Set
`PTAH_ALLOW_NONINTERACTIVE_EDIT=1` when `$EDITOR` is a script that edits and
exits on its own; that is an environment variable rather than a flag because
Atlas registers no flag for it. The native `--editor` selects a specific editor
command and is deliberately not part of this verb's Atlas flag surface.

`--lock-timeout` bounds a lock only on dialects that implement advisory locking.
On one that does not — SQLite, ClickHouse — the run says so on stderr rather than
accepting a bound that binds nothing.

`--qualifier` is refused on a dialect Ptah cannot qualify, so a checkpoint is
never written half-qualified.

Not registered here: `--format`, whose Go-template report needs the compat
formatting path this verb does not have yet, and `--lock-name`, which belongs to
the named-lock family.

`--dir-format` selects the checkpoint convention, and **on this verb it
defaults to `atlas`**, matching the default Atlas registers and every other
compat migrate verb:

- **`atlas`** writes one up-only file, `<version>_<name>.sql`, whose first line
  is the `-- atlas:checkpoint` directive, and refreshes `atlas.sum`. The version
  is a UTC timestamp, raised to one above the newest migration in the directory
  when that is higher — including migrations in subdirectories, which the replay
  and the reader both see but a bare timestamp would sort below. There is no
  down file: the Atlas format is up-only, so an Atlas checkpoint is not
  reversible.
- **`ptah`** writes the reversible pair
  `NNNNNNNNNN_<name>.checkpoint.up.sql` / `.checkpoint.down.sql` and refreshes
  `ptah.sum`.

The native `ptah migrations checkpoint` keeps `ptah` as its default; only the
compat surface defaults to `atlas`. Native accepts `--dir-format=auto` as a
spelling and refuses it with a named message, because writing under it would
have to guess the file convention and which integrity file to refresh. On the
compat surface `auto` is not an Atlas directory format at all, so it draws the
ordinary unknown-format rejection instead.

A directory must end up with one integrity file and one file convention, so two
shapes are refused before anything is written:

- a checkpoint that would leave both `ptah.sum` and `atlas.sum` behind, which
  `--dir-format auto` cannot read; and
- an Atlas-format checkpoint into a directory holding ptah-convention
  migrations, hashed or not. There the checkpoint would be permanently
  invisible: discovery reads the ptah files and never sees it, while
  verification finds the `atlas.sum` and reports the directory as valid.

Re-hash or convert the directory into one format first.

The read side honors the `-- atlas:checkpoint` directive either way: applying a
checkpoint directory bootstraps a fresh database from the latest checkpoint and
silently skips the checkpoint on a database that already applied the
pre-checkpoint history, matching measured Atlas behavior.

Atlas keeps `migrate checkpoint` in its Pro build — the pinned CE binary
registers the verb but aborts with "not supported by the community version" and
registers none of its own flags — so this is a free Ptah capability rather than
an Atlas CE stub.

`?format=` on this verb's `--dir` URL is still refused; use `--dir-format`. CE
aborts every `migrate checkpoint` invocation, so there is no CE behavior to
diverge from here and refusing an unimplemented spelling loudly is the intended
outcome. The eight verbs that accept a `--dir` query honor the parameter today;
`migrate diff` writes forward and reverse SQL in the selected layout.

### `ptah-compat migrate test [paths]`

Forwards to `ptah migrations test`.

| Atlas flag | Native equivalent |
| --- | --- |
| `--dir` | The native migration directory, Atlas-format by default via `--dir-format`. |
| `--dev-url` | The native throwaway database; an ephemeral SQLite database when omitted. |
| `--run` | The native case-name filter. |
| `--revisions-schema` | The native `--migrations-schema`: the schema a `migrate_to` step records revisions in. |
| Positional path (optional) | The directory of Ptah-native YAML test cases, default `./tests`. |

Exit codes match the native runner: 0 when all cases pass, 1 on test failure.

Atlas keeps `migrate test` in its Pro build, so this is a free Ptah capability
rather than an Atlas CE stub.

### `ptah-compat migrate edit {name | version}`

Forwards to `ptah migrations edit`: the positional maps to the native
`--version` (a migration file name contributes its leading version digits),
`--dir` maps to the native migration directory (Atlas-format by default via
`--dir-format`), the editor resolves from `$VISUAL`, then `$EDITOR`, and the
directory checksum is rewritten afterwards. Atlas keeps `migrate edit` outside
its community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah-compat migrate rebase {name | version}`

Forwards to `ptah migrations rebase`: re-timestamps the selected migration
past every existing version and rewrites the directory checksum. Multiple
positional values and `a...b` version ranges are rejected loudly; forward one
migration per run. Atlas keeps `migrate rebase` outside its community build,
so this is a free Ptah capability rather than an Atlas CE stub.

### `ptah-compat migrate rm {name | version}`

Forwards to `ptah migrations rm`: deletes the selected migration's files and
rewrites the directory checksum. Atlas keeps `migrate rm` outside its
community build, so this is a free Ptah capability rather than an Atlas CE
stub.

### `ptah-compat migrate push`

Registered but not implemented because the command targets an account-bound
hosted registry protocol. `--help` reports that the command is not implemented
and exits 0; direct execution reports the same status and exits 1. The open
replacement is the native `ptah migrations push` to any OCI registry.

## Schema commands

### `ptah-compat schema inspect`

Inspects the `--url` source and writes Atlas-compatible schema output without
Ptah status banners.

**Sources (`--url`)** accepts one of: a live database URL; a local `.hcl`,
`.yaml`, `.yml`, or `.sql` schema file; a migration directory (a directory
containing `atlas.sum`); or an `env://` reference resolved through the
evaluated `atlas.hcl` env.

Non-database sources require `--dev-url` and are evaluated on the dev database:
it is reset, the source is materialized on it (schema files executed, migration
directories replayed), and the result is introspected. Inspecting a file
without `--dev-url` fails with Atlas's `--dev-url cannot be empty` message.

**Output formats**

| Output | How to request it |
| --- | --- |
| HCL | The default, or `--format '{{ hcl . }}'`. |
| SQL | `--format '{{ sql . }}'`. |
| JSON | `--format '{{ json . }}'`. |
| Custom templates | `{{ .MarshalHCL }}`, `{{ hcl . }}`, `{{ sql . }}`, `{{ mermaid . }}`. |

Bare `--format hcl`, `--format sql`, and `--format json` write those literal
words. They add no line feed, and database contents do not change the values.
Surrounding whitespace is also preserved: `--format ' hcl '` writes hex
`20 68 63 6c 20`, with no line feed. Those literal cases match Atlas CE v1.3.0.
Native `ptah schema inspect --format hcl|sql|json` keeps its rendered
shorthands.

**Split-write exports.** `{{ hcl . | split | write "schema" }}` and
`{{ sql . | split | write "schema" }}` support the documented Atlas split
strategies: per object (the default, with a `main.sql` `atlas:import` entry
point for SQL), `split "schema"`, and `split "type"`, plus an optional
file-extension argument.

Exports render one output plan applied by a single writer. Duplicate output
paths, traversal or escape from the output directory, planned file/directory
collisions, and existing-directory destinations fail explicitly before anything
is written. The pinned Atlas CE binary rejects `split`, `write`, and `hcl` as
non-community template functions, so these exports are an open Ptah extension.

**Filtering**

- `--schema`/`-s` narrows inspection when supported by the database reader.
- `--include` positively selects the top-level resources that survive, with
  Atlas-style globs and `[type=...]` selectors. Repeated and comma-separated
  values union. Composition order is `--schema`, then `--include`, then
  `--exclude`. A selection that matches nothing renders no objects and keeps
  exit status 0, and reports itself on standard error; an empty value carries
  no selection and leaves inspection unfiltered.
- The OSS `--exclude` flag filters inspected resources with Atlas-style globs
  and `[type=...]` selectors, including the Atlas-documented
  `*[type=extension].version` field selector with schema-qualified globs.
- Child resources (columns, indexes, constraints, triggers, policies, grants)
  cannot be included on their own with `[type=column]`, which fails before any
  database is contacted.
- A positional spelling such as `table.column` is not refused on its shape: it
  is indistinguishable from a table literally named that. An identifier
  holding a dot is therefore selectable as `main."my.table"`, `a\.b\.c`, or
  bare `a.b.c`.
- Whether a selector matched is decided by the projection, not by the selector
  text: `path.Match` treats `.` as an ordinary character, so `table.column`,
  `table*column`, `table?column`, and `table[.]column` all reach past a
  top-level resource and select nothing. `schema apply` and `schema diff`
  refuse an empty `--include` selection; `schema inspect` keeps exit status 0
  and reports it on standard error.
- A selection that drops a dependency of a selected object is refused rather
  than rendered.
- Other field-level exclude selectors fail explicitly. Type selectors on
  non-final pattern segments fail too, except for the leading `[type=schema]`
  segment documented in the
  [Atlas comparison](../../atlas/comparison/#leading-schema-type-selector);
  exporter blocks remain an explicit gap.

The pinned Atlas CE binary rejects `schema inspect --include` with
`unknown flag: --include`; Atlas registers it. The measured
behavioral differences are tabulated in
[the Atlas comparison](../../atlas/comparison/#schema-inspect---include).

Native twin: [`ptah schema inspect`](../native-commands/).

### `ptah-compat schema apply`

Diffs a live database against the `--to` desired state, prints the planned SQL,
and applies it after interactive confirmation or explicit `--auto-approve`.

**Desired state (`--to`)** accepts one of:

- local `file://` `.hcl`, `.yaml`, `.yml`, or `.sql` schema files;
- one `file://` directory of `.sql` or `.hcl` schema files, read in filename
  order as an ordered script — the two formats together are ambiguous, other
  extensions are ignored, an empty directory is refused, a subdirectory is
  refused rather than descended into, and a file that declares an object an
  earlier file already declared is refused (`read state from "2_b.sql": table
  "users" already exists`) unless its declaration carries `IF NOT EXISTS` or
  `OR REPLACE`;
- one directly connectable database URL;
- one migration directory (a `file://` directory containing `atlas.sum`)
  replayed on the required `--dev-url` dev database;
- one `env://<attribute>` reference (`src`, `schema.src`, `url`, `dev`,
  `migration.dir`) resolved through the evaluated `atlas.hcl` env.

All `--to` values must be one source kind, database and migration-directory
sources accept one URL, and unsupported schemes such as `atlas://` fail before
the target database is contacted.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Prints the plan without applying. Mutually exclusive with `--auto-approve` on the command line. |
| `--auto-approve` | Applies without the interactive confirmation. Mutually exclusive with `--dry-run` on the command line. |
| `--tx-mode` | `file` and `all` execute the generated plan in one transaction; `none` executes statements without transaction wrapping. |
| `--format` | Atlas-style templates over planned changes with `sql`, `.MarshalSQL`, and the shared helper set including `json`. `{{ json . }}` renders `{Driver, URL, Changes{Applied\|Pending}}`. |
| `--exclude` | Filters matching resources out of both sides of the comparison before planning, as do disabled `schema.mode` values. |
| `--edit` | Opens the planned SQL in `$VISUAL`/`$EDITOR` before approval; the edited SQL is what gets applied. |
| `--file`/`-f` | Atlas's hidden alias, accepted for local HCL or SQL paths. |
| `--env` | Reads `env.url`, `env.src`, `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff` policy from `atlas.hcl`. |

`--dry-run` and `--auto-approve` contradict each other — one asks for the plan
and no execution, the other for execution with no prompt — and the pair is
refused rather than silently resolved
([#1231](https://github.com/stokaro/ptah/issues/1231)):

```text
Error: if any flags in the group [dry-run auto-approve] are set none of the others can be; [auto-approve dry-run] were all set
```

The rule reads the command line, not the environment. `PTAH_DRY_RUN` is not a
typed `--dry-run` for this purpose, so a wrapper that exports it does not turn
every `--auto-approve` in the pipeline into a refusal: the run behaves the way
`--dry-run` alone does, printing the plan and applying nothing.

```bash
PTAH_DRY_RUN=1 ptah-compat schema apply -u "$DATABASE_URL" \
  --to file://schema.sql --dev-url "$DEV_URL" --auto-approve
# Planned schema changes: … (exit 0, nothing executed)
```

The variable does not work in the other direction either. Typing both flags is
still typing both, so adding `--dry-run` to the command line above is refused
with the same sentence whether or not the variable is exported.

`--env` evaluation includes local variable defaults, locals, `getenv`, `file`,
`fileset`, `format`, `jsondecode`, `jsonencode`, `tolist`, and the supported
`hcl_schema`, `sql`, `external`, `runtimevar`, and `template_dir` project data
sources. Data sources execute only when the selected config depends on them;
dependency order is resolved before command settings are read.

**`--schema`/`-s` and `--include`** scope both sides of the comparison.
`--schema` restricts them to the named schema scopes; `--include` positively
selects top-level resources with Atlas-style glob selectors and `[type=...]`
filters. Repeated values union deterministically, `--exclude` plus disabled
`schema.mode` values subtract afterward, cross-scope dependencies refuse the
plan with explicit diagnostics, and an explicit include selection matching
nothing refuses the apply.

For a live PostgreSQL desired schema, a selected extension outside the default
schema retains its installation schema. A create plans `CREATE SCHEMA IF NOT
EXISTS` followed by `CREATE EXTENSION ... WITH SCHEMA ...`, identical live
placements compare as synced, and drops remain supported. A placement change
is detected but fails before SQL output because Ptah does not yet plan `ALTER
EXTENSION ... SET SCHEMA`.

**`--plan file://<path>`** executes a pre-approved local plan file instead of
re-planning. Both plan formats are accepted, detected by content: the Atlas
`.plan.hcl` shape and Ptah's native format_version-1 `.plan.json`.

- A JSON plan is verified against its recorded source fingerprint — a drifted
  target refuses with a stale-plan error — and may run without `--to`.
- An Atlas-format plan requires `--to`, as Atlas does: its
  hashes are Atlas-computed with no local recipe, so the plan is replayed on
  a dev database from the target's current schema, and the reached state must
  equal the `--to` desired state before the target is touched. SQLite targets
  get a throwaway dev database automatically; every other dialect requires
  `--dev-url`.
- Before replaying, statements matching a deny-list of known escape
  constructs are refused by name before anything executes. The lint covers
  SQLite (`ATTACH`/`DETACH`, `VACUUM INTO`, storage-directory pragmas,
  `load_extension`), PostgreSQL (`DO` blocks, routine bodies and dynamic SQL
  calling file-access or `dblink` functions, `COPY ... PROGRAM` or `COPY` with
  a file path, `postgres_fdw`, `file_fdw`), MySQL/MariaDB
  (`LOAD DATA INFILE`, `INTO OUTFILE`/`DUMPFILE`, `LOAD_FILE`,
  `ENGINE=FEDERATED`, `CREATE SERVER`, `INSTALL PLUGIN`/`COMPONENT`,
  `DATA`/`INDEX DIRECTORY`), SQL Server (`xp_cmdshell`, `xp_dirtree`,
  `OPENROWSET`, `OPENDATASOURCE`, `BULK INSERT`, `sp_addlinkedserver`), and
  ClickHouse (`URL`, `File`, `S3`, `HDFS`, `MySQL`, `PostgreSQL` table
  engines).
- **The lint is best-effort, not exhaustive, and it is not a sandbox.** String
  concatenation alone defeats any scanner, so a `--dev-url` must point at a
  database you are willing to have a foreign plan file execute arbitrary SQL
  against.
- **Real enforcement exists only on SQLite dev databases** — the ephemeral one
  Ptah creates for SQLite targets, and an operator-supplied SQLite
  `--dev-url`, since the restriction keys on the dev dialect. Their sessions
  refuse `ATTACH`, `DETACH`, and `VACUUM INTO` at the engine level and cannot
  load extensions; Ptah verifies the restriction is in force before rehearsing
  and refuses to rehearse if it is not. Storage-directory pragmas and
  `writable_schema` are not covered, so the converges-to-`--to` verdict is a
  good-faith check rather than an adversarial one. See
  [Save and execute plan files](../../atlas/schema-commands/#where-enforcement-is-real).
- The replay also runs under `--dry-run`, so a plan can be verified without
  committing to apply it.
- Whenever a desired state is available, the end state is verified again on
  the target after the apply and a mismatch fails loudly; the verification is
  always on, like Atlas's.
- Registry `atlas://` plan URLs are rejected. `--plan` cannot be combined
  with `--file`, `--exclude`, `--schema`, `--include`, or `--edit`, and
  `--dev-url` combines with `--plan` only together with `--to`.

**`--lock-timeout`** bounds waiting for the session advisory lock that
serializes concurrent schema applies against one target. The lock is acquired
before target inspection and planning, held through simulation, confirmation,
and execution, and released on every exit path. Empty waits indefinitely, an
elapsed timeout fails before the target is inspected, and dialects without
advisory locks (SQLite, ClickHouse, CockroachDB, Spanner) proceed unlocked with
a stderr note. PostgreSQL, YugabyteDB, MySQL, MariaDB, and SQL Server take a
real lock.

**`--lock-name`** replaces the lock name for the run (`ptah_schema_apply` by
default). Runs serialize only against other runs naming the same lock, which is
both how a run coordinates with a different tool and how it opts out of the
default. An empty value is refused; on a dialect without advisory locks the
stderr note names the lock that was not acquired.

**`--skip-lock`** takes no lock at all: a lock another process holds is ignored
rather than waited on, so concurrent applies can interleave. It cannot be
combined with `--lock-name`.

**`--dev-url` rehearsal.** `--dev-url` is required whenever `--to` is not
already a live database, failing with Atlas's `--dev-url cannot be empty`
message otherwise; a database `--to` needs none, and
`PTAH_ATLAS_APPLY_WITHOUT_DEV_URL=1` restores planning without one. Before the
apply, `--dev-url` rehearses the exact ordered plan on the dev database — reset,
the target's current schema recreated, then the planned (or edited) statements
executed under the same transaction mode. A failed rehearsal refuses the apply
with the target unchanged; the dev database must not be the target and must
share its schema scope. The rehearsal runs under `--dry-run` too, so a dry run
cannot report a plan the real apply would refuse.

Native twin: [`ptah schema apply`](../native-commands/).

### `ptah-compat schema plan`

Computes the declarative migration from the `--from` target database to local
`--to` schema files and saves it as a local plan file. The default format is
the Atlas `.plan.hcl` shape — one `plan` block with `from`/`to` fingerprints
and the migration SQL — so the saved file is readable by Atlas's plan reader;
an `--output` path ending in `.json` writes the native fingerprinted JSON
plan (format version 1) instead. Without `--save`/`--output`/`--dry-run`, the
plan document prints to stdout.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--save` | Atomically writes `<name>.plan.hcl`, using an Atlas-style UTC timestamp default name or `--name`. Refuses to replace any existing entry at the default path, including a concurrent writer or symlink. |
| `--output <path>`/`-o` | Chooses the location and atomically replaces that entry; a `.json` path selects the native JSON plan format. The plan name recorded inside a JSON plan stays fingerprint-derived unless `--name` is given. |
| `--dry-run` | Prints the plan document without saving. |
| `--auto-approve` | Accepted for Atlas CLI compatibility; a locally saved plan file is approved by operator review, so there is no prompt to skip. |
| `--edit` | Opens the planned SQL in `$VISUAL`, then `$EDITOR`, and saves the plan rebuilt from valid UTF-8 text. Comments round-trip, and dialect-aware statement severity and the destructive marker are re-derived from what you wrote. An edit leaving no statement is refused, and nothing is written. |
| `--name-format <template>` | Templates the name over `.FromHash` and `.ToHash`; hashes use Atlas's measured untagged standard-Base64 representation, and the Atlas template helpers (`json`, `upper`, `add`, `indent_ln`, …) are available. Cannot be combined with `--name`. A rendered `/` or `\` requires explicit `--output`. |
| `--skip-lint` | Accepted as an explicit no-op: `schema plan` runs no lint step, so there is nothing to skip. |
| `--env` | Reads `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`, and supported `diff` policy from `atlas.hcl`. |

The JSON plan records the ordered SQL statements with per-statement safety
severity, the dialect, the exclude patterns, and SHA-256 fingerprints of the
source and desired schema states. The `.plan.hcl` shape carries only the
name, the fingerprints, and the migration SQL; Ptah writes its own sha256
fingerprints there (Atlas parses the file but verifies its own
base64 hashes, which have no local recipe), re-derives statement severity at
read time, and refuses to save a plan computed with `--exclude` as
`.plan.hcl` because the shape cannot record the patterns.

The `.FromHash` and `.ToHash` field names and their untagged standard-Base64
representation were verified against Atlas's own reference; their values still
differ because Ptah fingerprints its independent
representation.

Editing changes the statements, never the fingerprints. `from` still describes
the live source database, so apply-time staleness detection keeps working. `to`
still describes the schema the plan was computed against, which edited SQL may
no longer reach: `schema apply` replays an Atlas-format plan on a dev database
and requires it to converge on `--to` before touching the target, but a native
`.json` plan carries no such replay, so an edited JSON plan is only as good as
its review.

Everything that can refuse the plan without reading its statements — the
`--exclude`/`.plan.hcl` incompatibility, and every `--name-format` failure —
happens before the editor opens, so an edit is never thrown away over a problem
that was decidable beforehand.

A default plan name becomes a file name, so `--name` and `--name-format`
refuse path separators unless `--output` supplies the location separately.
Both flags always refuse control characters, `.`/`..`, and the characters
Windows forbids in a file name (`:*?"<>|`). Atlas's documented
`plan_{{ slice .ToHash 0 8 }}` example works against Ptah's untagged
standard-Base64 hash value; because standard Base64 can contain `/`, use an
explicit `--output` when the rendered name must never depend on file-system
path rules.

**Not implemented**

- Registry-bound `--push`, `--pending`, and `--repo` are recorded waivers
  that fail loudly.
- `--format` fails explicitly. Atlas's plan report payload was never executed
  in Atlas, so its field names are unknown; an invented shape
  would silently break Pro templates that reference the real ones.
- `--directive` fails explicitly. The measured Atlas `.plan.hcl` carries only
  `from`, `to`, and `migration`, so a directive would have to ride inside the
  migration heredoc in an unmeasured spelling — and Ptah's own reader ignores
  `-- atlas:checkpoint` today, so emitted directives would be silent no-ops.
- `--schema`, `--include`, and `--lock-timeout` fail explicitly until
  implemented.
- The registry sub-verbs (`approve`, `list`, `pull`, `push`, `rm`) stay
  unsupported-boundary stubs: they arbitrate plan state in a remote registry.
- `lint` and `test` also stay stubs — see below for why.

Atlas keeps `schema plan` in its Pro registry flow, so this is a free Ptah
capability rather than an Atlas CE stub.
Native twin: [`ptah schema plan`](../native-commands/).

#### `ptah-compat schema plan new`

Creates a plan file for the schema transition. It is `schema plan` restricted
to the flag set Atlas registers on this sub-verb, with saving always on:
Atlas gives `new` neither `--save` nor `--dry-run`, and its documented purpose
is to create the plan file.

The plan is written to `--output`/`-o` when given, and to `<name>.plan.hcl` in
the working directory otherwise; an existing default-named plan file is never
overwritten. An `--output` path ending in `.json` writes the native JSON plan
format. `--edit`, `--name` and `--name-format` behave exactly as they do on
`schema plan`, and the same refusals apply to `--repo`, `--format`,
`--lock-timeout`, `--schema` and `--include`.

`--save`, `--dry-run`, `--push`, `--pending`, `--skip-lint` and `--directive`
are **not registered** here, because Atlas does not register them here.

#### `ptah-compat schema plan validate`

Checks that the plan file named by `-f`/`--file` describes the transition from
the `--from` target database to the local `--to` schema files, without changing
the target database. On success it writes nothing to stdout and exits 0.

Two checks run, and they are the two `schema apply --plan` runs before it
touches anything:

1. the plan's recorded from-fingerprint must match the live `--from` database,
   for plans carrying Ptah's own `sha256:` fingerprints. An Atlas-written plan
   file carries Atlas hashes with no local recipe, so this check is skipped for
   those and the replay below is the only from-state gate.
2. the plan's statements are replayed on a dev database seeded from the
   target's current schema, and the state they reach must equal `--to`.

The replay always runs, in both plan formats — unlike `schema apply --plan`,
which may skip it for a fingerprint-verified native plan. A matching
from-fingerprint says the plan was computed against this database, not that its
statements reach `--to`, and the second question is the one this command exists
to answer. A SQLite target gets a throwaway dev database; every other dialect
requires `--dev-url`.

An explicit `--dev-url` is refused when it identifies the `--from` target,
even through an equivalent or percent-encoded SQLite path, a symlink or hard
link, or a network URL with different credentials, default-port spelling,
loopback alias, or driver-level endpoint/database override. Network URLs with
the same dialect and selected database name fail closed even across different
endpoints because DNS aliases and replicas cannot be proven independent.
Malformed target and desired-state URLs also fail closed. These shared checks
protect both `schema apply` simulation and `schema plan validate`, and run
before the dev database can be reset.

The plan's SQL is **not** required to equal a freshly computed plan's. Atlas
documents editing a saved plan's `migration` attribute, so what is checked is
where the statements arrive, not how they are spelled.

`--to` is required; without it the command reports Atlas's own wording,
`the flag "to" is required to verify the provided plan`. `--exclude` is
refused: a JSON plan records the patterns it was computed with and the Atlas
`.plan.hcl` shape records none at all, so flag-supplied patterns would verify a
different transition than the plan describes. Registry plan URLs
(`atlas://…`) are refused like they are on `schema apply --plan`.

#### Evidence for the two local sub-verbs

Their **flag sets** match a sanitized standard Atlas v1.3.0 help bundle
captured on 2026-08-02 with the exact binary and artifact SHA-256 values pinned
in testdata, and the published
[Atlas CLI reference](https://atlasgo.io/cli-reference). Their
**behavior** is not established by help, and Atlas Community Edition (CE)
settles nothing because it aborts the entire `schema plan` path. Ptah records
that limitation in tests and documentation instead of printing development
provenance during normal commands; successful `new` and `validate` runs keep
stderr empty. Runtime parity remains tracked in
[`stokaro/ptah#1037`](https://github.com/stokaro/ptah/issues/1037).

The validation tests do not rely only on plans written by Ptah. They also run
an Atlas-produced `.plan.hcl` artifact against live
SQLite, then mutate the source, desired schema, migration SQL, HCL syntax, and
statement set. Its versioned bundle includes source and desired SQL plus a
manifest of file hashes, known capture facts, and the original evidence that
was not preserved. Every invalid semantic variant is refused without changing
the target. Structurally valid Atlas Base64 SHA-256 `from` and `to` hashes are
treated as unauthenticated metadata because their derivation is not public;
malformed values are rejected. The replayed end state, not a foreign hash, is
the integrity boundary.

**Why `lint` and `test` are not implemented.** Both are local by their Atlas
flag sets, and both are deferred deliberately:

- `schema plan lint` has no measured output contract, no measured failure
  threshold and no measured exit code. The engine that would back it — the
  `migrate lint` analyzer set — reports on a migration *directory*, and a
  linter narrower than Atlas's analyzer set sitting in a gating position would
  report clean on a plan Atlas flags. That is a silent wrong answer in the one
  position where silence is most expensive.
- `schema plan test` consumes `test "plan"` blocks in `.test.hcl` files.
  Nothing in Ptah parses that format yet; the test engine reads YAML cases.
  `.test.hcl` ingestion is its own item, shared with `migrate test` and
  `schema test`.

### `ptah-compat schema diff`

Diffs two desired-state sources and prints migration SQL.

**Sources.** Each of `--from`/`-f` and `--to` accepts one of:

- local `file://` schema files with `.hcl`, `.yaml`, `.yml`, or `.sql`
  extensions;
- one directly connectable database URL, whose live schema is introspected;
- one migration directory (a `file://` directory containing `atlas.sum`)
  replayed on the required `--dev-url` dev database;
- one `env://<attribute>` reference resolved through the evaluated `atlas.hcl`
  env.

Unsupported schemes such as `atlas://` fail during validation. The SQL dialect
is pinned by `--dev-url` first, then by `--from` and `--to` database URLs; local
schema files alone still require `--dev-url`.

**Flags**

| Flag | Behavior |
| --- | --- |
| `--format` | Atlas-style templates with `sql` and `.MarshalSQL`. |
| `--exclude` | Filters resources out of both sides before diffing, as do disabled `schema.mode` values. |
| `--schema`/`-s`, `--include` | Positively scope both sides, with the same selection semantics as `schema apply`. |
| `--env` | Reads `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.diff`, and supported `diff` policy from `atlas.hcl`. |

Selection order matches `schema apply`: schema universe for schema-owned
resources, include selection, exclusion last, and cross-scope dependency
diagnostics. Database-wide extensions remain on both sides regardless of
installation placement. An extension-only `--include` selects their qualified
or bare identities; when a non-extension resource matches, all extensions ride
as non-removing support even beside extension selectors. Schema-only and
extension-only scopes remain authoritative for extension removal. Exclusions
still subtract afterward. A selection that matches neither side exits 1 with
no diff output rather than reporting a synced schema.

Native twin: [`ptah schema diff`](../native-commands/).

### `ptah-compat schema fmt`

Formats local `.hcl` files using HCL canonical layout. Native twin:
[`ptah schema fmt`](../native-commands/).

### `ptah-compat schema clean`

Cleans user-owned schema objects through Ptah's destructive database-cleanup
runtime.

| Flag | Behavior |
| --- | --- |
| `--dry-run` | Prints the planned cleanup. |
| `--auto-approve` | Skips the interactive confirmation, which is otherwise preserved. |
| `--include` | Full mode only. Keeps objects matched by Atlas resource selectors; dependent cleanup rows such as foreign keys and implicit sequences ride with their table. An owned sequence cannot be selected without its table. |
| `--exclude` | Full mode only. Subtracts objects matched by Atlas resource selectors. Every independently writer-owned object kind remains selectable; an owned sequence cannot be preserved while its table is selected for removal. |
| `--format` | Renders Atlas-style templates over the cleanup plan. |
| `--env` | Reads `env.url` and `format.schema.clean` from `atlas.hcl`. |

The plan reports the object kinds the target dialect's cleanup really destroys,
so a `--dry-run` or `--format` report is not narrower than the apply:

| Dialect | Reported and destroyed |
| --- | --- |
| PostgreSQL family | Foreign keys, tables, views, materialized views, enum, domain, composite and range types, and functions. PostgreSQL itself also reports standalone sequences, foreign tables, procedures, aggregates, collations, and default privileges. |
| MySQL, MariaDB | Foreign keys, tables, views, stored functions and procedures, events, and MariaDB sequences. |
| SQLite | Tables and views. |
| SQL Server | Foreign keys and tables. Views are not dropped, so they are not reported. |
| ClickHouse | Base tables, views, and materialized views. A materialized view is removed with `DROP VIEW`, which takes its inner storage table with it; live views and window views are neither read nor dropped. |

Ptah's own migration revision table is included in that accounting. On
PostgreSQL, MySQL, MariaDB and SQL Server the cleanup destroys it like any other
table, so the plan names it and the operator confirming a destructive run is
confirming the loss of the record of which migrations have been applied. On
SQLite the cleanup keeps it, so it is neither reported nor destroyed. Both
revision-table layouts are covered, Ptah's `schema_migrations` and the
Atlas-compatible `atlas_schema_revisions`, and a revision table configured under
any other name is reported as the ordinary table it is.

Objects that vanish as collateral of a listed drop are not listed separately:
indexes, triggers, non-foreign-key constraints, RLS policies, and comments. The
report order is alphabetical by object kind rather than an execution order. An
unscoped cleanup rebuilds its statements from the live catalog; a scoped
cleanup executes the reported `Cmd` values in a separate deterministic order
that removes known dependents before their dependencies. PostgreSQL uses live
catalog depth to order dependent views and materialized views of the same kind.
Every PostgreSQL-family target executes the complete scoped plan in one
transaction and retries selected dependencies after a `RESTRICT` refusal. An
external dependency rolls back earlier selected drops instead of leaving a
partially cleaned schema.

PostgreSQL `SERIAL` and identity sequences are recognized as implicit table
children, execute after their parent table, and are not reported as forbidden
standalone sequences by the strict CE oracle profile. A selector that tries to
make the owned sequence and its table disagree is refused before mutation.
Function objects and changes expose full declaration `Parameters`.

PostgreSQL-family scoped drops use `RESTRICT`, so the database refuses a
selected parent when an unselected view, foreign key, or other catalog
dependency still refers to it. The narrowed command cannot cascade beyond the
objects its plan reports.

Function `Cmd` values use PostgreSQL identity arguments, so overloaded,
defaulted, and OUT-only functions remain distinct and executable.

Native twin: [`ptah schema clean`](../native-commands/).

### `ptah-compat schema test [paths]`

Forwards to `ptah schema test`.

| Atlas flag | Native equivalent |
| --- | --- |
| `-u`/`--url` | `--root-dir`. Accepts Go annotations, a SQL or HCL file, or a live database URL. |
| `--dev-url` | The native throwaway database; an ephemeral SQLite database when omitted. |
| `--run` | The native case-name filter. |
| `--var` | Repeatable HCL schema-variable values. An explicit `--url` keeps these values; a project `data.hcl_schema` source uses its block-scoped `vars`. |
| Positional path (optional) | The directory of Ptah-native YAML test cases. |

With `--env`, `schema.src` supplies the desired schema URL and `dev` the dev
database. A source from `data.hcl_schema` keeps that source's variable scope,
including an empty scope that excludes run-wide values. Exit codes match the
native runner: 0 when all cases pass, 1 on test failure.

Atlas keeps `schema test` in its Pro build, so this is a free Ptah capability
rather than an Atlas CE stub.

### `ptah-compat schema push`

Registered but not implemented because the command targets an account-bound
hosted registry protocol. `--help` reports that the command is not implemented
and exits 0; direct execution reports the same status and exits 1. The open
replacement is the native `ptah schema push` to any OCI registry.

## Related pages

- Runnable migrate workflows and format template fields:
  [Atlas migrate commands](../../atlas/migrate-commands/).
- Runnable schema workflows and format template fields:
  [Atlas schema commands](../../atlas/schema-commands/).
- Measured compatibility evidence: [Conformance](../../atlas/conformance/).
