# Native CLI Command Tree

Ptah has a native CLI surface in the `ptah` binary, plus Atlas-compatible
command surfaces outside the native tree:

- Native Ptah commands, owned by Ptah and documented here.
- Atlas-compatible commands under `ptah atlas <command> ...`.
- The separate `ptah-compat` binary, which exposes the Atlas-compatible tree at
  process root and can be copied or symlinked as `atlas`.

Do not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect` to the native `ptah` binary.

## Canonical Native Tree

The native tree uses Ptah-owned noun/verb groups. Ptah is pre-GA, so old
root-level command spellings are removed instead of preserved.

| Native command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah oci referrers` | List direct referrer metadata attached to an OCI artifact. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, HCL, SQL, or external-command inputs. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export a schema to HCL, an OpenAPI 3.0 component schema, or a GraphQL SDL. |
| `ptah schema push` | Publish a lossless canonical desired schema to an OCI registry. |
| `ptah schema pull` | Pull a canonical desired schema from an OCI registry. |
| `ptah schema test` | Apply a desired schema to a throwaway database and run declarative seed/SQL/assert test cases against it. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah db read` | Read schema from a live database. |
| `ptah db drop-all` | Drop all schema objects in a live database. |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. |
| `ptah migrations create` | Create empty migration files for manual SQL. |
| `ptah migrations data` | Generate a migration from declarative reference/seed data drift against a live database. |
| `ptah migrations import` | Convert another tool's migration directory to Ptah format. |
| `ptah migrations push` | Publish a migration directory to an OCI registry. |
| `ptah migrations pull` | Pull and reconstruct a migration directory from an OCI registry. |
| `ptah migrations up` | Run pending migrations. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations baseline` | Record existing migrations as applied. |
| `ptah migrations checkpoint` | Squash history into a cumulative-schema checkpoint fresh databases bootstrap from. |
| `ptah migrations repair` | Repair migration revision metadata. |
| `ptah migrations hash` | Write or update migration directory integrity. |
| `ptah migrations validate` | Validate migration directory integrity and, optionally, SQL execution with `--dev-url`. |
| `ptah migrations edit` | Edit a migration's SQL (via `$EDITOR` or `--up-file`/`--down-file`) and rewrite the integrity file. |
| `ptah migrations rebase` | Move a migration to the end of history by re-timestamping it, and rewrite the integrity file. |
| `ptah migrations rm` | Delete a migration's up/down pair and rewrite the integrity file. |
| `ptah migrations lint` | Lint migration files. |
| `ptah migrations test` | Run migrate/apply-schema/seed/SQL/assert steps against a throwaway database. |
| `ptah sql lint` | Lint standalone SQL files. |
| `ptah seed` | Apply environment-scoped SQL seed files. |
| `ptah version` | Print Ptah build information. |

The native `ptah migrations up`, `status`, and `down` commands accept an
`oci://` reference through `--migrations-dir`, including movable tags and
immutable digest pins. `up --verify-sum` verifies `ptah.sum` or `atlas.sum`
inside the pulled artifact before connecting to the database. `ptah migrations
lint` accepts an OCI `--dir` and can attach its canonical report to the exact
migration digest with `--attach`.

The native `ptah schema compare`, `drift`, and `ptah migrations plan` commands
accept an OCI desired-schema artifact through `--schema-file`. A plan built
from exactly one OCI schema source can attach its canonical safety report with
`--attach`. Pass `--plain-http` only for an explicitly trusted local registry.
See [OCI Registry Artifacts](./oci_registry.md).

`ptah oci referrers <oci-reference>` lists direct attachment descriptors. Its
`--type` filter accepts `all`, `lint`, `plan`, or `deployment`, and `--format`
accepts `text` or `json`. Unqualified subjects resolve to `:latest`, tags
resolve to their current manifest, and digest subjects remain immutable. The
command uses Docker credentials and HTTPS by default; `--plain-http` is only
for an explicitly trusted local registry. It lists metadata and does not pull
or consume attachment payloads.

These commands do not add an Atlas Cloud implementation to `ptah atlas`.
`ptah atlas migrate push` and `ptah atlas schema push` stay community-edition
boundary stubs by decision (see "Atlas Compatibility Waivers"), and
Atlas-compatible apply commands do not gain native OCI transport flags.

The native desired-schema consumers — `schema render`, `schema compare`,
`schema drift`, `migrations plan`, and `migrations generate` — accept Go roots,
YAML/HCL/SQL schema files, and an external command whose stdout is SQL, HCL, or
YAML. They also read the same `ptah.yaml external_schema` block when
`--schema-cmd` is not set, but execute a config-sourced program only with
`--allow-external-schema`. See
[Schema sources](site/src/content/docs/workflows/schema-files.md).

The schema-diff commands (`ptah schema render`, `ptah migrations generate`,
`ptah migrate`, `ptah compare`) emit `CREATE`/`ALTER`/`DROP SEQUENCE` for
standalone PostgreSQL sequences declared with `//migrator:schema:sequence`. See
[Sequences](./sequences.md).

The same commands emit `CREATE DOMAIN` / `CREATE TYPE … AS (…)` / `CREATE TYPE …
AS RANGE (…)` (and their drops) for PostgreSQL user-defined types declared with
`//migrator:schema:domain` / `:composite` / `:range`, and `read-db` introspects
them. See [User-defined types](./user_defined_types.md).

`ptah migrations up` evaluates any `-- +ptah check` pre-migration assertions in a
migration before applying its statements, aborting (non-zero) if a precondition
does not hold; `--skip-checks` is an emergency bypass. See
[Pre-migration checks](./pre-migration-checks.md).

`ptah migrations down --shadow-db <url>` replays the rollback plan on a
disposable shadow database before touching the target: the shadow database is
dropped clean, migrated up to the target's current version, and migrated down
to the requested target. A failing or missing down migration aborts with the
target untouched and its revision state clean. The shadow database must match
the target dialect. The Atlas-compatible `ptah atlas migrate down --dev-url`
maps to this verification, and `ptah atlas migrate down --format` (flag or
`PTAH_FORMAT`) renders an Atlas Go-template report (`.Env`, `.Planned`,
`.Reverted`, `.Current`, `.Target`, `.Total`, `.Start`, `.End`, `.Error`) over
the same rollback engine, with the YES confirmation prompt on stderr so the
report stays alone on stdout. Like `ptah atlas migrate set`, the down forward
defaults to Atlas revision bookkeeping (`--revision-format atlas`), so a bare
`atlas migrate down` reverts the revisions `atlas migrate apply` wrote instead
of silently no-opping against an empty ptah revision table; passing the native
`--revision-format ptah` through the forward still selects ptah bookkeeping.

`ptah migrations import` converts an existing migration directory from another
versioned-migration tool into Ptah's native format, preserving version order and
rewriting `ptah.sum`, so a team can adopt Ptah without hand-rewriting its
history. See [Importing migrations](./migrations-import.md).

`ptah migrations test` and `ptah schema test` use the same Ptah-native YAML
case format and support text, JSON, and HTML reports. Migration cases can apply
the desired schema with `apply_schema: true` and `--root-dir`; schema cases
receive the desired schema before their steps run. The Atlas-compatible
`ptah atlas migrate test` and `ptah atlas schema test` verbs forward to these
commands with Atlas-shaped flags. See
[Declarative database testing](./testing.md).

`ptah migrations edit`, `rebase`, and `rm` safely maintain an existing migration
directory: `edit` changes a migration's SQL, `rebase` re-timestamps a migration
to the end of history so it applies after concurrently-merged work, and `rm`
deletes a migration. Each rewrites the integrity file (`ptah.sum` or `atlas.sum`)
atomically, so `ptah migrations validate` passes immediately afterward — no
separate `hash` step. All three take `--migrations-dir`, `--version`, and
`--dir-format`. To protect deployed history, they refuse to modify a migration
that is already applied in the database given by `--db-url` unless `--force` is
passed; without `--db-url` they warn that applied state could not be verified.
These are directory-maintenance commands Atlas keeps in its proprietary (Pro)
build; Ptah provides them natively and for free. The Atlas-compatible
`ptah atlas migrate edit`, `rebase`, and `rm` verbs forward to these commands
with Atlas-shaped `--dir`/`--dir-format` flags and a `{name | version}`
positional; the editor for `edit` resolves from `$VISUAL`, then `$EDITOR`.
`ptah migrations create --edit` opens the just-created migration files in the
same editor and refreshes `atlas.sum` for Atlas-format directories; the
Atlas-compatible `migrate new --edit`, `migrate diff --edit`, and
`schema apply --edit` flags use the same editor path.

`ptah migrations lint` and `ptah sql lint` report findings by rule code, grouped
into families (`DS` data-safety, `PG`/`MY` dialect-specific, `TX` transaction,
and others). Alongside `DS`, the `CD` (constraint-deletion) family flags dropping
a constraint whose type is recoverable from the SQL — `CD101` foreign key,
`CD102` check, `CD103` primary key (all `SeverityError`). `DS105` remains the
fallback for the ANSI `DROP CONSTRAINT <name>` form, whose constraint type the
SQL does not encode, so a typed drop yields exactly one finding (its `CD` code,
never also `DS105`). Individual codes and whole families are disabled by code or
prefix (for example `CD`, or a single `CD101`) and re-severitied per code through
lint configuration, the same as every other family.

### Atlas Pro Analyzer Coverage

Atlas gates whole analyzer families behind Atlas Pro. Ptah's native lint covers
most of them locally and for free. The table below audits every Pro-marked
analyzer check code from the Atlas analyzers documentation
(<https://atlasgo.io/lint/analyzers>, fetched 2026-07-28) against Ptah's native
rules. Codes the Atlas docs mark as non-Pro (`DS`, `MF`, `BC`, `PG110`,
`MY101`–`MY123`, `LT`, `NM`, `SA`) are outside this audit; note that Ptah's
native code namespace intentionally differs from Atlas's where meanings differ
(for example Ptah `PG102` is enum-value-in-transaction, not Atlas's
drop-index-concurrently, which Ptah codes as `PG106`).

| Atlas Pro code | Atlas meaning | Ptah native rule(s) | Status |
| --- | --- | --- | --- |
| CD101 | Foreign-key constraint dropped | `CD101` | Covered |
| CD102 | Check constraint dropped | `CD102` | Covered |
| CD103 | Primary-key constraint dropped | `CD103` | Covered |
| PG101 | Index created without `CONCURRENTLY` | `PG101` | Covered |
| PG102 | Index dropped without `CONCURRENTLY` | `PG106` | Covered |
| PG103 | Missing `atlas:txmode none` header for concurrent operation | `PG103` | Covered — Ptah flags `CONCURRENTLY` inside a transactional migration and honors both the `atlas:txmode none` header and its native no-transaction directive |
| PG104 | `PRIMARY KEY` creation acquires `ACCESS EXCLUSIVE` lock | `PG104` | Covered |
| PG105 | `UNIQUE` constraint creation acquires `ACCESS EXCLUSIVE` lock | `PG105` | Covered |
| PG301 | Column type change requires table and index rewrite | `DS103` | Partial — the type change is flagged as a data-safety risk, without PostgreSQL rewrite/lock analysis |
| PG302 | Volatile `DEFAULT` on added column rewrites the table | `PG302` | Covered |
| PG303 | `SET NOT NULL` scans existing rows | `PG303` | Covered |
| PG304 | `PRIMARY KEY` on nullable columns requires full scan | `PG104` | Partial — every `ADD PRIMARY KEY` is flagged; the nullable-column refinement needs schema knowledge statement-scoped lint does not have |
| PG305 | `CHECK` constraint requires full table scan | `PG305` | Covered |
| PG306 | `FOREIGN KEY` requires full scan and blocks writes | `PG306` | Covered |
| PG307 | Logging mode change rewrites the table | `PG307` | Covered |
| PG308 | Trigger creation acquires `SHARE ROW EXCLUSIVE` lock | `PG308` | Covered |
| PG309 | `STORED` generated column rewrites the table | `PG309` | Covered |
| PG310 | Identity column rewrites the table | `PG310` | Covered |
| PG311 | Access method change rewrites the table | `PG311` | Covered |
| MY130 | Column type change requires table copy | `MY101`, `DS103` | Partial — `MODIFY`/`CHANGE` is flagged as lock-heavy DDL and the type change as a data-safety risk, without a dedicated table-copy code |
| MY131 | Foreign key added blocks DML | `MY131` | Covered |
| MY132 | Primary key added requires table rebuild | `MY132` | Covered |
| MY133 | Primary key dropped without replacement requires table copy | `CD103` | Partial — the drop is flagged as an error-severity constraint deletion; the table-copy concern has no dedicated code |
| MY134 | `FULLTEXT` index added blocks DML | `MY134` | Covered |
| MY135 | `SPATIAL` index added blocks DML | `MY135` | Covered |
| MY136 | Character set change requires table rebuild | `MY101` | Partial — `CONVERT TO CHARACTER SET`/`CHARSET` is flagged as lock-heavy DDL; other charset-change spellings are not scanned |
| TX101 | Statements cannot run in a single transaction | `TX101` | Covered |
| TX201 | Nested transaction detected | `TX201` | Covered |
| OW101 | User not authorized to modify resource | — | Waived — ownership policy binds to Atlas Pro schema-ownership annotations and an account/identity model; Ptah reviews destructive changes through its `DS`/`CD` safety gates instead |
| OW102 | User explicitly denied access to resource | — | Waived — same rationale as OW101 |

Under `ptah atlas migrate lint`, native findings report under the `ptah`
analyzer with their native codes, except the proven Atlas identities mapped by
`internal/atlaslint`: native `DS101` reports as Atlas `destructive`/`DS102`,
native `DS102` as `destructive`/`DS103`, and native `DD101` as
`data_depend`/`MF103`. Atlas `-- atlas:nolint` analyzer selectors map onto the
native families (`destructive` → `DS`+`CD`, `concurrent_index` →
`PG101`/`PG103`, `data_depend` → `DD`, `incompatible` → `BC`, `nestedtx` →
`TX201`).

## Local Declarative Plan Files

Atlas gates `schema plan` behind the Atlas Pro registry approval flow. Ptah
implements the open local replacement on the Atlas-compatible tree, with no
registry, account, or cloud round-trip:

- `ptah atlas schema plan --from <db-url> --to file://<schema> --save` computes
  the declarative migration from the `--from` target database to the local
  `--to` schema files and saves it as a local plan file (`--output <path>`
  chooses the location; the default is `<name>.plan.json` with a deterministic
  fingerprint-derived name, or pass `--name`). `--dry-run` prints the plan
  document without saving. With `--env`, the selected `atlas.hcl` env supplies
  `url` (the plan target), `schema.src`, `dev`, `exclude`, `schema.mode`, and
  supported diff policy values, mirroring `schema apply`.
- `ptah atlas schema apply --url <db-url> --plan file://<path>` executes the
  saved plan instead of re-planning. Before executing, the plan's source
  fingerprint is verified against the live database: if the schema changed
  since the plan was computed, apply refuses with a stale-plan error — the
  point of a pre-approved plan is that exactly the reviewed SQL runs against
  exactly the reviewed state. `--dry-run` prints the plan SQL without
  executing, and `--auto-approve`/the interactive `YES` confirmation work as
  in the normal apply path. Registry plan URLs (`atlas://...`) are rejected.

The plan file is JSON (`format_version` 1); Ptah does not parse or emit
Atlas's `.plan.hcl`. Fields:

| Field | Meaning |
| --- | --- |
| `format_version` | Plan-file contract version; this build writes and accepts `1`. |
| `name` | Plan name (`--name`, or `plan_<hash>` derived from the fingerprints). |
| `dialect` | Target dialect the plan was computed for; apply requires a matching target. |
| `from_fingerprint` | SHA-256 digest (`sha256:<hex>`) of the canonical JSON encoding of the exclude-filtered introspected source schema. Verified by `schema apply --plan` before executing. |
| `to_fingerprint` | Same digest mechanism over the loaded desired schema model. Informational: it lets tooling detect that a plan no longer corresponds to the desired sources. |
| `exclude` | Exclude patterns the plan was computed with; re-applied when the fingerprint is verified. |
| `destructive` | True when any statement is classified destructive. |
| `statements` | Ordered statements, each with `sql`, `severity` (`safe`/`warning`/`destructive`), and `reason` from Ptah's safety classifier. |

Unknown fields, other format versions, and empty plans are rejected when the
file is read. The registry sub-verbs (`schema plan approve/lint/list/new/pull/
push/rm/test/validate`) remain Atlas CE boundary stubs: they operate on plans
stored in the Atlas Registry, which the local plan file replaces.

## Atlas Compatibility Waivers

Some Atlas Pro commands and flags are accepted for surface parity but rejected
loudly with a recorded rationale, because their behavior is bound to Atlas
Cloud services Ptah intentionally has no counterpart for. These are waivers,
not pending work:

- `ptah atlas migrate push` / `ptah atlas schema push`: kept as byte-for-byte
  Atlas CE boundary stubs by decision. Both verbs push to the Atlas Registry, a
  proprietary, account-bound cloud service whose protocol is not an open
  target. The open replacement is the native `ptah migrations push` /
  `ptah schema push` pair, which publishes to any OCI registry you already
  operate (bring-your-own-registry, no account, digest pinning, referrer
  attachments). Accepting `oci://` destinations under the atlas verbs was
  considered and rejected: an Atlas-shaped verb that silently speaks a
  different protocol to a different registry would not be a drop-in, so the
  boundary stays loud and identical to Atlas CE.
- `ptah atlas migrate down --to-tag`: migration tags exist only in Atlas
  Registry (Atlas Cloud); use `--to-version` with a migration version instead.
- `ptah atlas migrate down --skip-checks`: Atlas down checks are part of the
  Atlas Cloud plan-approval flow; Ptah reverts through locally reviewed down
  migrations and has no generated checks to skip.
- `ptah atlas migrate down --plan`: dynamic down planning is bound to the Atlas
  Cloud plan-approval flow; use `--dev-url` to verify the pre-planned rollback
  on a dev database instead. Ptah's local plan files (see "Local Declarative
  Plan Files") are declarative apply plans, not down plans.
- `ptah atlas schema plan --push/--pending/--repo/--auto-approve`: plan push,
  pending state, schema repositories, and plan approval are Atlas Registry
  (Atlas Cloud) concepts. Ptah's local plan workflow saves plan files with
  `--save`/`--output`, and a locally saved plan file is approved by operator
  review, so there is no approval prompt to skip.
- `ptah atlas migrate checkpoint --dir-format=atlas`: Ptah's checkpoint engine
  marks checkpoints with the ptah two-file convention
  (`NNNNNNNNNN_name.checkpoint.up.sql`/`.down.sql` plus `ptah.sum`); Atlas
  marks them with an `-- atlas:checkpoint` file directive that Ptah's
  Atlas-format reader does not parse, so an Atlas-format checkpoint file would
  replay as an ordinary migration. Checkpoint output stays ptah-format only
  (`--dir-format=ptah` passes through; every other value is rejected).

## Exit Codes

Canonical grouped commands inherit the exit-code contract of the implementation
they delegate to. See [CLI Exit Codes](exit_codes.md) for the command-by-command
matrix.
