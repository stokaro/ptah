# Public Go API

Ptah is pre-GA, but embedders need a documented surface and a typed error
contract. Packages in this document are the only non-command, non-example,
non-fixture packages that may remain importable without an explicit review.
For task-oriented guidance and examples, see the
[Reusable components](site/src/content/docs/extend/components.md)
guide.

## Stable Embedder API

These packages are intended for application and tool embedders:

- `github.com/stokaro/ptah/atlascompat`
- `github.com/stokaro/ptah/config`
- `github.com/stokaro/ptah/config/projectconfig`
- `github.com/stokaro/ptah/core/ast`
- `github.com/stokaro/ptah/core/goschema`
- `github.com/stokaro/ptah/core/platform`
- `github.com/stokaro/ptah/core/platform/capability`
- `github.com/stokaro/ptah/core/platform/identifier`
- `github.com/stokaro/ptah/core/ptaherr`
- `github.com/stokaro/ptah/core/query`
- `github.com/stokaro/ptah/core/renderer`
- `github.com/stokaro/ptah/core/schemasource`
- `github.com/stokaro/ptah/core/sqlutil`
- `github.com/stokaro/ptah/dbschema`
- `github.com/stokaro/ptah/dbschema/types`
- `github.com/stokaro/ptah/migration/datadiff`
- `github.com/stokaro/ptah/migration/dbtest`
- `github.com/stokaro/ptah/migration/diffpolicy`
- `github.com/stokaro/ptah/migration/generator`
- `github.com/stokaro/ptah/migration/importer`
- `github.com/stokaro/ptah/migration/lint`
- `github.com/stokaro/ptah/migration/migrator`
- `github.com/stokaro/ptah/migration/planner`
- `github.com/stokaro/ptah/migration/risk`
- `github.com/stokaro/ptah/migration/safety`
- `github.com/stokaro/ptah/migration/schemadiff`
- `github.com/stokaro/ptah/migration/schemadiff/types`
- `github.com/stokaro/ptah/migration/seeder`

`atlascompat` is a narrow compatibility surface for external Atlas parity and
conformance tooling. It intentionally wraps parser, HCL schema,
conversion, and migration sum internals without making those implementation
packages importable directly.

`config/projectconfig` is the canonical typed project configuration IR. Its
online-DDL policy is parsed, merged, validated, and then passed to migration
execution without a second configuration-file read.

`migration/lint` provides the compact `LintFS` findings API and the richer
`AnalyzeFS` API. `AnalyzeFS` captures each migration input once: SQL files,
integrity metadata, and `.ptah-lint.yaml`. It returns deep-copy views of
prepared files and findings together with a read-only source snapshot. Finding
contexts identify the exact statement and affected tables or columns; column
subjects can also carry the parent table and declared data type. Each prepared
up-migration file also carries the semantic schema changes it expresses
(`File.Changes`, typed `SchemaChange`), recovered from Ptah's dialect-aware SQL
parser so one statement can map to zero, one, or several changes. Atlas-ignored
files are marked explicitly without changing version selection.
Compatibility-specific directive behavior must be selected explicitly; native
Ptah behavior is the zero-value default.

`migration/migrator` exposes `WithStatementObserver` for tools that need to
audit successful filesystem-migration execution without replacing the
interceptor, splitter, directive, or transaction path. Observers receive
structured source and statement metadata after execution but no connection
handle, so they cannot alter the migrator execution path.

`migration/dbtest` exposes the declarative testing engine used by
`ptah migrations test` and `ptah schema test`. Embedders can construct
`Case`/`Step`/`Assertion` values in Go or load YAML, select cases with
`FilterCases`, run against an ephemeral or explicit throwaway database, and
render text, JSON, or HTML reports. See [Declarative database
testing](testing.md).

`core/schemasource` executes an explicitly configured program without a shell,
bounds its runtime and captured output, cleans up descendant processes, and
parses SQL, HCL, or YAML stdout into Ptah's schema IR. Empty output is rejected
to prevent an accidentally broken provider from becoming an empty desired
schema, and displayed stderr/parser diagnostics are bounded, secret-redacted,
and terminal-safe. Embedders can use the same external desired-schema contract
as the CLI without depending on Cobra or any `cmd/internal` package.

`migration/schemadiff/types.SchemaDiff` stores index additions and removals as
canonical `[]IndexRef` fields. Every index reference includes its owning table.
Live comparisons also snapshot catalog identifier semantics into the diff so
comparison, destructive-change policy, forward planning, and reverse planning
use one source of truth. `core/platform/identifier` exposes the reusable value
types and conservative dialect defaults behind that contract.
`migration/generator.GenerateCheckpointWithDatabaseInfo` preserves the same
live semantics when an introspected schema is rendered as a checkpoint, but
SQL Server callers should use `GenerateCheckpointWithDatabase` so Ptah resolves
the complete candidate identifier set under the target catalog collation.
`GenerateCheckpoint` remains the conservative dialect-only entry point.
`migration/planner.Planner` exposes only checked planning; malformed references,
unresolved additions, and target index-namespace conflicts fail before SQL is
returned.

Public failures from these packages should use `core/ptaherr` where the caller
can reasonably branch on the error. In particular, annotation failures should
support `errors.As(err, *ptaherr.ParseError)`, and unsupported dialect failures
should support `errors.Is(err, ptaherr.ErrUnsupportedDialect)`. Invalid schema
diffs rejected during planning support
`errors.Is(err, ptaherr.ErrInvalidSchemaDiff)`.

## Provisional Surface

There is no provisional public surface. Packages that are not listed under
Stable Embedder API are either command/example/fixture/test packages or are
behind Go `internal/` boundaries. Promoting another package to public API must
be an explicit design decision that updates this document and the snapshot in
the same reviewed change.

## Compatibility Guard

CI runs three public API checks:

- `scripts/check-public-api.sh` fails when `go list ./...` finds a
  non-command, non-example, non-fixture package that is importable from outside
  this module but not listed here.
- `scripts/check-public-api-snapshot.sh` regenerates the `go doc -short`
  exported-symbol snapshot for every package listed here, then appends the full
  `go doc` output for every exported named type (struct, interface, alias, map,
  func type), and compares the result with `docs/public_api.snapshot`. Because
  the full per-type output is recorded, changes to exported struct fields and to
  methods on concrete named types are caught, not only interface method sets.
  Any exported surface change must update the snapshot in the same reviewed
  change. The guard is itself covered by a self-test in
  `internal/apiguard` that fails if this per-type coverage regresses.
- `scripts/check-public-api-released.sh` compares each stable package against
  the latest `v0.x` release tag with `apidiff -incompatible`. Until the first
  `v0.x` tag exists, the script reports that no released baseline is available
  and exits successfully. Once a `v0.x` tag exists, CI checks out repository
  tags and uses that real release tag as the baseline.

## Intentional API Changes Before v1

Ptah is still pre-v1, so maintainers may intentionally approve breaking changes
to the stable embedder API. Intentional approval must be explicit in the same
reviewed change:

- update this document if packages move between stable and non-public surfaces;
- update `docs/public_api.snapshot` when any exported surface changes —
  symbols, struct fields, interface method sets, or methods on concrete named
  types;
- add one package-level approval line to `docs/public_api_approvals.txt` when
  `scripts/check-public-api-released.sh` reports an incompatibility against the
  latest `v0.x` baseline;
- include the compatibility rationale in the PR description.

Do not weaken the CI checks, broaden exclusions, or silently remove packages
from the stable list to hide an API change.
