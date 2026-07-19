# Public Go API

Ptah is pre-GA, but embedders need a documented surface and a typed error
contract. Packages in this document are the only non-command, non-example,
non-fixture packages that may remain importable without an explicit review.

## Stable Embedder API

These packages are intended for application and tool embedders:

- `github.com/stokaro/ptah/config`
- `github.com/stokaro/ptah/config/projectconfig`
- `github.com/stokaro/ptah/core/ast`
- `github.com/stokaro/ptah/core/goschema`
- `github.com/stokaro/ptah/core/platform`
- `github.com/stokaro/ptah/core/platform/capability`
- `github.com/stokaro/ptah/core/ptaherr`
- `github.com/stokaro/ptah/core/renderer`
- `github.com/stokaro/ptah/core/sqlutil`
- `github.com/stokaro/ptah/dbschema`
- `github.com/stokaro/ptah/dbschema/types`
- `github.com/stokaro/ptah/migration/generator`
- `github.com/stokaro/ptah/migration/lint`
- `github.com/stokaro/ptah/migration/migrator`
- `github.com/stokaro/ptah/migration/planner`
- `github.com/stokaro/ptah/migration/risk`
- `github.com/stokaro/ptah/migration/safety`
- `github.com/stokaro/ptah/migration/schemadiff`
- `github.com/stokaro/ptah/migration/schemadiff/types`
- `github.com/stokaro/ptah/migration/seeder`

Public failures from these packages should use `core/ptaherr` where the caller
can reasonably branch on the error. In particular, annotation failures should
support `errors.As(err, *ptaherr.ParseError)`, and unsupported dialect failures
should support `errors.Is(err, ptaherr.ErrUnsupportedDialect)`.

## Provisional Surface

These packages are currently importable and therefore tracked by CI, but they
are not yet a stable API commitment. Before GA, either promote them to the
stable list above or move them behind `internal/` package boundaries. Track that
package-boundary cleanup in
[#426](https://github.com/stokaro/ptah/issues/426).

- `github.com/stokaro/ptah/core/astbuilder`
- `github.com/stokaro/ptah/core/atlashcl`
- `github.com/stokaro/ptah/core/atlashclrender`
- `github.com/stokaro/ptah/core/convert/dbschematogo`
- `github.com/stokaro/ptah/core/convert/fromschema`
- `github.com/stokaro/ptah/core/convert/toschema`
- `github.com/stokaro/ptah/core/lexer`
- `github.com/stokaro/ptah/core/parser`
- `github.com/stokaro/ptah/core/renderer/dialects/clickhouse`
- `github.com/stokaro/ptah/core/renderer/dialects/mariadb`
- `github.com/stokaro/ptah/core/renderer/dialects/mysql`
- `github.com/stokaro/ptah/core/renderer/dialects/mysqllike`
- `github.com/stokaro/ptah/core/renderer/dialects/postgres`
- `github.com/stokaro/ptah/core/renderer/dialects/sqlite`
- `github.com/stokaro/ptah/core/renderer/types`
- `github.com/stokaro/ptah/core/sqllint`
- `github.com/stokaro/ptah/core/yamlschema`
- `github.com/stokaro/ptah/dbschema/clickhouse`
- `github.com/stokaro/ptah/dbschema/mysql`
- `github.com/stokaro/ptah/dbschema/postgres`
- `github.com/stokaro/ptah/dbschema/sqlite`
- `github.com/stokaro/ptah/migration/migratesum`
- `github.com/stokaro/ptah/migration/onlineddl`
- `github.com/stokaro/ptah/migration/planner/dialects/clickhouse`
- `github.com/stokaro/ptah/migration/planner/dialects/mysql`
- `github.com/stokaro/ptah/migration/planner/dialects/postgres`
- `github.com/stokaro/ptah/migration/planner/dialects/sqlite`
- `github.com/stokaro/ptah/migration/planner/registry`
- `github.com/stokaro/ptah/migration/typechange`

## Compatibility Guard

CI runs two public API checks:

- `scripts/check-public-api.sh` fails when `go list ./...` finds a
  non-command, non-example, non-fixture package that is importable from outside
  this module but not listed here.
- `scripts/check-public-api-snapshot.sh` regenerates the `go doc -short`
  exported-symbol snapshot for every package listed here and compares it with
  `docs/public_api.snapshot`. Any exported surface change must update the
  snapshot in the same reviewed change.

After the first `v0.x` tag exists, add a released-baseline API compatibility
check (`apidiff` or `gorelease`) for the stable package list. Until then, there
is no authoritative released baseline to compare against; the package ledger
and snapshot are the enforceable pre-release guards. Track the released-baseline
upgrade in [#427](https://github.com/stokaro/ptah/issues/427).
