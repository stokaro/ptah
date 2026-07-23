# Public Go API

Ptah is pre-GA, but embedders need a documented surface and a typed error
contract. Packages in this document are the only non-command, non-example,
non-fixture packages that may remain importable without an explicit review.
For task-oriented guidance and examples, see the
[Reusable components](site/src/content/docs/reference/reusable-components.md)
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

`atlascompat` is a narrow compatibility surface for external Atlas parity and
conformance tooling. It intentionally wraps parser, HCL schema,
conversion, and migration sum internals without making those implementation
packages importable directly.

Public failures from these packages should use `core/ptaherr` where the caller
can reasonably branch on the error. In particular, annotation failures should
support `errors.As(err, *ptaherr.ParseError)`, and unsupported dialect failures
should support `errors.Is(err, ptaherr.ErrUnsupportedDialect)`.

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
  exported-symbol snapshot for every package listed here, expands public
  interface method sets, and compares it with `docs/public_api.snapshot`.
  Any exported surface change must update the snapshot in the same reviewed
  change.
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
- update `docs/public_api.snapshot` when exported symbols or public interface
  method sets change;
- add one package-level approval line to `docs/public_api_approvals.txt` when
  `scripts/check-public-api-released.sh` reports an incompatibility against the
  latest `v0.x` baseline;
- include the compatibility rationale in the PR description.

Do not weaken the CI checks, broaden exclusions, or silently remove packages
from the stable list to hide an API change.
