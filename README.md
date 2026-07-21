# Ptah

Ptah is a schema and migration toolkit for Go projects. It can read annotated Go
models, YAML schema files, supported Atlas HCL schema files, and live databases;
render SQL; plan and run migrations; validate migration hashes; and expose
Atlas-compatible command paths under `ptah atlas <command> ...` and through the
separate `ptah-compat` binary.

Ptah is pre-GA. The native command tree is still allowed to change when a cleaner
architecture is better.

## Start Here

| Need | Read |
| --- | --- |
| First successful local run | [Quick start](docs/site/src/content/docs/getting-started.md) |
| Application-owned Go schema | [Go schema workflow](docs/site/src/content/docs/workflows/go-schema.md) |
| YAML or Atlas HCL schema source | [Schema files](docs/site/src/content/docs/workflows/schema-files.md) |
| Migration operations | [Migrations](docs/site/src/content/docs/workflows/migrations.md) |
| Atlas-compatible CLI paths | [Atlas-compatible CLI](docs/site/src/content/docs/workflows/atlas-cli.md) |
| CI setup | [CI](docs/site/src/content/docs/workflows/ci.md) |
| Command and feature comparison | [Comparison](docs/site/src/content/docs/reference/comparison.md) |
| Dialect behavior | [Capabilities](docs/site/src/content/docs/reference/capabilities.md) |
| Problems during use | [Troubleshooting](docs/site/src/content/docs/operate/troubleshooting.md) |

The documentation site source lives in [`docs/site`](docs/site). It is built
with Astro + Starlight, following the same versioned-site structure used by the
Inventario documentation.

## Install Or Build

From a checkout:

```bash
GOWORK=off go build -o ./bin/ptah ./cmd/ptah
./bin/ptah version

GOWORK=off go build -o ./bin/ptah-compat ./cmd/ptah-compat
./bin/ptah-compat migrate --help
```

From Go modules:

```bash
go install github.com/stokaro/ptah/cmd/ptah@latest
ptah version

go install github.com/stokaro/ptah/cmd/ptah-compat@latest
ptah-compat migrate --help
```

## Minimal Example

```go
package models

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//migrator:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string
}
```

```bash
ptah schema render --root-dir ./models --dialect postgres
ptah migrations plan --root-dir ./models --db-url "$DATABASE_URL"
ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --verify-sum
```

For a complete copy-pasteable SQLite run, use the
[quick start](docs/site/src/content/docs/getting-started.md).

## Command Surfaces

Ptah has two CLI surfaces:

- Native Ptah commands such as `ptah schema render`, `ptah db read`,
  `ptah migrations up`, and `ptah viz`.
- Atlas-compatible commands, either under `ptah atlas <command> ...` or through
  the binary-level `ptah-compat <command> ...` entry point. A copied or
  symlinked `ptah-compat` executable named `atlas` presents Atlas-style root
  commands for existing scripts.

Do not use root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect`; those paths are intentionally absent from the native
`ptah` binary. Use `ptah-compat migrate apply` or a binary named `atlas` when a
script needs Atlas-style root commands.

See [native CLI command tree](docs/native_cli.md) and
[Atlas-compatible CLI](docs/site/src/content/docs/workflows/atlas-cli.md).

## Atlas Compatibility Status

Ptah is working toward Atlas OSS compatibility, but this repository does not
claim full Atlas parity until the conformance gates prove it.

The current Atlas compatibility evidence lives in the separate
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository. That repo owns the regenerated reports:

- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

See [Conformance](docs/site/src/content/docs/operate/conformance.md).

## License-Clean Boundary

Ptah does not use Atlas source code.

Ptah is an independent implementation that studies Atlas's public interface,
observable behavior, and test assets. Atlas-derived Apache-2.0 fixture material
is kept in the separate `ptah-atlas-conformance` repository so this MIT-licensed
source tree stays implementation-clean:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

See [License boundary](docs/site/src/content/docs/operate/license-boundary.md).

## Existing References

The docs site is the human-facing entrypoint. The existing markdown files remain
the detailed source references:

- [Native CLI](docs/native_cli.md)
- [Project configuration](docs/project_config.md)
- [Atlas project config subset](docs/atlas_project_config.md)
- [Atlas HCL schema](docs/atlas_hcl_schema.md)
- [YAML schema](docs/yaml_schema.md)
- [Capabilities](docs/capabilities.md)
- [Exit codes](docs/exit_codes.md)
- [GitHub Action](docs/github_action.md)
- [System design](docs/system_design.md)

## Examples

- [Schema visualization example](examples/viz)
- [Embedded migrator example](examples/migrator)
- [Parser example](examples/migrator_parser)

## Build The Documentation Site

```bash
cd docs/site
npm install
npm run build
```

For versioned output, set `DOCS_VERSION`:

```bash
DOCS_VERSION=edge npm run build
```
