# Ptah

Some systems are built to carry the weight of the world as it already exists.

Ptah begins from a different premise: infrastructure should not merely bear
complexity, but shape it. Named after the ancient Egyptian god of architects and
craftsmen, Ptah turns intent into structure through four deliberate stages:
**Parse, Transform, Apply, Harmonize**.

It is built to understand the current state, reshape it with precision, apply
change safely, and bring systems into alignment — without inheriting more weight
than necessary.

## What Ptah does

Ptah is a schema and migration toolkit for Go projects. It can read annotated Go
models, YAML schema files, supported HCL schema files, and live databases;
render SQL; plan and run migrations; and validate migration hashes. A separate
`ptah-compat` binary is a drop-in replacement for the Atlas CLI.

## Independent implementation under European law

Ptah is developed in the Czech Republic under Czech and European Union law,
including [Directive 2009/24/EC](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:32009L0024)
on the legal protection of computer programs.

Ptah is an independent project, not affiliated with or endorsed by Ariga.

Ptah is an original, independent implementation. It does not reverse engineer
proprietary implementations: it does not decompile or disassemble proprietary
binaries, access proprietary source code, or copy protected implementation
expression from Atlas or any other product. The Ptah implementation includes
no third-party product code, including Atlas code. External implementation code
enters the project only through dependencies explicitly declared in repository
manifests.

Compatibility work is limited to public interfaces, documentation, properly
licensed assets, and external behavior lawfully observed while using software
under a valid right to use it. Subject to applicable law and without copying
protected expression, Ptah reserves the right to independently reimplement the
interface of any application to provide a free and open-source alternative.

This position rests in particular on:

- Articles 1(2), 5(3), and 8 of Directive 2009/24/EC. They distinguish protected
  expression from the ideas and principles underlying a program and its
  interfaces, permit an authorized user to observe, study, and test program
  behavior, and make contrary contractual terms null and void.
- [Sections 65 and 66 of Czech Act No. 121/2000 Coll.](https://e-sbirka.gov.cz/sb/2000/121),
  including Section 66(1)(d), which implements the right to study and test a
  computer program's functionality.
- [*SAS Institute Inc. v World Programming Ltd.*, C-406/10](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:62010CJ0406),
  where the Court of Justice of the European Union held that program
  functionality, programming languages, and data file formats are not protected
  forms of program expression, and confirmed the licensed user's right to
  observe, study, and test program behavior.

Atlas-derived Apache-2.0 fixture material remains in the separate
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository so this MIT-licensed source tree stays implementation-clean:

```text
ptah-atlas-conformance -> ptah
ptah                  !-> ptah-atlas-conformance
```

Legal questions concerning Ptah's development and compatibility work should be
addressed under Czech and European Union law. This section records the project's
development and provenance policy; it is not legal advice. See the detailed
[license boundary](docs/site/src/content/docs/atlas/license-boundary.md).

Ptah is pre-GA. The native command tree is still allowed to change when a cleaner
architecture is better.

## Start Here

| Need | Read |
| --- | --- |
| First successful local run | [Quick start](docs/site/src/content/docs/start/quick-start.md) |
| Application-owned Go schema | [Go annotations](docs/site/src/content/docs/schema/go-annotations.md) |
| SQL, YAML, HCL, or external desired schema | [YAML](docs/site/src/content/docs/schema/yaml.md), [HCL](docs/site/src/content/docs/schema/hcl.md), and [SQL](docs/site/src/content/docs/schema/sql.md) schema pages |
| ORM-owned desired schema | [ORM and external loaders](docs/site/src/content/docs/schema/orm-and-external.md) |
| Compose one schema from several packages or files | [Composite desired schema](docs/site/src/content/docs/schema/composite.md) |
| Migration operations | [Versioned migrations](docs/site/src/content/docs/versioned/overview.md) |
| Publish or consume migrations and schemas through OCI | [OCI registry artifacts](docs/site/src/content/docs/operate/oci-registry.md) |
| Test migrations or a desired schema | [Test migrations and schemas](docs/site/src/content/docs/testing/migrations-and-schema.md) |
| Atlas-compatible CLI paths | [Atlas compatibility overview](docs/site/src/content/docs/atlas/overview.md) |
| Reusable Go packages | [Reusable components](docs/site/src/content/docs/extend/components.md) |
| CI setup | [CI](docs/site/src/content/docs/testing/ci.md) |
| Command and feature comparison | [Comparison](docs/site/src/content/docs/atlas/comparison.md) |
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
go install go.5x5.cz/ptah/cmd/ptah@latest
ptah version

go install go.5x5.cz/ptah/cmd/ptah-compat@latest
ptah-compat migrate --help
```

## Minimal Example

```go
package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//ptah:schema:field name="email" type="TEXT" unique="true" not_null="true"
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
[quick start](docs/site/src/content/docs/start/quick-start.md).

## Command Surfaces

Ptah has two CLI surfaces:

- Native Ptah commands in the `ptah` binary, such as `ptah schema render`,
  `ptah db read`, `ptah migrations up`, and `ptah viz`.
- Atlas-compatible commands in the separate `ptah-compat` binary, such as
  `migrate apply` and `schema inspect`.

The `ptah-compat` binary is the binary-level drop-in replacement for scripts
that need Atlas-style root commands, invoked as `ptah-compat <command> ...`.
The main `ptah` binary has no Atlas command paths.

Do not use root-level Atlas spellings such as `ptah migrate apply`; those
paths are intentionally absent from the native `ptah` binary, whose migration
verbs live under `ptah migrations ...`.

See the [native CLI command reference](docs/site/src/content/docs/reference/native-commands.md) and
[Atlas compatibility overview](docs/site/src/content/docs/atlas/overview.md).

## Atlas Compatibility Status

Ptah is working toward Atlas OSS compatibility, but this repository does not
claim full Atlas parity until the conformance gates prove it.

The current Atlas compatibility evidence lives in the separate
[`stokaro/ptah-atlas-conformance`](https://github.com/stokaro/ptah-atlas-conformance)
repository. That repo owns the regenerated reports:

- [`gaps.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps.md)
- [`gaps-live.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-live.md)
- [`gaps-diff.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-diff.md)
- [`gaps-orm-providers.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/gaps-orm-providers.md)
- [`PARITY.md`](https://github.com/stokaro/ptah-atlas-conformance/blob/main/PARITY.md)

See [Conformance](docs/site/src/content/docs/atlas/conformance.md).

## Existing References

The docs site is the human-facing entrypoint. Repository-level Markdown files
remain only where they provide contributor or implementation detail beyond the
site:

- [Native CLI command reference](docs/site/src/content/docs/reference/native-commands.md)
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

## Examples

- [Schema visualization example](examples/viz)
- [Embedded migrator example](examples/migrator)
- [Parser example](examples/annotation_parser)

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
