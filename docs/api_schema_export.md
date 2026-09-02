# API schema export (OpenAPI / GraphQL)

Ptah exports a desired schema to API-facing formats: OpenAPI 3.0 component
schemas, GraphQL SDL, and Protobuf definitions. The parsed
`schemamodel.Database` already carries types, nullability, enums and foreign
keys, so each format is a direct projection of that intermediate
representation.
That intermediate representation also separates source transport from export:
`--schema-file` reads YAML, HCL, SQL, or DBML through `internal/schemaload`, and
`--root-dir` reads Go annotations. Sources produce the same artifact for
semantics both can express. YAML, HCL, and Go can additionally author API names,
contract type overrides, and exposure; SQL and DBML cannot carry that metadata
losslessly and derive the contract from storage instead.
This is contract generation, not database publication: Ptah emits no runtime
server, data access, authentication, or authorization.

- Generated OpenAPI passes [`redocly lint`](https://redocly.com/docs/cli/commands/lint/).
- Generated GraphQL passes [`graphql-js`](https://github.com/graphql/graphql-js)
  `parse` and `buildSchema`, for the types-only default and for every operation
  profile. A profile that selects a query shape also passes `validateSchema`
  with no errors.

Both are exercised in CI (`.github/workflows/export-acceptance.yml`).

The generated artifact is not automatically a safe public API. Database models
can contain internal, tenant, audit, credential, and personally identifiable
fields. Declare `api_expose` on a column to decide whether it reaches a contract
and in which direction, and pass `--api-field-policy=allowlist` so a column that
declares nothing reaches none. Both shape the generated document and neither is
access control. Review every generated field before publishing it.

This file covers the two stateless targets. The Protobuf target (`--to protobuf`,
rendered by `internal/protobufrender`) is stateful — field numbers are persistent
wire identifiers, so `--out` is required and the previously generated file is
read back as committed compatibility state. Its mapping, Edition 2023 rationale,
and the `--proto-*` policy flags are documented in
[Protobuf schema export](./site/src/content/docs/schema/protobuf.md).

## Commands

```bash
# OpenAPI 3.0 — components.schemas keyed by table name
ptah schema export --to openapi-v3 --schema-file schema.yaml --out openapi.yaml

# GraphQL SDL — data types per table, and nothing else by default
ptah schema export --to graphql --schema-file schema.yaml --out schema.graphql

# Ask for operation shapes by name
ptah schema export --to graphql --schema-file schema.yaml \
  --graphql-operations list,by-id,create-input --out schema.graphql

# Omit --out to write the schema to stdout for piping into a validator
ptah schema export --to graphql --schema-file schema.yaml > schema.graphql

# Go annotations use --root-dir instead
ptah schema export --to openapi-v3 --root-dir ./models --out openapi.yaml
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `--from` | all | Format of the `--schema-file` value: `go` (default), `yaml`, `hcl`, `sql`, or `dbml`. Checked against the file extension; unset takes the format from the extension. `db` is refused — inspect the database to a file first. |
| `--to` | all | Target format: `hcl`, `openapi-v3`, `graphql`, [`protobuf`](./site/src/content/docs/schema/protobuf.md), `markdown`, `html`, or `dbml`. The old `atlas-hcl` value is accepted as an alias. |
| `--root-dir` | all | Directory scanned for Go annotations. |
| `--schema-file` | every target except `hcl` | YAML, HCL, SQL, or DBML schema file. Repeatable; merged with `--root-dir` when both are given. Refused for `hcl`, whose export rewrites the Go files it reads. |
| `--out` | all | Output file. Optional for `openapi-v3`/`graphql` (stdout when omitted); required for `hcl` and for [`protobuf`](./site/src/content/docs/schema/protobuf.md), where it is also the compatibility state read back on the next run. |
| `--include-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated allowlist of tables. |
| `--exclude-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated denylist, applied after the allowlist. |
| `--title` | `openapi-v3` | Value for `info.title` (default `Ptah Exported Schema`). |
| `--graphql-operations` | `graphql` | Comma-separated operation shapes to generate: `none` (the default), `list`, `by-id`, `create-input`, `update-input`. Unset means `none`. `none` cannot be combined with another value, and an unrecognized value is refused. |

Export warnings — such as an enum whose values cannot be resolved, or a foreign
key to a filtered-out table — are written to stderr. A schema piped from stdout
is therefore never corrupted by diagnostic text.

## OpenAPI output

Each table becomes one Schema Object under `components.schemas`. Columns become
properties. `NOT NULL` columns (and primary keys, which are `NOT NULL` by rule)
are listed in `required`; nullable columns get `nullable: true`. A `VARCHAR(n)`
contributes `maxLength: n`, and enum columns contribute an `enum` list.

The document is minimal but valid — `paths` is empty and a placeholder `servers`
entry is emitted so the recommended redocly ruleset reports no errors. The
`components.schemas` block can be `$ref`'d from, or merged into, a hand-authored
specification.

## GraphQL output

The default export is types-only. Each table produces:

- an **object type** — one field per column, with `NOT NULL` rendered as `Type!`;
- object **relations** for foreign keys (`author_id: Int` keeps its scalar column
  and gains `author: Author`); and
- **enum types** for enum columns whose values are valid GraphQL names.

Primary keys are rendered as `ID`, and `DateTime`/`JSON` are declared as custom
scalars when used. Type names are singularized and PascalCased
(`simplified_users` → `SimplifiedUser`); collisions are disambiguated with a
numeric suffix so the schema builds cleanly. The object-type names do not depend
on the operation selection, so turning an operation on never renames a published
type.

A types-only document declares no root operation type. It parses and builds with
`graphql-js`, and `validateSchema` reports the absent `Query` root: that is what
a type-system document meant for composition looks like, and it is the honest
shape for an export with no resolvers behind it.

### Operation shapes are opt-in

`--graphql-operations` selects operation shapes by name. Each value is
independent, and the generated output carries a comment saying what the shapes
are not.

| Value | What it adds |
| --- | --- |
| `none` | Nothing. The default, spelled out. |
| `list` | A Relay-style `Connection`/`Edge` pair per table, a shared `PageInfo`, and a `Query` field `<tables>(first: Int, after: String)`. |
| `by-id` | A `Query` field `<table>(<key>: <KeyType>!)` for each table with a single-column primary key. |
| `create-input` | A `<Type>CreateInput` per table, from the write projection. |
| `update-input` | A `<Type>UpdateInput` per table, from the write projection minus the primary key, with every field optional. |

The **write projection** is the set of columns a caller may assign. It excludes
columns the database produces: auto-increment and `SERIAL` columns, PostgreSQL
`GENERATED ALWAYS AS IDENTITY` columns, generated/computed columns, and columns
with a MySQL `ON UPDATE` expression. A column with a plain `DEFAULT` stays in the
projection but becomes optional in the create input, because the database fills
it in when it is omitted. `GENERATED BY DEFAULT AS IDENTITY` also stays: a caller
may supply it.

An operation whose inputs are incomplete is omitted rather than emitted broken,
and each omission is reported as a warning on stderr:

- a table with no exportable columns contributes no type and no operations;
- a composite or absent primary key produces no `by-id` field;
- a primary-key column that the object type did not publish (a name collision,
  for instance) produces no `by-id` field;
- a create or update projection that is empty produces no input type;
- two tables whose query field names collide keep the first and report the
  second.

The `by-id` argument repeats the key column's published type, so a key that did
not map to `ID` is not re-declared as one.

Selecting only input shapes adds no root operation, so the document still has no
`Query`. Selecting a query shape always yields a legal, non-empty `Query`, even
when `--include-tables` selected nothing: the root is then the placeholder
`type Query { _empty: Boolean }`.

## Type mapping

The lookup is dialect-agnostic: the Postgres and MySQL spellings Ptah emits
(`SERIAL`, `INT AUTO_INCREMENT`, `DOUBLE PRECISION`) all normalize to the same
result.

| Ptah type | OpenAPI (`type` / `format`) | GraphQL |
| --- | --- | --- |
| `SMALLINT`, `INT`, `SERIAL`, `INT AUTO_INCREMENT` | `integer` / `int32` | `Int` |
| `BIGINT`, `BIGSERIAL` | `integer` / `int64` | `Int` |
| `BOOLEAN` | `boolean` | `Boolean` |
| `DECIMAL(p,s)`, `NUMERIC`, `REAL`, `DOUBLE PRECISION` | `number` | `Float` |
| `VARCHAR(n)`, `CHAR(n)` | `string` (`maxLength: n`) | `String` |
| `TEXT`, `UUID`, `INET`, `BYTEA`, … | `string` | `String` |
| `DATE`, `TIMESTAMP`, `TIME` | `string` / `date-time` or `date` | `DateTime` (custom scalar) |
| `JSON`, `JSONB` | `object` | `JSON` (custom scalar) |
| enum column | `string` + `enum` list | enum type |
| single-column primary key | as above, in `required` | `ID!` |

An unrecognized column type maps to `string` / `String` and emits a warning, so
an unresolved custom type (for example an enum whose definition was not found) is
visible rather than silently wrong.

## API export metadata by source

YAML, HCL, and Go annotations use the same authored names:

- tables: `api_name`, `openapi_name`, `graphql_name`, `proto_name`;
- columns: those four plus `api_type` and `api_expose`.

Target-specific names win over `api_name`, which wins over the database name.
Table GraphQL and Protobuf values are stems that Ptah singularizes and
PascalCases; column GraphQL and Protobuf values are exact field identifiers.
`api_type` changes the generated contract type only. `api_expose` accepts
`read`, `write`, `read-write`, or `none`.

SQL has no portable DDL spelling for these values. DBML settings and notes are
also not a lossless carrier, so Ptah rejects attempted metadata in DBML and
refuses to render a metadata-bearing schema to DBML before writing output.
Canonical HCL preserves all attributes through an OCI push/pull round trip.
Composite sources retain metadata on complete YAML, HCL, or Go definitions and
reject a duplicate definition with absent or different metadata; SQL and DBML
cannot act as overlays. External loaders carry metadata only when their declared
payload format is YAML or HCL.

See the reader-facing
[API schema export guide](./site/src/content/docs/schema/export.mdx#names-in-the-contract)
for YAML, HCL, and Go examples and the collision/refusal behavior.

## Scope and limitations

The export describes selected table columns, primary keys, foreign keys, and
enums. Non-column database objects such as views, materialized views, triggers,
functions, row-level security (RLS) policies, and standalone indexes are not
emitted.

`--include-tables` and `--exclude-tables` select whole tables. `api_expose`
selects a column's read/write direction, and
`--api-field-policy=allowlist` withholds undeclared columns. These controls shape
the document; they are not authorization. `api_name` and the target-specific
names can keep a published identity stable across storage renames. An additive
database column can still become an unintended API change under the default
field policy. Generated descriptions also expose table and field comments.

OpenAPI output contains components but no paths, Protobuf output contains
messages and enums but no services, and GraphQL output contains data types and
no operations. All three separate data shapes from operations by default.

A GraphQL export that opts in to operation shapes gets type declarations and
nothing more. Ptah generates no resolvers and defines no authorization, tenant
isolation, assignment, filtering, ordering, pagination, or transaction behavior
for them. A `Connection`/`Edge` pair names the Relay shape without implementing
cursors; `first` and `after` are argument declarations, not a pagination
guarantee. Row-level security is not translated: the presence or absence of a
generated field says nothing about who may read or write it.

The projection is lossy. It does not carry database defaults, unique or check
constraints, generated expressions, or transaction behavior. GraphQL `Int`
cannot represent the full `BIGINT` range, decimal values mapped to GraphQL
`Float` can lose precision, and custom `DateTime`/`JSON` scalars have no runtime
implementation. The write projection recognizes the server-owned column kinds
listed above, but it cannot see a value a trigger or an application layer
supplies, so review every generated input field.

Treat these outputs as reviewed contract inputs:

1. Select the smallest set of tables needed.
2. Inspect every generated field, relation, and exported source comment.
3. Keep field and row authorization in the implementing service.
4. Do not pass generated GraphQL inputs directly to persistence code without an
   assignment allowlist.
5. Review generated diffs and run consumer compatibility tests before
   publication.

The HCL schema target (`--to hcl`) is documented in
[HCL Schema](atlas_hcl_schema.md). The old `--to atlas-hcl` spelling remains an
accepted alias for existing scripts. The Protobuf target (`--to protobuf`) is
documented in
[Protobuf schema export](./site/src/content/docs/schema/protobuf.md); its scope
is narrower still, since Protobuf also has no way to express `NOT NULL` and
cannot distinguish an empty repeated field from SQL `NULL`.
