---
title: API schema export
description: Project selected Go entities into OpenAPI or GraphQL contract candidates and review their trust-boundary limitations.
---

Ptah projects a desired schema into API-facing formats: OpenAPI 3.0 component
schemas, GraphQL SDL, and Protobuf definitions. Use these artifacts when the
selected database entities intentionally match your transport model, or as input
to a separately designed contract.

The source can be Go annotations under `--root-dir` or a YAML, HCL, or SQL
schema file named by `--schema-file` — the same desired-schema sources
`ptah schema render` reads.

The export does not expose database rows or create a working API. It does not
generate handlers, resolvers, Protobuf services, authentication, authorization,
or database access. The generated schema is a contract candidate that you must
review before publishing.

The generated OpenAPI passes `redocly lint`; the generated GraphQL parses and
builds with `graphql-js`, for the types-only default and for every operation
profile.

This page covers the OpenAPI and GraphQL targets, which are stateless. The
Protobuf target is stateful — field numbers are persistent wire identifiers, so
its generated file is committed compatibility state — and has its own page:
[Protobuf schema export](../protobuf/).

## Commands

```bash
# OpenAPI 3.0 — components.schemas keyed by table name
ptah schema export --to openapi-v3 --root-dir ./models --out openapi.yaml

# GraphQL SDL — one object type per table, and no operations
ptah schema export --to graphql --root-dir ./models --out schema.graphql

# Add operation shapes by name
ptah schema export --to graphql --root-dir ./models \
  --graphql-operations list,by-id,create-input --out schema.graphql

# Omit --out to write the schema to stdout (for piping into a validator)
ptah schema export --to graphql --root-dir ./models > schema.graphql

# Export a YAML, HCL, or SQL schema file instead of Go annotations
ptah schema export --to openapi-v3 --schema-file schema.yaml --out openapi.yaml
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `--to` | all | `hcl`, `openapi-v3`, `graphql`, or [`protobuf`](../protobuf/). The old `atlas-hcl` value is accepted as an alias. |
| `--from` | all | Format of the `--schema-file` value: `go` (default), `yaml`, `hcl`, or `sql`. |
| `--root-dir` | all | Directory scanned for Go annotations. |
| `--schema-file` | `openapi-v3`, `graphql`, `protobuf` | YAML, HCL, or SQL schema file to export instead of Go annotations. Repeatable. |
| `--out` | all | Output file. Optional for `openapi-v3`/`graphql` (stdout when omitted); required for `hcl` and for [`protobuf`](../protobuf/), where it is also the compatibility state read back on the next run. |
| `--include-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated allowlist of tables. |
| `--exclude-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated denylist, applied after the allowlist. |
| `--title` | `openapi-v3` | Value for `info.title` (default `Ptah Exported Schema`). |
| `--graphql-operations` | `graphql` | Comma-separated operation shapes: `none` (default), `list`, `by-id`, `create-input`, `update-input`. |

Export warnings (for example an enum whose values cannot be resolved) are written
to stderr, so a schema piped from stdout is never corrupted.

## Sources

`--schema-file` is read by the same resolver as `ptah schema render`, so a
desired schema is spelled the same way on both commands. The `openapi-v3`,
`graphql`, and `protobuf` targets read all four sources below:

- **Go annotations** — the directory named by `--root-dir`, which defaults to
  `.`. This is `--from go`.
- **[YAML schema](../yaml/)** — a `--schema-file` whose extension is `.yaml` or
  `.yml`. This is `--from yaml`.
- **[HCL schema](../hcl/)** — a `--schema-file` whose extension is `.hcl`. This
  is `--from hcl`.
- **[SQL schema](../sql/)** — a `--schema-file` whose extension is `.sql`. This
  is `--from sql`.

An export taken from a schema file is byte-identical to the export taken from
annotated Go models describing the same tables, so a project can change how it
spells its schema without republishing a different contract.

`--from` declares the file's format and is checked against its extension:
`--from yaml --schema-file schema.sql` is refused rather than parsed as YAML.
Leave `--from` unset and the extension decides. Naming both `--root-dir` and
`--schema-file` merges them into one
[composite desired schema](../composite/); `--root-dir` alone keeps its `.`
default, which a schema-file export never picks up.

Two combinations are refused rather than approximated:

- `--to hcl` with `--schema-file`. That target rewrites the Go files it reads —
  `--cleanup-go-annotations` removes their annotations after a lossless export —
  so its source is `--root-dir`. Converting a schema file to HCL is a different
  operation from migrating annotations out of Go code.
- `--from db`. An export reads a schema definition, not a live database. Run
  [`ptah introspect`](../../start/adopt-an-existing-database/) to generate
  annotated models from a database URL, review them, then export those.

## OpenAPI

Each table becomes one Schema Object under `components.schemas`. Columns become
properties; `NOT NULL` columns (and primary keys, which are `NOT NULL` by rule)
go in `required`; nullable columns get `nullable: true`.

```yaml
openapi: 3.0.3
info:
  title: Ptah Exported Schema
  version: 1.0.0
servers:
  - url: /
paths: {}
components:
  schemas:
    products:
      type: object
      required:
        - id
        - name
        - price
        - status
      properties:
        id:
          type: integer
          format: int32
        name:
          type: string
          maxLength: 255
        price:
          type: number
        status:
          type: string
          enum:
            - active
            - inactive
```

The document is minimal but valid: `paths` is empty and a placeholder `servers`
entry is included so `redocly lint` passes. `components.schemas` can be `$ref`'d
from, or merged into, a hand-authored specification.

## GraphQL

Each table becomes an object type. Enum columns become enum types, and foreign
keys become object relations alongside the scalar id column. That is the whole
default export: no `Query`, no inputs, no connections.

```graphql
scalar DateTime

enum ProductStatus {
  active
  inactive
}

type Product {
  id: ID!
  name: String!
  price: Float!
  status: ProductStatus!
  category_id: Int!
  category: Category!
}
```

A foreign key whose target table is filtered out is dropped and reported as a
warning rather than producing a dangling reference.

The document declares no root operation type, so `graphql-js` builds it and
`validateSchema` reports the absent `Query`. That is the correct shape for a
type-system document meant to be composed into a schema you design.

### Operation shapes

`--graphql-operations` adds operation-shaped definitions by name. Each value is
selected independently.

| Value | What it adds |
| --- | --- |
| `none` | Nothing; this is the default written out. |
| `list` | A `Connection`/`Edge` pair per table, a shared `PageInfo`, and a `Query` field `<tables>(first: Int, after: String)`. |
| `by-id` | A `Query` field `<table>(<key>: <KeyType>!)` per table with a single-column primary key. |
| `create-input` | A `<Type>CreateInput` per table. |
| `update-input` | A `<Type>UpdateInput` per table, without the primary key and with every field optional. |

```graphql
type Query {
  products(first: Int, after: String): ProductConnection
  product(id: ID!): Product
}

input ProductCreateInput {
  name: String!
  price: Float!
  status: ProductStatus
  category_id: Int!
}

input ProductUpdateInput {
  name: String
  price: Float
  status: ProductStatus
  category_id: Int
}
```

Inputs come from the **write projection**: the columns a caller may assign.
Auto-increment and `SERIAL` columns, `GENERATED ALWAYS AS IDENTITY` columns,
generated/computed columns, and columns with a MySQL `ON UPDATE` expression are
excluded, because the database produces their values. A column with a plain
`DEFAULT` stays but becomes optional on create — `status` above.

An operation Ptah cannot complete is omitted and reported, never emitted broken:
a composite or absent primary key gets no `by-id` field, a key column that the
object type did not publish gets none either, and an empty projection produces
no input type. Selecting only input shapes leaves the document without a `Query`;
selecting a query shape always produces a legal, non-empty one.

:::caution[Generated operations are declarations, not an API]
Ptah generates no resolvers, data access, authorization, tenant isolation,
filtering, ordering, pagination behavior, or transactions. A `Connection`/`Edge`
pair names the Relay shape without implementing cursors, and `first`/`after` are
argument declarations rather than a pagination guarantee. The generated file
says so in a comment when operation shapes are present.
:::

## Type mapping

The lookup is dialect-agnostic: Postgres and MySQL spellings (`SERIAL`,
`INT AUTO_INCREMENT`, `DOUBLE PRECISION`) normalize to the same result.

| Ptah type | OpenAPI (`type`/`format`) | GraphQL |
| --- | --- | --- |
| `SMALLINT`, `INT`, `SERIAL`, `INT AUTO_INCREMENT` | `integer` / `int32` | `Int` |
| `BIGINT`, `BIGSERIAL` | `integer` / `int64` | `Int` |
| `BOOLEAN` | `boolean` | `Boolean` |
| `DECIMAL(p,s)`, `NUMERIC`, `REAL`, `DOUBLE PRECISION` | `number` | `Float` |
| `VARCHAR(n)`, `CHAR(n)` | `string` (`maxLength: n`) | `String` |
| `TEXT`, `UUID`, `INET`, … | `string` | `String` |
| `DATE`, `TIMESTAMP`, `TIME` | `string` / `date-time` (or `date`) | `DateTime` (custom scalar) |
| `JSON`, `JSONB` | `object` | `JSON` (custom scalar) |
| enum column | `string` + `enum` | enum type |
| single-column primary key | as above, in `required` | `ID!` |

An unrecognized column type maps to `string`/`String` and emits a warning, so an
unresolved custom type is visible rather than silently wrong.

## What the projection does not preserve

The generated contract is not a complete translation of database behavior:

- OpenAPI and GraphQL do not carry database defaults, unique constraints, check
  constraints, generated expressions, or transaction behavior. Enforce the API
  rules in the implementing service even when the database also enforces them.
- GraphQL `Int` is a signed 32-bit value. A non-primary-key `BIGINT` maps to
  `Int`, so values outside that range need a separately designed scalar or
  contract.
- `DECIMAL` and `NUMERIC` map to OpenAPI `number` and GraphQL `Float`. Those
  representations can lose decimal precision.
- GraphQL declares `DateTime` and `JSON` scalar names but does not provide their
  parsing, serialization, or validation behavior.
- The GraphQL write projection recognizes auto-increment, `SERIAL`,
  `GENERATED ALWAYS AS IDENTITY`, generated/computed, and MySQL `ON UPDATE`
  columns as server-owned. It cannot see a value supplied by a trigger or by an
  application layer, so review every generated input field.
- Foreign-key relation fields describe a possible object shape. They do not
  define loading, batching, tenant checks, or authorization.

## Scope and limitations

The export describes selected table columns, primary keys, foreign keys, and
enums. Non-column objects such as views, triggers, functions, row-level security
(RLS) policies, and indexes are not emitted.

Database and API models solve different problems. A database model can contain
tenant identifiers, audit fields, internal states, credential material, and
operational columns that no external caller should see. It can also change on a
different schedule from a public API.

Use direct projection for an internal contract or a deliberately isomorphic
domain model. Use a curated API model when the contract crosses a trust boundary,
must remain stable across storage refactors, or exposes only part of a row.

:::caution[Table selection is not field-level access control]
`--include-tables` and `--exclude-tables` select whole tables. Once a table is
selected, every exportable column enters the generated shape. Ptah does not
currently provide field allowlists, read/write visibility, API aliases, or
sensitive-field classification.
:::

The generated surface differs by target:

| Target | Ptah emits | Ptah does not emit |
| --- | --- | --- |
| OpenAPI | Component schemas under `components.schemas` | Paths, operations, handlers, or authorization |
| GraphQL | Object and enum types; input, connection, and `Query` shapes when `--graphql-operations` asks for them | Resolvers, a server, data access, or authorization |
| Protobuf | Messages and enums | Services, remote procedure calls (RPCs), handlers, or authorization |

Publishing any generated schema reveals the selected entity and field names,
translated types, enum values, relations, and exported source comments. Schema
metadata is not an authorization boundary, but you should not disclose internal
metadata that consumers do not need.

RLS omission is important: the generated schema cannot describe who may read or
write a field or row. Keep authorization in the service that implements the
contract. Do not infer API permissions from database constraints or from the
presence or absence of a generated field.

Database identifiers also become API identifiers after target-specific name
normalization. Ptah does not currently provide a stable alias between a table or
column name and its exported API name. A storage rename can therefore become a
contract rename.

GraphQL operation-shaped definitions do not grant access by themselves, but
they can be wired unsafely. They are opt-in for that reason: request the shapes
you have decided to implement, and do not pass generated input objects directly
to persistence code without an explicit assignment allowlist.

## Review before publishing

Before publishing or generating runtime code from an export:

1. Use `--include-tables` rather than exporting the entire model by default.
2. Inspect every generated field, including identifiers, audit columns, and
   server-managed values. Review exported table and field comments too.
3. If a selected table contains a field that must not cross the trust boundary,
   use a curated source model or a separately authored contract. Table filters
   cannot remove individual fields.
4. Define authentication, authorization, tenant isolation, validation, and
   assignment rules in the implementing service.
5. Review the generated diff when the database model changes. An additive
   database column can be an additive but unintended API change.
6. Run the target linter or compiler and the consumer compatibility tests before
   publishing the artifact.

For OpenAPI, merge selected components into a hand-authored specification when
the public contract differs from storage. For GraphQL, start from the types-only
default and add an operation shape only once you have decided how it will be
resolved and authorized; the generated `Query` and input types are syntax, not
an authorization or resolver design.
For Protobuf, use generated messages behind separately designed services or
wrapper messages when the public model must differ.

## Next steps

- Need a stateful wire contract? Use
  [Protobuf schema export](../protobuf/).
- Need to verify what the source model declares? Review
  [Go annotations](../go-annotations/).
- Publishing the artifact from a pull request? Add it to
  [Continuous integration](../../testing/ci/).
