---
title: API schema export
description: Project selected Go entities into OpenAPI or GraphQL contract candidates and review their trust-boundary limitations.
---

Ptah projects the schema it parses from Go annotations into API-facing formats:
OpenAPI 3.0 component schemas, GraphQL SDL, and Protobuf definitions. Use these
artifacts when the selected database entities intentionally match your transport
model, or as input to a separately designed contract.

The export does not expose database rows or create a working API. It does not
generate handlers, resolvers, Protobuf services, authentication, authorization,
or database access. The generated schema is a contract candidate that you must
review before publishing.

The generated OpenAPI passes `redocly lint`; the generated GraphQL parses and
builds with `graphql-js`.

This page covers the OpenAPI and GraphQL targets, which are stateless. The
Protobuf target is stateful — field numbers are persistent wire identifiers, so
its generated file is committed compatibility state — and has its own page:
[Protobuf schema export](../protobuf/).

## Commands

```bash
# OpenAPI 3.0 — components.schemas keyed by table name
ptah schema export --to openapi-v3 --root-dir ./models --out openapi.yaml

# GraphQL SDL — an object, input, and Relay connection per table
ptah schema export --to graphql --root-dir ./models --out schema.graphql

# Omit --out to write the schema to stdout (for piping into a validator)
ptah schema export --to graphql --root-dir ./models > schema.graphql
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `--to` | all | `hcl`, `openapi-v3`, `graphql`, or [`protobuf`](../protobuf/). The old `atlas-hcl` value is accepted as an alias. |
| `--root-dir` | all | Directory scanned for Go annotations. |
| `--out` | all | Output file. Optional for `openapi-v3`/`graphql` (stdout when omitted); required for `hcl` and for [`protobuf`](../protobuf/), where it is also the compatibility state read back on the next run. |
| `--include-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated allowlist of tables. |
| `--exclude-tables` | `openapi-v3`, `graphql`, `protobuf` | Comma-separated denylist, applied after the allowlist. |
| `--title` | `openapi-v3` | Value for `info.title` (default `Ptah Exported Schema`). |

Export warnings (for example an enum whose values cannot be resolved) are written
to stderr, so a schema piped from stdout is never corrupted.

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

Each table becomes an object type, a create `input`, and a Relay-style
`Connection`/`Edge` pair. Enum columns become enum types, foreign keys become
object relations alongside the scalar id column, and a `Query` root exposes a
by-id lookup and a paginated list per table.

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

input ProductInput {
  name: String!
  price: Float!
  status: ProductStatus!
  category_id: Int!
}

type Query {
  products(first: Int, after: String): ProductConnection
  product(id: ID!): Product
}
```

The `input` type omits server-generated columns (serial / auto-increment). A
foreign key whose target table is filtered out is dropped and reported as a
warning rather than producing a dangling reference.

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
- A GraphQL create input omits serial and auto-increment columns. Other
  server-managed or database-defaulted columns are not recognized
  automatically and can still appear in the input.
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
| GraphQL | Object and input types, connections, and a `Query` root | Resolvers, a server, data access, or authorization |
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
they can be wired unsafely. In particular, do not pass generated input objects
directly to persistence code without an explicit assignment allowlist.

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
the public contract differs from storage. For GraphQL, treat the generated
`Query` and input types as syntax, not as an authorization or resolver design.
For Protobuf, use generated messages behind separately designed services or
wrapper messages when the public model must differ.

## Next steps

- Need a stateful wire contract? Use
  [Protobuf schema export](../protobuf/).
- Need to verify what the source model declares? Review
  [Go annotations](../go-annotations/).
- Publishing the artifact from a pull request? Add it to
  [Continuous integration](../../testing/ci/).
