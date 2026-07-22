---
title: API schema export
description: Export Go entities to OpenAPI 3.0 component schemas or GraphQL SDL.
---

Ptah projects the schema it parses from Go annotations into API-facing formats:
OpenAPI 3.0 component schemas and GraphQL SDL. The parsed schema already carries
types, nullability, enums and foreign keys, so each format is a direct projection
of it — handy for teams that hand-author API specs from a database schema.

The generated OpenAPI passes `redocly lint`; the generated GraphQL parses and
builds with `graphql-js`.

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
| `--to` | all | `hcl`, `openapi-v3`, or `graphql`. The old `atlas-hcl` value is accepted as an alias. |
| `--root-dir` | all | Directory scanned for Go annotations. |
| `--out` | all | Output file. Optional for `openapi-v3`/`graphql` (stdout when omitted); required for `hcl`. |
| `--include-tables` | `openapi-v3`, `graphql` | Comma-separated allowlist of tables. |
| `--exclude-tables` | `openapi-v3`, `graphql` | Comma-separated denylist, applied after the allowlist. |
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

## Scope

The export describes table columns, primary keys, foreign keys and enums —
everything an API component schema or GraphQL type needs. Non-column objects
(views, triggers, functions, RLS, indexes) are not part of an API schema and are
not emitted. Use `--include-tables` / `--exclude-tables` to scope the output to
the entities you expose.
