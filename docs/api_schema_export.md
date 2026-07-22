# API Schema Export (OpenAPI / GraphQL)

Ptah exports the schema it parses from Go annotations to API-facing formats:
OpenAPI 3.0 component schemas and GraphQL SDL. The parsed `goschema.Database`
already carries types, nullability, enums and foreign keys, so each format is a
direct projection of that intermediate representation.

- Generated OpenAPI passes [`redocly lint`](https://redocly.com/docs/cli/commands/lint/).
- Generated GraphQL passes [`graphql-js`](https://github.com/graphql/graphql-js)
  `parse` and `buildSchema`.

Both are exercised in CI (`.github/workflows/export-acceptance.yml`).

## Commands

```bash
# OpenAPI 3.0 — components.schemas keyed by table name
ptah schema export --to openapi-v3 --root-dir ./models --out openapi.yaml

# GraphQL SDL — object, input, and Relay connection types per table
ptah schema export --to graphql --root-dir ./models --out schema.graphql

# Omit --out to write the schema to stdout for piping into a validator
ptah schema export --to graphql --root-dir ./models > schema.graphql
```

| Flag | Applies to | Meaning |
| --- | --- | --- |
| `--from` | all | Source format. Only `go` is supported. |
| `--to` | all | Target format: `hcl`, `openapi-v3`, or `graphql`. The old `atlas-hcl` value is accepted as an alias. |
| `--root-dir` | all | Directory scanned for Go annotations. |
| `--out` | all | Output file. Optional for `openapi-v3`/`graphql` (stdout when omitted); required for `hcl`. |
| `--include-tables` | `openapi-v3`, `graphql` | Comma-separated allowlist of tables. |
| `--exclude-tables` | `openapi-v3`, `graphql` | Comma-separated denylist, applied after the allowlist. |
| `--title` | `openapi-v3` | Value for `info.title` (default `Ptah Exported Schema`). |

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

Each table produces:

- an **object type** — one field per column, with `NOT NULL` rendered as `Type!`;
- object **relations** for foreign keys (`author_id: Int` keeps its scalar column
  and gains `author: Author`);
- a create **input type** that omits server-generated (serial / auto-increment)
  columns;
- a Relay-style **`Connection`/`Edge`** pair, plus a shared `PageInfo`;
- **enum types** for enum columns whose values are valid GraphQL names; and
- a **`Query`** root with a by-id lookup (single-column primary keys) and a
  paginated list per table.

Primary keys are rendered as `ID`, and `DateTime`/`JSON` are declared as custom
scalars when used. Type names are singularized and PascalCased
(`simplified_users` → `SimplifiedUser`); collisions are disambiguated with a
numeric suffix so the schema builds cleanly.

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

## Scope and limitations

The export describes what an API component schema or GraphQL type needs: table
columns, primary keys, foreign keys and enums. Non-column database objects
(views, materialized views, triggers, functions, RLS policies, standalone
indexes) are not part of an API schema and are not emitted. Use `--include-tables`
/ `--exclude-tables` to scope the output to the entities you actually expose.

The HCL schema target (`--to hcl`) is documented in
[HCL Schema](atlas_hcl_schema.md). The old `--to atlas-hcl` spelling remains an
accepted alias for existing scripts.
