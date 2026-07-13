# YAML Schema Input

Ptah can generate SQL from a language-agnostic YAML schema file instead of
scanning Go source annotations. The YAML frontend builds the same
`goschema.Database` intermediate representation as Go annotations, then runs the
normal finalization, dependency ordering, AST conversion, and dialect renderers.

Use YAML input when schema ownership should not depend on Go structs or when a
tool needs to generate a Ptah schema from another language.

## Generate SQL

```bash
go run ./cmd generate --schema-file schema.yaml --dialect postgres
```

`--schema-file` accepts `.yaml` and `.yml` files. When it is set, `--root-dir`
is ignored. If `--dialect` is omitted, Ptah renders every supported dialect.

## Minimal Example

```yaml
tables:
  users:
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
        unique: true
    indexes:
      idx_users_email:
        fields: [email]
```

This is equivalent to:

```go
//migrator:schema:table name="users"
type User struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
    Email string

    //migrator:schema:index name="idx_users_email" fields="email"
    _ int
}
```

## Complete Shape

Top-level objects are maps. Their keys are used as default object names when a
`name` field is not provided.

```yaml
enums:
  account_status: [active, suspended]

extensions:
  pg_trgm:
    if_not_exists: true

functions:
  current_tenant:
    params: ""
    returns: TEXT
    language: SQL
    security: DEFINER
    volatility: STABLE
    body: SELECT current_setting('app.current_tenant_id')

roles:
  app_user:
    login: true
    inherit: false

grants:
  app_user_schema:
    role: app_user
    privilege: USAGE
    on_schema: public
  app_user_users:
    role: app_user
    privileges: [SELECT, INSERT, UPDATE, DELETE]
    on_table: users

tables:
  tenants:
    columns:
      id: { type: SERIAL, primary: true }

  users:
    rls_enabled: true
    columns:
      id: { type: SERIAL, primary: true }
      tenant_id:
        type: INTEGER
        not_null: true
        foreign: tenants(id)
        foreign_key_name: fk_users_tenant
        on_delete: CASCADE
      status:
        type: account_status
        not_null: true
        default: active
      email:
        type: VARCHAR(255)
        not_null: true
        unique: true
        platform:
          mysql:
            type: VARCHAR(191)
    indexes:
      idx_users_email:
        fields: [email]
        unique: true
    constraints:
      chk_users_email:
        type: CHECK
        check: "position('@' in email) > 1"

rls_policies:
  users_tenant_isolation:
    table: users
    for: ALL
    to: app_user
    using: tenant_id = current_tenant()::INTEGER
```

## Tables

Each entry under `tables` declares one table.

| Key | Meaning |
|---|---|
| `name` | Database table name. Defaults to the map key. |
| `struct_name` | Internal goschema owner name. Defaults to the map key. |
| `engine` | Table engine value used by dialects that support it. |
| `comment` | Table comment. |
| `primary_key` | Table-level primary key column list. Scalar comma-separated values and YAML sequences are both accepted. |
| `checks` | Table-level check expressions. |
| `custom_sql` | Custom SQL attached to the table. |
| `columns` / `fields` | Ordered column map. Use one or the other; duplicate names across both are rejected. |
| `indexes` | Ordered table-local index map. |
| `constraints` | Ordered table-local constraint map. |
| `rls_enabled` | Adds row-level security enablement for this table. |
| `platform` / `overrides` | Dialect-specific override map, for example `platform.mysql.type`. |

Table-local `columns`, `fields`, `indexes`, and `constraints` preserve YAML
author order. Top-level maps render deterministically by sorted key.

## Columns

Columns support the same information as field annotations:

| Key | Meaning |
|---|---|
| `name` | Database column name. Defaults to the column map key. |
| `field_name` | Internal goschema field name. Defaults to the column map key. |
| `type` | SQL type or enum type name. |
| `nullable` | Explicit nullability. Defaults to nullable unless `not_null` is true. |
| `not_null` | Marks the column `NOT NULL`. |
| `primary` | Marks the column as a primary key. |
| `auto_increment` / `auto_inc` | Marks the column as auto-incrementing. |
| `unique` | Marks the column unique. |
| `unique_expr` | Unique expression. |
| `index` | Requests an index for the column. |
| `default` | Literal default value. |
| `default_expr` | Default SQL expression, such as `NOW()`. |
| `foreign` | Foreign key reference in `table(column)` form. |
| `foreign_key_name` | Explicit foreign key constraint name. |
| `on_delete` / `on_update` | Foreign key actions. |
| `enum` | Inline enum values. Scalar comma-separated values and sequences are accepted. |
| `check` | Column check expression. |
| `check_name` | Explicit column check constraint name. |
| `comment` | Column comment. |
| `platform` / `overrides` | Dialect-specific overrides. |

If `enum` is provided and `type` is empty or `ENUM`, Ptah creates an enum named
`enum_<struct_name>_<field_name>` and uses that generated type for the column.

## Indexes

Indexes can be table-local under `tables.<table>.indexes` or top-level under
`indexes`.

```yaml
indexes:
  idx_users_email:
    table: users
    fields: [email]
    unique: true
```

| Key | Meaning |
|---|---|
| `name` | Index name. Defaults to the map key. |
| `table` | Target table. Required for top-level indexes. |
| `fields` / `columns` | Indexed columns. Required. |
| `unique` | Emits a unique index. |
| `comment` | Index comment. |
| `type` | Dialect-specific index type. |
| `condition` | Partial-index condition where supported. |
| `ops` | Operator or operator class string. |
| `granularity` | ClickHouse data-skipping index granularity. |

## Constraints

Constraints can be table-local under `tables.<table>.constraints` or top-level
under `constraints`.

```yaml
constraints:
  fk_users_tenant:
    table: users
    type: FOREIGN KEY
    columns: [tenant_id]
    foreign_table: tenants
    foreign_column: id
    on_delete: CASCADE
```

Supported `type` values are `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY`, `CHECK`,
and `EXCLUDE`.

| Type | Required keys |
|---|---|
| `PRIMARY KEY` | `columns` |
| `UNIQUE` | `columns` |
| `FOREIGN KEY` | `columns`, `foreign_table`, `foreign_column` |
| `CHECK` | `check` |
| `EXCLUDE` | `using`, `elements` |

Top-level constraints also require `table`. `condition` is supported for
`EXCLUDE` constraints.

## Schema Objects

YAML input supports these schema objects. Extensions, functions, materialized
views, RLS, roles, and grants are PostgreSQL-specific; views and triggers are
also rendered for MySQL/MariaDB with dialect-specific trigger bodies.

- `extensions`: `name`, `if_not_exists`, `version`, `comment`
- `functions`: `name`, `params` or `parameters`, `returns`, `language`,
  `security`, `volatility`, `body`, `comment`
- `views`: `name`, `body`, `with_check`, `comment`
- `matviews`: `name`, `body`, `refresh_strategy`, `comment`
- `triggers`: `name`, `table`, `timing`, `event`, `for`, `body`, `comment`
- `rls_enabled_tables` or `rls_enabled`: map of tables with optional `table`,
  `struct_name`, and `comment`
- `rls_policies`: `name`, `table`, `for`, `to`, `using`, `with_check`,
  `comment`
- `roles`: `name`, `login`, `password`, `superuser`, `create_db`,
  `create_role`, `inherit`, `replication`, `comment`
- `grants`: `role`, `privilege` or `privileges`, `on_table`, `on_schema`,
  `with_option`, `comment`

`matviews.refresh_strategy` is retained as authoring metadata for future
refresh workflows. It is not drift-compared because PostgreSQL does not persist
that policy in `pg_class`/`information_schema`.

## Validation

The parser is intentionally strict:

- Unknown YAML fields are rejected.
- Duplicate keys in ordered maps are rejected.
- Multiple YAML documents in one file are rejected.
- Top-level indexes and constraints must name their target table.
- Constraint types and required semantic fields are validated before SQL
  generation.
