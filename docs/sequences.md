# PostgreSQL Sequences with Ptah

Ptah supports standalone PostgreSQL sequences as a first-class schema object through the `//migrator:schema:sequence` annotation. A standalone sequence is created with `CREATE SEQUENCE`, altered with `ALTER SEQUENCE`, and dropped with `DROP SEQUENCE`, and participates in the full generate / compare / migrate / rollback lifecycle just like tables, views, functions, and roles.

Standalone sequences are a PostgreSQL feature that Atlas keeps out of its open-source core (it lives only in the proprietary "Pro" build). Ptah provides it as an open (MIT), local, no-account capability — see epic [#654](https://github.com/stokaro/ptah/issues/654).

## Standalone vs. implicit sequences

PostgreSQL creates an *implicit* sequence for every `SERIAL` / `BIGSERIAL` / `SMALLSERIAL` column and for identity columns (`GENERATED ... AS IDENTITY`). Those implicit sequences are owned by their column and are created and dropped automatically with it — you do not declare them.

A *standalone* sequence is one you declare explicitly, typically to share a single number generator across several tables or to control a column default with `DEFAULT nextval(...)`. Ptah's introspection deliberately excludes implicit serial/identity sequences, so declaring a plain `SERIAL` column never produces a spurious standalone-sequence diff.

## Declaring a sequence

```go
package models

//migrator:schema:sequence name="order_number_seq" as="bigint" start="1000" increment="1" cache="20"
type OrderNumberSeq struct{}
```

This renders:

```sql
CREATE SEQUENCE "order_number_seq" AS bigint INCREMENT BY 1 START WITH 1000 CACHE 20;
```

### Sequence attributes

| Attribute | Description |
| --- | --- |
| `name` | Sequence name (required) |
| `schema` | Target schema/namespace (optional) |
| `as` | Underlying integer type, e.g. `bigint`, `integer`, `smallint` |
| `start` | `START WITH` value |
| `increment` | `INCREMENT BY` value (must be non-zero) |
| `minvalue` | `MINVALUE` bound |
| `maxvalue` | `MAXVALUE` bound |
| `cache` | `CACHE` size |
| `cycle` | `true` to enable `CYCLE` (default `NO CYCLE`) |
| `owned_by` | `table.column` association (`OWNED BY`) |
| `if_not_exists` | Emit `IF NOT EXISTS` |
| `comment` | Optional comment |

Only the options you set are emitted. An option left unset relies on the PostgreSQL default and is never reported as a difference during comparison, so a sequence that uses defaults does not churn against the introspected catalog values.

## Using a sequence as a column default

Because a standalone sequence may back a column default, Ptah always creates sequences before the tables that reference them:

```go
//migrator:schema:sequence name="order_number_seq" start="1000"
type OrderNumberSeq struct{}

//migrator:schema:table name="orders"
type Order struct {
	//migrator:schema:field name="id" type="BIGINT" primary="true" default_expr="nextval('order_number_seq')"
	ID int64
}
```

## Ownership (`OWNED BY`)

`owned_by="table.column"` ties the sequence's lifecycle to a column. Because a sequence referenced by a column default must be created *before* its table, while `OWNED BY` requires the table to already exist, Ptah emits the association as a separate `ALTER SEQUENCE ... OWNED BY` *after* the table is created — the same ordering `pg_dump` uses.

> Note: PostgreSQL cannot distinguish a `SERIAL`-generated sequence from a manually created sequence when both are owned by a column *and* supply that column's default. A sequence in that exact shape is treated as the column's implicit sequence and is not tracked as standalone. A sequence that is merely consumed via `DEFAULT nextval(...)` (without `OWNED BY`), or that carries a lifecycle-only `OWNED BY` (whose owner column does not draw its default from it), is tracked normally.

> Rollback note: changing an existing sequence's `owned_by` and then rolling back restores the previous owner only when the sequence declared an owner before the change; because Ptah manages only declared options, a rollback does not emit `OWNED BY NONE` to detach an ownership that a prior migration added.

## Granting privileges on a sequence

Use `on_sequence` on a `//migrator:schema:grant` annotation. Valid sequence privileges are `USAGE`, `SELECT`, and `UPDATE`:

```go
//migrator:schema:grant role="app_writer" privilege="USAGE,SELECT" on_sequence="order_number_seq"
type AccessControl struct{}
```

See [PostgreSQL Role Management](./POSTGRESQL_ROLES.md) for the full grant model.

## Other dialects

Standalone sequences are PostgreSQL-only. MySQL, MariaDB, and SQL Server render a "not supported" comment (or, in the case of the planner, reject the change for SQLite), because these targets do not have a standalone sequence object.
