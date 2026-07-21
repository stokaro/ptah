# DML Upsert AST

Ptah exposes a small dialect-independent DML AST for one-row upsert rendering.
The first concrete renderer is SQL Server, where an `ast.UpsertNode` renders to
`MERGE`.

This API is separate from schema DDL parsing. Ptah's schema parser still treats
DML statements such as `INSERT`, `UPDATE`, `DELETE`, and `MERGE` as
schema-neutral migration content and skips them when building a schema AST.

## Model

`ast.UpsertNode` describes the portable intent of an upsert:

- target table name;
- insert columns and value expressions;
- match columns used to compare `target` and `source`;
- update assignments for the matched arm;
- optional update and insert predicates;
- optional SQL comment.

The renderer escapes table and column identifiers. Value expressions,
assignment expressions, and predicates are SQL fragments by design, so callers
should pass bind placeholders or trusted builder output, not unsanitized user
input.

Match conditions are intentionally structured as column comparisons. Do not use
target-only filters to decide whether a row exists; SQL Server documents that
filtering target rows in the `MERGE` `ON` clause can produce incorrect results.
Use `UpdatePredicate` and `InsertPredicate` for action-specific filtering, or
include every key column in `MatchColumns`.

## SQL Server Rendering

SQL Server renders the node as `MERGE INTO ... USING (VALUES ...)`:

```go
node := ast.NewUpsert("dbo.users").
    SetInsert(
        []string{"id", "email", "updated_at"},
        []string{"@p1", "@p2", "SYSUTCDATETIME()"},
    ).
    SetMatchColumns("id").
    AddUpdateAssignment("email", "source.[email]").
    AddUpdateAssignment("updated_at", "source.[updated_at]").
    SetComment("upsert user")

sql, err := renderer.RenderSQL("sqlserver", node)
```

The generated SQL is:

```sql
-- upsert user
MERGE INTO [dbo].[users] WITH (HOLDLOCK) AS target
USING (VALUES (@p1, @p2, SYSUTCDATETIME())) AS source ([id], [email], [updated_at])
ON target.[id] = source.[id]
WHEN MATCHED THEN
    UPDATE SET [email] = source.[email], [updated_at] = source.[updated_at]
WHEN NOT MATCHED THEN
    INSERT ([id], [email], [updated_at])
    VALUES (source.[id], source.[email], source.[updated_at]);
```

`WITH (HOLDLOCK)` is emitted by default for SQL Server upserts. SQL Server's
`MERGE` concurrency behavior is different from separate `INSERT` and `UPDATE`
statements, and `HOLDLOCK` gives the target table serializable locking
semantics for the statement.

## Unsupported Dialects

Dialects without an implemented upsert renderer fail explicitly with
`ptaherr.ErrUnsupportedFeature`. They do not emit comments or partial SQL,
because DML rendering must not look successful when no mutation statement was
generated.
