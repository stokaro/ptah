---
title: ClickHouse
description: ClickHouse in Ptah - the capability-limited preset, the MergeTree round trip, views and materialized views, roles and grants, and the revision table's engine.
---

ClickHouse support is capability-limited. The preset models enums as inline
`Enum8`/`Enum16` column types; foreign keys and enforced `CHECK` constraints
are outside the preset. Roles and grants are managed declaratively, within the
boundaries [Roles and grants](#roles-and-grants) states.
Review generated SQL and the
[capability gates](../../reference/capabilities/) before adopting a workflow
on ClickHouse.

## Dev-database cleanup

Dev-database replay cleanup requires ClickHouse 24.11 or newer
and global `SHOW DATABASES` plus `SHOW TABLES`. Because ordinary views have no
complete catalog dependency metadata, cleanup also requires that other user
databases contain no view-like or dictionary objects and no `Buffer`,
`Distributed`, or `Merge` tables.

The narrower reset used by shadow verification, by the `schema apply` dev
rehearsal, and by `schema clean` removes tables, views, and materialized views.
A materialized view goes as one object, with `DROP VIEW`, so its inner storage
table leaves with it rather than being dropped out from under it. Live views and
window views are left alone, matching what the ClickHouse reader reports; use
the database-realm cleanup above when a database has to be emptied completely.

## Tables and their engine clauses

A MergeTree table's sorting key is carried through the read, so a table Ptah
creates reads back as itself: the primary-key flag comes from
`system.columns.is_in_primary_key`, and the renderer derives the `ORDER BY` a
MergeTree engine requires from it. Where a table declares both a narrower
`PRIMARY KEY` and a wider `ORDER BY`, the sorting key is carried separately, so
a description does not silently sort by fewer columns than the table it
describes. Before this the read dropped the key entirely: a declaration carrying
`primary="true"` differed from its own table on every comparison, so
`ALTER TABLE ... MODIFY COLUMN` was re-planned forever, and `ptah db read`
exited 2 against any ClickHouse database Ptah could create because the
description it produced could not be rendered (stokaro/ptah#1603).

Every other clause of the engine is carried with it, so a table read and
replayed is the table that was read rather than a MergeTree that happens to hold
the same columns. The engine is taken with its parameters — a
`ReplacingMergeTree(ver)` stays one, and does not fall back to the default
`MergeTree`, which would merge on nothing — and so are the `PARTITION BY`,
`PRIMARY KEY`, `ORDER BY`, `SAMPLE BY`, `TTL` and `SETTINGS` clauses. The `TTL`
is the one that changes what the data does rather than how fast it is read: a
table replayed without it keeps rows it was configured to delete.

Two things follow that are worth knowing before adopting the round trip:

- **The settings are the server's resolved values, not the ones a `CREATE`
  named.** `system.tables` reports `index_granularity = 8192` for a table that
  never mentioned it, so a description carries that value and a replay pins it.
  It is the value the source table is actually running with.
- **A clause keyword is a legal column name, and the two are told apart by
  position.** A table sorted by a column named `settings` or `ttl` is read as
  what it is; the clause is only recognized where a clause can start.

## Views and materialized views

Plain views participate in the complete render, plan, and introspection cycle.
Ptah emits `CREATE VIEW`, `CREATE OR REPLACE VIEW`, and `DROP VIEW`, preserving
qualified names and query bodies, and reads ordinary views from
`system.tables`. An empty query body, `WITH CHECK OPTION`, or
`DROP VIEW ... CASCADE` fails instead of being ignored.

Materialized views do too. Ptah emits
`CREATE MATERIALIZED VIEW <name> ENGINE = MergeTree ORDER BY tuple() AS <query>`,
reads the object back from `system.tables`, and plans a changed query as a drop
followed by a create. Several ClickHouse-specific points are worth knowing
before adopting them:

- A materialized view maintains itself from inserts into its source. Ptah
  declares no refresh strategy and emits no refresh: a declaration that carries
  `refresh_strategy` is refused when it is parsed, on every dialect.

- ClickHouse's own **scheduled** materialized views are managed. A declaration
  carries the schedule as ClickHouse spells it, and Ptah renders it, reads it
  back, and reconciles it:

  ```go
  //ptah:schema:matview name="user_stats" body="SELECT count() AS c FROM users" refresh="every 1 hour"
  ```

  The server rewrites what it stores — `EVERY 60 MINUTE` becomes `EVERY 1 HOUR`,
  `AFTER 90 SECOND` becomes `AFTER 1 MINUTE 30 SECOND` — so a declaration is
  normalized to the stored spelling before anything compares it. Any spelling of
  the same schedule therefore converges instead of re-planning forever.

  A changed schedule is applied with `ALTER TABLE <view> MODIFY REFRESH`, which
  keeps the rows the view accumulated. A view gaining its first schedule or
  losing its last is a drop and a create instead, because the server refuses
  that transition in place: `Alter of type 'MODIFY_REFRESH' is not supported by
  storage MaterializedView`. That drop empties the view.

  `OFFSET`, `RANDOMIZE FOR`, `DEPENDS ON` and `APPEND` are carried too. `OFFSET`
  belongs to `EVERY` alone, and an interval mixing calendar units with clock
  ones is refused where it is declared, both matching the server.

- The storage clause is written explicitly rather than left to the server.
  ClickHouse 25.x and later accept a materialized view with no storage clause
  and supply `MergeTree ORDER BY tuple()` themselves; 24.x rejects it with
  `ORDER BY or PRIMARY KEY clause is missing`.
- The drop is spelled `DROP VIEW`. `DROP MATERIALIZED VIEW` is a syntax error on
  ClickHouse, and `DROP VIEW` removes the view together with the inner table
  that stores its result.
- `POPULATE` is never emitted, so a materialized view starts empty and fills
  from inserts into its source rather than from the rows already there.
  `POPULATE` is a one-shot argument that leaves no trace in the catalog, so
  nothing Ptah reads back could diff it. Backfill existing rows yourself if you
  need them.
- A query written without a database qualifier is read back carrying one.
  ClickHouse resolves the query when the view is created and records what it
  resolved, so `SELECT count() AS c FROM users` comes back from
  `system.tables.as_select` as `SELECT count() AS c FROM <database>.users`.
  Comparison removes the qualifier the object's own database added, the same way
  it does for an ordinary view, so an unchanged declaration is not reported as
  drift and is never planned as a drop and a create. A qualifier naming some
  other database is a real difference and is still reported.

- A changed query is applied destructively. The drop takes the inner storage
  table and every row the view had accumulated, and the replacement omits
  `POPULATE`, so the view starts empty again and refills only from new inserts.
  ClickHouse does have `ALTER TABLE <view> MODIFY QUERY`, which keeps the stored
  rows, but it refuses any query whose output columns differ from the ones the
  storage table already has, and a Ptah declaration carries a query body with no
  column list to compare, so the planner cannot tell the two cases apart before
  the statement runs. Treat a materialized-view body change as a change that
  empties the view.

The `TO <target table>` form and refreshable materialized views are not emitted:
the shared schema model carries a name and a query, so it can name neither a
separate target table nor a refresh schedule.

A materialized view created elsewhere with `TO <target table>` is still read, and
it is read as though it owned its storage: `system.tables` reports the same
engine and the same `as_select` for both forms, and the target appears only in
`create_table_query`, which this reader does not consult. Such a view therefore
compares as synchronized against a declaration of the same query, and a later
body change is planned as a drop and a create that recreates it in the
inner-storage form, so inserts stop reaching the original target table. Do not
manage a `TO` materialized view with Ptah.

## Roles and grants

Roles and grants complete the render, plan, apply, introspect, and diff cycle.
A declared role and a database- or table-scoped grant are applied, read back
from `system.roles` and `system.grants`, and compared to zero difference on the
next run. Declaring them is the same pair of annotations every other engine
uses, with the grant scope written as `database.table`:

```go
//ptah:schema:role name="ptah_reader"
type PtahReaderRole struct{}

//ptah:schema:grant role="ptah_reader" privileges="SELECT" on_table="ptah_test.orders"
type PtahReaderOrdersGrant struct{}
```

`ptah schema render --dialect clickhouse` emits those as:

```sql
CREATE ROLE IF NOT EXISTS `ptah_reader`;
GRANT SELECT ON `ptah_test`.`orders` TO `ptah_reader`;
```

Roles are always planned before grants, because ClickHouse refuses a grant to a
role it does not know. `GRANT ... WITH GRANT OPTION` is emitted when the
declaration asks for it, and taking that option away again is the single
statement `REVOKE GRANT OPTION FOR ...`, which leaves the privilege itself in
place. Removing a grant emits `REVOKE <privileges> ON <scope> FROM <role>`.
`DROP ROLE IF EXISTS` has a rendering for a schema source that spells one out,
but comparison never plans it — see "Roles are never dropped" below.

What Ptah manages here is roles and grants, and nothing else in ClickHouse's
access control:

| Not managed | What that means for you |
| --- | --- |
| Users | No user is created, altered, or read, so no credential enters a description, a plan, or a log. Provision users outside Ptah and grant them a managed role. |
| Role membership | `GRANT <role> TO <role>` is not modeled. Ptah reads and writes privilege grants only. |
| Quotas, row policies, settings profiles | Outside the schema model entirely; a declaration cannot express them and a read does not report them. |
| Column-scoped grants | `GRANT SELECT(id) ON db.t` is refused when declared and excluded when read. Grants are managed at database and table scope. |
| Wildcard and global scopes | `*.*` and a wildcard database are refused. Such a grant reaches objects no declared schema describes. |
| Privilege names the server rewrites | `ALL`, `CREATE`, `DROP`, `SYSTEM`, `SYSTEM FLUSH`, `ACCESS MANAGEMENT`, `SHOW ACCESS`, `SHOW FILESYSTEM CACHES`, and — at table scope only — `SHOW` and `ALTER`. See below. |

Five refusals arrive before anything on the server changes, and the reason for
each is worth knowing in advance:

- **A ClickHouse role carries no attributes.** `system.roles` is
  `(name, id, storage)`, so a declared `password`, `login`, `superuser`,
  `createdb`, `createrole`, or `replication` is refused rather than dropped.
  Dropping a password would leave you believing a credential was set on an
  object that cannot hold one. `ALTER ROLE` is refused for the same reason:
  there is nothing to alter.
- **The server absorbs a narrower grant into a broader one.** Granting `SELECT`
  on `db.*` and on `db.t` leaves one row for `db.*`, in either order, and the
  table-level grant is recorded nowhere. Declaring both is refused, because the
  pair could never converge: the absorbed grant would read as missing on every
  inspection and the plan would re-issue it forever. Declare the broader scope.
- **A grant scope must be qualified as `database.table`.** An unqualified table
  name is refused. Rendering is offline and has no current database to resolve
  it against, and resolving an access-control decision against whichever
  database a session happens to have selected is not a formatting mistake. A
  trailing dot such as `shop.` is refused too, rather than read as the whole
  database: a typo must not widen one table's privilege to every table.
- **A grant must name a role the same schema declares.** ClickHouse resolves a
  grantee by name across users AND roles, with no syntax to say which is meant.
  If nothing of that name exists the `GRANT` fails partway through a migration
  with `UNKNOWN_ROLE`; if a *user* of that name exists it succeeds and lands on
  the user, where the reader never sees it again — the plan re-issues it forever
  and a real account quietly holds a privilege nobody declared for it. Declaring
  the role removes both outcomes, because Ptah creates it in the same plan.
  One case this cannot cover: if a live ClickHouse user shares a name with a
  declared role, resolution still prefers the user. Do not name a role after an
  account.
- **Some privilege names are groups the server rewrites.** `GRANT CREATE ON db.*`
  records four rows — `CREATE DATABASE`, `CREATE TABLE`, `CREATE VIEW`,
  `CREATE DICTIONARY` — and never reads back as `CREATE`, so a schema declaring
  it can never converge. `GRANT ALL` records 45 individual rows on 26.7 and 39 on
  24.10. `GRANT SHOW ACCESS` is stored as `SHOW ROW POLICIES`. `GRANT SHOW
  FILESYSTEM CACHES` is accepted and records *nothing at all*, which would tell
  you a grant applied while the role held no privilege. Declare the individual
  privileges instead. Group names that do read back as written stay
  declarable — `ALTER TABLE`, `ALTER VIEW`, `ALTER COLUMN`, `ALTER INDEX`,
  `ALTER STATISTICS`, `ALTER PROJECTION`, `ALTER CONSTRAINT`, `SYSTEM SENDS`,
  and `SHOW` and `ALTER` on a database scope. A name the server itself refuses,
  such as `INTROSPECTION` or `SYSTEM RELOAD`, needs no gate here: ClickHouse
  answers `Code: 509 ... cannot be granted on the database level`, which names
  the problem more precisely than Ptah could.

Three more boundaries shape what a run does:

- **Roles are never dropped.** A role that exists on the server and not in the
  schema is named in the plan and left alone. A ClickHouse role is server-wide
  and may carry grants outside the managed schema, so removing it is not Ptah's
  decision to make. A read reports only the roles the described grants name, and
  leaves out roles defined in the server's configuration files, whose `storage`
  is `users_xml`, because SQL does not own those.
- **A partial revoke fails the comparison.** `GRANT SELECT ON db.* TO r`
  followed by `REVOKE SELECT ON db.t FROM r` leaves two rows in `system.grants`,
  and the role's effective privileges are the first minus the second. A
  declaration cannot express an exception, so on a managed role Ptah refuses
  instead of comparing equal and reporting convergence. Remove the partial
  revoke, or stop declaring the role.
- **An account that may not read the access catalog still gets a read.**
  Reading `system.roles` and `system.grants` needs a privilege reading a table
  does not, and an account holding only `SELECT`, `SHOW TABLES` and
  `SHOW COLUMNS` is answered `Code: 497 ... (ACCESS_DENIED)` by both. Rather
  than failing the whole read — which would break reading a schema that
  declares no role at all — Ptah describes everything else and records that it
  did not look. Comparison then withholds every declared role instead of
  planning a `CREATE ROLE` it could not verify, and nothing destructive can
  follow, because removal is decided from live rows and there are none.
  `ptah-compat schema inspect` says so on stderr.

Two operational notes. ClickHouse RBAC statements carry no `ON CLUSTER` clause,
so on a cluster they affect the connected replica only; Ptah does not model
cluster propagation, and a multi-replica deployment needs its own arrangement
for that. And a ClickHouse role is server-wide rather than database-scoped, so a
role created against a dev or throwaway database outlives the database that
workflow drops.

The connected account needs the privileges these statements require —
`ROLE ADMIN` plus `GRANT OPTION` on what it grants. The `docker-compose.yaml`
in this repository configures `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`, which
is what gives its `ptah_user` that authority.

## The revision table's storage engine

ClickHouse gives a table no engine unless the statement names one, and whether
an unnamed one is legal at all is decided by the server's `default_table_engine`
— whose own default value is `None`. Against such a server the first
`migrations up` stopped before the first migration:

```text
code: 119, message: Table engine is not specified in CREATE query
```

Ptah names `ENGINE = MergeTree` in both revision layouts, which is what a server
whose default is already `MergeTree` was producing, so an existing deployment
sees the table it had.

**A replicated deployment has to say so.** With the default, the migration
history is a node-local `MergeTree` on whichever node ran the migration, and
every replica then reports itself consistent. Name the engine instead:

```console
$ ptah migrations up --db-url 'clickhouse://…' \
    --migrations-engine "ReplicatedMergeTree('/clickhouse/tables/{shard}/schema_migrations', '{replica}')"
```

`PTAH_MIGRATIONS_ENGINE` carries the same value, and is the only spelling on the
compatibility surface — `ptah-compat` registers no flag for it, because the
community binary has none and the conformance `cli-surface` tier asserts flag
parity against it. `ptah-compat migrate apply`, `status` and `down` read the
variable; the native verbs take either.

Two things follow:

- **Every command that can create the table has to be given the same value.**
  `migrations up`, `down`, `status`, `tag`, `baseline`, `repair`, and the
  maintenance verbs `edit`, `rm` and `rebase` all initialize metadata when they
  are the first to run. `CREATE TABLE IF NOT EXISTS` does not re-engine a table
  that already exists, so whichever command ran first decided it.
- **An engine the revision table cannot be is refused before any statement
  runs.** On MySQL and MariaDB it must be InnoDB, which is the only engine that
  records an applied migration when the statement around it fails; SQL Server's
  revision table has no engine clause at all. The refusal happens first because
  MySQL DDL commits: rejecting the table afterwards would leave one Ptah will
  neither use nor recreate.

## Atlas revision metadata

Ptah creates the Atlas revision table with `partial_hashes` declared as text.
ClickHouse reads a trailing `NULL` on a column definition as `Nullable(T)`, so
declaring that column `JSON` asks for `Nullable(JSON)`, and no ClickHouse server
handles that the way the rest of the Atlas revision layout needs:

- Servers that reject `Nullable(JSON)` refuse the `CREATE` during type analysis
  (`code: 43, Nested type JSON cannot be inside Nullable type`). The rejection
  happens before the `IF NOT EXISTS` existence check, so an already-provisioned
  database does not escape it, and Atlas-format apply fails before any user
  migration SQL runs.
- Servers that accept `Nullable(JSON)` coerce the JSON null Ptah writes into the
  JSON type and store `{}` instead. The value the author wrote is replaced, and
  the column can no longer be scanned into a string by an Atlas-compatible
  consumer.

Declaring the column text resolves to `Nullable(String)` on both, which stores
the same JSON null every other dialect stores. This is stated by behavior rather
than by a version cut-off on purpose: the release that changed `Nullable(JSON)`
from rejected to accepted was not measured, and the column type is correct on
either side of it.

`CREATE TABLE IF NOT EXISTS` does not alter a table that already exists, so a
revision table created by an earlier Ptah against a server that accepted
`Nullable(JSON)` keeps that column type and keeps storing `{}`. Ptah ignores a
legacy `{}` on clean rows, but reads `partial_hashes` on dirty Atlas-format rows
with committed progress. Malformed metadata, including `{}` where a digest
array is required, fails closed during status or retry. Ptah does not rewrite
the column automatically; alter it to `Nullable(String)` or recreate the table
before recovering such a dirty row.

Setting a revision directly (`SetAtlasRevision`) is still unsupported on
ClickHouse, because revision history cannot be updated atomically there.

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- Declaring roles and grants in Go sources: [Go annotation reference](../../reference/go-annotations/).
