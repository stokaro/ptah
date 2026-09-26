---
title: MySQL and MariaDB
description: MySQL and MariaDB in Ptah - the dialect differences that reach generated SQL, the foreign-key and routine rules, cleanup privileges, and online-DDL routing.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which MySQL and MariaDB differences affect Ptah workflows?"
goal: "Compare the MySQL and MariaDB differences that affect Ptah workflows."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
searchAliases:
  - MySQL supported versions
  - MariaDB supported versions
overlaps: []
disposition: keep
---

MySQL and MariaDB share one planner and renderer family, but they are separate
dialects with different capability sets. Pass an explicit `--dialect mysql` or
`--dialect mariadb` in examples and CI jobs, and treat a plan reviewed for one
variant as unreviewed for the other. Differences that show up in generated
SQL:

- Enums are inline `ENUM` column types, not standalone type objects.
- MariaDB guards constraint and index drops with `IF EXISTS`; MySQL rejects
  that guard, so the `mysql` renderer strips it.
- The `DROP CHECK` spelling exists only on MySQL 8.0.16+; MariaDB uses the
  generic `DROP CONSTRAINT` clause.
- Portable foreign keys require InnoDB tables and compatible column types,
  signedness, character sets, and collations. MariaDB generated FK columns and
  MySQL virtual generated FK columns fail before rendering. MySQL stored
  generated columns reject referential actions the engine cannot apply. When
  an FK-participating table has no declared engine, Ptah emits
  `ENGINE=InnoDB` explicitly instead of trusting the session default.
- `SET NULL` requires nullable local columns. Explicit foreign-key names are
  limited to 64 characters; generated names are shortened deterministically.
- A nonunique referenced key must be a complete leftmost BTREE prefix.
  FULLTEXT, SPATIAL, HASH, parser-backed, expression, and prefix indexes do not
  qualify.
- A modified `SQL SECURITY DEFINER` routine is refused before migration SQL is
  planned when its catalog `DEFINER` differs from the connected
  `CURRENT_USER()`. Connect as that definer, change the desired routine to
  `SQL SECURITY INVOKER`, or leave the foreign routine unchanged. Missing
  ownership facts fail closed too.
- Routines have no overloads. A schema that declares two functions, or two
  procedures, of one name with different parameters is refused before any
  statement is planned. Applied, the second `CREATE` fails with Error 1304 and
  the first stays, measured on MySQL 8.4 and MariaDB 11.8. A function and a
  procedure of one name are two routines.
- An index declaring `SPATIAL` or `FULLTEXT` is compared against the access
  method the server reports, so a plain index of the same name over the same
  column is a difference rather than a match. An index declaring **no** type
  accepts whatever the engine chose, and the asymmetry is the engines': `CREATE
  INDEX` over a `POINT` column leaves `INDEX_TYPE=BTREE` on MariaDB 11.8 and
  `SPATIAL` on MySQL 8.4, so comparing an undeclared type would plan a rebuild
  on MySQL that MySQL immediately undoes.
- `USING BTREE` and `USING HASH` are read on a `KEY`, `INDEX` or `UNIQUE KEY`
  and rendered back after the column list. A `UNIQUE KEY` asking for a method
  is read as a unique index, which is what the server builds and where a method
  has somewhere to live. `BTREE` is not carried: `INDEX_TYPE` reports it for a
  declared `USING BTREE` and for an index that asked for nothing alike, so the
  two are one index to every reader Ptah has, and emitting it would put the
  clause into the DDL of every index read back from a server. `HASH` is
  carried, and whether the server honors it belongs to the storage engine
  rather than to the dialect: on InnoDB, MySQL 8.4 records `BTREE` and drops
  the clause from `SHOW CREATE TABLE` while MariaDB 11.8 records `HASH` and
  prints it back, and on `MEMORY` both record `HASH`. MariaDB records it on
  every engine that takes an index -- `InnoDB`, `MEMORY`, `MyISAM` and `Aria`
  all report `HASH`, and `ARCHIVE` refuses an index at all -- so there a desired
  `HASH` against a server reporting otherwise is a real difference and is
  reported. MySQL records it only on `MEMORY`, so the comparison stays quiet
  there for the same reason the undeclared type above is not compared: on the
  default engine a desired `HASH` reads back as `BTREE`, and reporting it would
  plan a rebuild MySQL immediately undoes. Deciding the MySQL case properly
  needs the table's storage engine, which the index comparison does not have.
- Two constraints on one table may share a name, and both engines accept
  `CONSTRAINT same UNIQUE (a)` beside `CONSTRAINT same FOREIGN KEY (a)`. Ptah
  identifies a named constraint by its type as well as its table and name, so
  both survive the read, the desired model, and the comparison. MySQL also lets
  a `CHECK` share a name with a `UNIQUE`, where MariaDB answers `ERROR 1826`.
- A key part's direction is read back from the catalog, so `KEY (a DESC)` and
  `KEY (a)` are told apart rather than both arriving ascending. It also decides
  which index a foreign key owns: MySQL will not back one with a descending
  leading part and builds its own index instead, while MariaDB reuses whatever
  covers the columns. So a same-named index beside a covering one is the
  author's on MariaDB and the engine's on MySQL, and only the first is planned
  for removal.
- A key part may be an expression on MySQL -- `KEY ((a + 1))`, a functional key
  part -- and an unnamed one takes the name the server gives it,
  `functional_index`, then `functional_index_2` and `functional_index_3`. Two
  refusals go with it, and they are different facts rather than one rule:
  MariaDB has no functional key parts at all and answers `ERROR 1064` to every
  spelling, so the dialect decides; MySQL accepts them in an index and refuses
  one in a `PRIMARY KEY` with `ERROR 3756`, so that refusal holds on both
  engines. A functional part in a table-body `UNIQUE KEY` is read as a unique
  index rather than as a constraint, which is what the server builds: MySQL
  reports one index with `NON_UNIQUE=0`, a null column and the expression, and
  a constraint has nowhere to keep an expression.
- An inline `KEY`, `INDEX` or `UNIQUE KEY` the author did not name is read with
  the name its server would assign: the first key part's column, then `_2`,
  `_3` for a name already taken. A prefix length and a `DESC` direction stay
  out of the name, and a column-level `UNIQUE` claims its column before any
  index does. The name is decided when the SQL is read rather than when it is
  written, because the catalog reports what the server chose and a desired
  schema that guessed differently would never converge with it.
- A foreign key the author did not name is read, on MySQL, with the name the
  server gives it: `<table>_ibfk_<n>`. In `CREATE TABLE` the unnamed keys are
  numbered from 1 in the order they are written, and a named key does not move
  the count. A key that `ALTER TABLE` adds takes one more than the highest
  `<table>_ibfk_<n>` the table holds. The server names the index it builds for
  such a key after the key's first column, then `_2` and on, and that index is
  read as the key's, so a schema file compares equal to the database its own
  SQL built. A derived name that another foreign key of the database already
  holds is refused, because MySQL refuses it with `ERROR 1826`. MariaDB names
  an unnamed key `<table>_ibfk_<n>` too, but Ptah reads it there with its own
  name, `fk_<table>_<columns>`, so a MariaDB file with an unnamed key plans a
  rename against the database it built
  ([stokaro/ptah#3743](https://github.com/stokaro/ptah/issues/3743)).
- Both engines accept `CONSTRAINT` without a name before `PRIMARY KEY`,
  `UNIQUE`, `FOREIGN KEY` and `CHECK`, as in
  `CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id)`. Ptah reads such a clause as
  the same clause written without the keyword, which is how the server builds
  it: it takes the name that clause takes, and an unnamed foreign key counts in
  the same `<table>_ibfk_<n>` sequence as the other unnamed keys of its table.
  The server answers `ERROR 1064` to every other symbol-less spelling and to a
  `CONSTRAINT` in front of `KEY`, `INDEX`, `FULLTEXT` or `SPATIAL`, with a name
  or without one, and Ptah refuses each of them by name.
- On a column, MySQL accepts `CONSTRAINT` only before `CHECK` and MariaDB only
  before `REFERENCES`, with a name or without one, as in
  `a INT CONSTRAINT ck CHECK (a > 0)` on MySQL. Measured on MySQL 8.4,
  MySQL 26.7 and MariaDB 11.8, the server answers `ERROR 1064` to every other
  kind, such as `a INT CONSTRAINT uq UNIQUE`, `CONSTRAINT pk PRIMARY KEY` or
  `CONSTRAINT nn NOT NULL`, and Ptah refuses each of them by name. Declared at
  table level, a unique key keeps its name on both engines, and a check keeps
  its name on MariaDB. In the column definition of `ALTER TABLE ... MODIFY`,
  MariaDB takes `CONSTRAINT` before nothing, and neither engine takes
  `REFERENCES`, with `CONSTRAINT` in front or without it; Ptah refuses those
  too.
- A bare column `REFERENCES`, as in `a INT REFERENCES p(id)`, means something
  different on each MySQL line: MySQL 8.4 builds nothing from it, while MySQL
  9.7 and 26.7 build the foreign key `<table>_ibfk_<n>` and an index on the
  column. A SQL file is read without the server version, so
  `--dialect mysql` refuses the clause and asks for a table-level
  `FOREIGN KEY`, which every line builds. MariaDB builds the key, and
  `--dialect mariadb` reads it.
- A non-ASCII index name is refused, rather than compared. The two engines fold
  such names differently and not in a way one rule covers: measured on MySQL
  8.4.11 and MariaDB 11.8.9 over a `utf8mb4` connection, `I` beside dotless
  `ı` and `Σ` beside final `ς` are accepted by MySQL and answer `ERROR 1061` on
  MariaDB, while dotted `İ` beside `i` and the Kelvin sign beside `K` do the
  opposite. A lone `prımary` is accepted by MySQL and answers `ERROR 1280` on
  MariaDB, which is why a solitary non-ASCII name is not a safe exception
  either -- it is still an unresolved comparison against the reserved
  `PRIMARY`. ASCII folding is shared and deterministic and is unchanged; a name
  derived from a non-ASCII column is refused for the same reason an explicit
  one is.
- Migration planning refuses a plan whose index names may collide, rather than
  emitting one the server rejects halfway. A name carrying a non-ASCII rune has
  an equivalence class Ptah cannot compute offline, so it is treated as a
  possible conflict with every other index name on that table, ASCII ones
  included -- `İ` collides with plain `i` on MySQL, so grouping only the
  non-ASCII names together would still miss it. Measured, the alternative was a
  half-applied migration: rendering `CREATE INDEX İ` and `CREATE INDEX i`
  against MySQL 8.4.11 creates the first and answers `ERROR 1061` on the
  second.
- **A comparison that reaches the target asks it instead of guessing.** The
  conservative rule above is what an offline run still does, and it was the
  only answer until the names could be resolved against the server: `a` beside
  `ä` is accepted by both engines and was reported as a possible conflict for
  want of anything better. A comparison holding a connection now asks, and gets
  the engine's own answer for each pair.
  What is asked is the collision rather than a fold to imitate, because neither
  engine exposes the fold its identifier comparison uses -- measured, none of
  `LOWER()`, `utf8mb4_general_ci`, `utf8mb4_unicode_ci`, `utf8mb4_uca1400_ai_ci`
  or `utf8mb4_bin` reproduces MariaDB's. A temporary table carrying the names as
  keys either creates or answers `ERROR 1061`, which is exactly the question,
  and a temporary table is this session's alone and gives the per-table
  namespace an index name actually lives in.
  Only names Ptah cannot fold are asked about, so a schema whose index names are
  all ASCII reaches no server and pays nothing. Sixty-four keys is the per-table
  ceiling on both engines, so a larger set is asked in several statements, each
  carrying every non-ASCII name so no pairing is missed. A refusal that is not
  the duplicate-name answer -- a lost connection, or an account without
  `CREATE TEMPORARY TABLES`, which answers `ERROR 1044` -- is reported rather
  than read as an equivalence.
- Column names are compared ASCII-case-insensitively, and a non-ASCII one is
  treated as a possible conflict with every column in its table. Both engines
  fold ASCII case: a table declaring `A` and `a` answers `ERROR 1060`, and a
  foreign key written `a` binds to a column declared `A` and reuses its key --
  so modeling these names as exact reported drift on every run and missed a
  duplicate the server refuses. Beyond ASCII the two disagree, and the same
  fold decides both questions: MySQL calls `İ`/`i` and the Kelvin sign/`K` one
  column and MariaDB calls `I`/`ı` and `σ`/`ς` one column, each engine
  accepting a foreign key written with either spelling of a pair it folds and
  reporting a missing key column for a pair it does not. The ASCII half of a
  pair is why the conflict is table-wide rather than per name: `İ` collides
  with plain `i` on MySQL.
- A non-ASCII column named by a key, a constraint, or its own `UNIQUE` is
  refused for the same reason, and the disagreement runs deeper there. Asked
  whether two columns differing only by the pair are one name, MySQL folds
  dotted `İ`/`i` and the Kelvin sign/`K` while MariaDB folds `I`/`ı` and
  `σ`/`ς` -- and MariaDB folds the Kelvin pair for that question while treating
  the two as different columns when it resolves a foreign key, so the rule is
  not one per engine either. A column nothing keys takes part in no comparison
  and is kept.
- Two indexes on one table claiming one name are refused. Both engines answer
  `ERROR 1061 Duplicate key name`, so accepting it would describe a table
  neither can create. `KEY (a), KEY a (b)` is that shape: the unnamed index
  takes `a` as soon as it is read, and the later explicit `a` collides with it.
- A table body declares a spatial or full-text index as
  `{SPATIAL|FULLTEXT} [INDEX|KEY] [name] (columns)`, and every optional part of
  that is optional here too — `FULLTEXT (bio)` is as readable as
  `FULLTEXT INDEX ft_bio (bio)`. The `WITH PARSER <name>` clause travels with
  it. `KEY` matters as much as `INDEX`: both dump tools normalize to it, so a
  table written with `FULLTEXT INDEX` comes back out of `mysqldump` as
  `FULLTEXT KEY`. An index left unnamed takes the name its server would give
  it, by the rule above.
- `KEY`, `SPATIAL`, `FULLTEXT` and `INDEX` open a table-level index here, and
  both engines reserve all four, so no column carries those names bare. Ptah
  reads the word as an index on this family and in dialect-neutral documents,
  which have no family to ask. A `.sql` source read for another dialect gets a
  column wherever that engine has no table element opening with the word.
  `key`, `spatial` and `fulltext` are columns on PostgreSQL, YugabyteDB,
  CockroachDB, Spanner, ClickHouse, SQLite and Oracle. SQL Server reserves
  `key`, so only `spatial` and `fulltext` are columns there. `index` is a
  column on PostgreSQL, YugabyteDB and Spanner, and on CockroachDB unless a
  column list follows it, directly or after a name. SQL Server reads `index` as
  its own inline index and ClickHouse as a data-skipping index, and SQLite and
  Oracle reserve it. Read as an index on the other dialects, `key text` would be
  refused, and `spatial nvarchar(32)` has the shape of an index named after its
  type over a column named `32` (stokaro/ptah#3089, stokaro/ptah#3299). An index
  names columns, so a column list that is a number is refused, and the refusal
  names the keyword. MySQL and MariaDB answer the same statement with
  `Error 1064`.
- A column carrying both a primary key and a `UNIQUE` is written back the way
  it was read, because the two spellings do not mean the same thing.
  `a INT UNIQUE, PRIMARY KEY (a)` builds the primary key and a secondary unique
  index named `a` on both engines, and it is rendered as the table-level key it
  was; `a INT PRIMARY KEY UNIQUE` builds both on MySQL and the primary key
  alone on MariaDB, and it is rendered inline so each engine gives its own
  answer. Folding the first into the second would lose MariaDB's second index.
- DDL commits implicitly on both engines, so a failed migration cannot be
  rolled back by the surrounding transaction.

## Dev-database cleanup privileges

Database-realm cleanup requires global `SELECT`, `DROP`, `ALTER`,
`ALTER ROUTINE`, `EVENT`, `LOCK TABLES`, and `PROCESS`. MySQL also requires
global `TRIGGER` and, on MySQL 8.0.20 and newer, `SHOW_ROUTINE`; MariaDB
requires global `SHOW VIEW`. Ptah verifies this privilege set before destructive
DDL. Cleanup fails closed when another user database contains a routine, event,
or trigger because its body can reference the cleanup realm without a catalog
dependency. Grant these privileges only to credentials used with a dedicated
disposable dev database.

## Making a column NOT NULL

A plan that makes an existing column `NOT NULL` writes `MODIFY COLUMN` with
the whole new definition. What the server does with a row that holds `NULL`
depends on the session's SQL mode:

- Under strict SQL mode, the default on both servers, the statement fails:
  MySQL answers error 1138 and MariaDB error 1265, and the row keeps its `NULL`.
- Without `STRICT_TRANS_TABLES`, both servers apply the statement and rewrite
  the `NULL` to the type's zero value, such as `0` or the empty string, with a
  warning. A `DEFAULT` in the same `MODIFY` is not what they write.

The safety report lists the statement as a warning for both reasons. Update
the `NULL` rows in a migration of their own first. A `MODIFY` that repeats
`NOT NULL` for a column that already has it, because its type or default
changed, is judged by that change instead.

## Online DDL for large tables

For large tables, `ptah migrations up` and `down` can route `ALTER TABLE`
statements through gh-ost or pt-online-schema-change, either per migration
with a `-- +ptah online_ddl_tool=ghost` directive or automatically above a
configured row-count threshold:

```yaml
online_ddl:
  tool: ghost
  threshold_rows: 1000000
```

A tool-routed migration runs on the tool's own connections and is not atomic:
keep online-DDL migrations minimal, ideally one `ALTER` per file. The
`online_ddl` keys, including `fallback` and `args`, are listed in
[Configuration](../../reference/configuration/).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- The `online_ddl` keys and every other configuration key: [Configuration](../../reference/configuration/).
