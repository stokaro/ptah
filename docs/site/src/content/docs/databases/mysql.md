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
  second. The rule is deliberately conservative and says so: `a` beside `ä` is
  accepted by both engines and is still reported as a possible conflict, and
  the way to make it exact is to resolve the names against the target rather
  than to guess a folding rule offline.
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
