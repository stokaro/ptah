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
- A foreign key may reference a table in another database, such as
  `REFERENCES crm.customers (id)` on a table of `shop`. A read of `shop`
  keeps the name `crm`: `db read` and SQL output write
  ``REFERENCES `crm`.`customers`(`id`)``, HCL output writes
  `table.crm.customers`, and a schema file declaring the key compares equal to
  the database. The description does not hold `crm.customers`, so Ptah cannot
  check its columns or its key, and the server checks them when the statement
  runs. An unqualified reference, or one to a database the description holds a
  table of, must still name a table the description holds. Where the read a
  command compares against covers `crm` too, as a URL naming no database does
  without `--schemas`, a desired schema that declares nothing of `crm` asks for
  its tables to be dropped; Ptah refuses a key into it by name rather than plan
  the drop.
- A nonunique referenced key must be a complete leftmost BTREE prefix.
  FULLTEXT, SPATIAL, HASH, parser-backed, expression, and prefix indexes do not
  qualify.
- A routine that states no security mode runs with its definer's rights, as
  the server applies to one created by hand: a declaration that omits
  `security` is `SQL SECURITY DEFINER`. Declare `security="INVOKER"` for a
  routine that runs with its caller's rights.
- A modified `SQL SECURITY DEFINER` routine is refused before migration SQL is
  planned when its catalog `DEFINER` differs from the connected
  `CURRENT_USER()`. Connect as that definer, change the desired routine to
  `SQL SECURITY INVOKER`, or leave the foreign routine unchanged. Missing
  ownership facts fail closed too. An unrelated `--exclude` selector does not
  change the connected account used for this check.
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
  on MySQL that MySQL immediately undoes. Spatial indexes report a bounding-box
  size in `STATISTICS.SUB_PART`. Ptah keeps that size out of column prefixes,
  so reading and planning an unchanged spatial index produces no migration.
- `USING BTREE` and `USING HASH` are read on a `PRIMARY KEY`, `KEY`, `INDEX`
  or `UNIQUE KEY` and rendered back after the column list. A `UNIQUE KEY`
  asking for a method is read as a unique index, which is what the server
  builds and where a method has somewhere to live. `BTREE` is not carried: `INDEX_TYPE` reports it for a
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
  A primary key is compared the same way; where its method differs, the plan
  drops the key and adds it with the method. Other dialects have no clause for
  a primary key's method, so a schema asking for one is refused there.
- A column-level `UNIQUE` is compared by the name the server gives it: the
  column's name, with `_2` and on where another key of the table holds it.
  When the database holds the key over the column under another name, the
  plan drops that key and adds the column's, as Atlas CE plans it. For a
  migration that names the key `c_x_uq` and a schema file that writes
  `x INT UNIQUE`, the plan drops `c_x_uq` and adds `x`. Where a key the plan
  drops holds the column's name, the plan drops it first, so the server gives
  the column's key that name. Where a foreign key stands on that key, the drop
  goes in the statement that gives the column its key, `ALTER TABLE t DROP
  INDEX x, MODIFY COLUMN x INT UNIQUE`, because `DROP INDEX` alone answers
  `ERROR 1553`, and the foreign key then stands on the column's key. A foreign
  key the column's key cannot serve, one over more columns, gets an index of
  its own in the statement that drops the old one, ahead of the column change.
  A plan that turns a `UNIQUE` into an index under the same name, as the
  rollback of such a column does, writes one statement as well: `ALTER TABLE t
  DROP INDEX x, ADD INDEX x (x)`. Both engines compare index names without case,
  so a key named `X` over `x` is the column's key; Atlas CE renames it to `x`,
  and Ptah keeps it, because the server does not tell the two names apart.
- A column-level `UNIQUE` accounts for one key over its column alone. Every
  other `UNIQUE` of the table is compared by its name: a second key over the
  column, and a key over more columns that the column leads. So
  `a INT UNIQUE, b INT, UNIQUE (a, b)` matches the keys `a` and `a_2` it
  builds, and a key the file no longer declares is dropped, as Atlas CE plans
  it. A key over the column that the file declares by name, as a constraint
  or as a unique index, stays that declaration's, and the column's own key is
  compared beside it. Both engines build `a INT UNIQUE` and
  `CONSTRAINT uq_a UNIQUE (a)` as two keys, so a database with `uq_a` alone
  is planned the key `a`, as Atlas CE plans it.
- A plan that drops a column with its own `UNIQUE` writes no `DROP INDEX` for
  that key. Both engines drop an index with the last of its columns, and
  MySQL 26.10 answers a later `DROP INDEX x` with `Can't DROP 'x'; check that
  column/key exists`. The rollback of a plan that gives a column its `UNIQUE`
  drops the key again, and a key the rollback adds back is added once, as a
  constraint, not through the column's `UNIQUE` as well.
- A plan that changes a column writes `MODIFY COLUMN` with the whole new
  definition, and writes `UNIQUE` in it only when the change gives the column
  its `UNIQUE`. Both engines read the clause as a request for a new key, so
  `MODIFY COLUMN x BIGINT UNIQUE` on a column that already has its key builds
  a second one, `x_2`. A column that keeps its `UNIQUE` while its type,
  nullability or default changes is written without the clause and keeps its
  one key, as Atlas CE writes it.
- An index's `COMMENT`, including a primary key's, is kept in `STATISTICS.INDEX_COMMENT` on both engines.
  SQL inspection preserves primary-key comments on both CLI surfaces. Applying
  the export to the same database keeps the key and comment.
  A changed secondary-index comment rebuilds the index in one statement: `ALTER TABLE t DROP INDEX k,
  ADD INDEX k (...) COMMENT '...'`. MySQL 8.4.11 refuses a `DROP INDEX` alone on
  an index a foreign key needs (`ERROR 1553`) and takes the pair. Atlas CE
  v1.3.0 writes the drop and the add as two statements. Primary-key replacements
  also use one `ALTER TABLE` so `AUTO_INCREMENT` and incoming foreign keys
  keep their supporting index.
- A column's `AUTO_INCREMENT` is compared, and a change is written as
  `MODIFY COLUMN`. A key column is restated `NOT NULL` without `PRIMARY KEY`,
  because both engines refuse a `MODIFY COLUMN ... PRIMARY KEY` on a table that
  has a key (`ERROR 1068`); the same holds for a key column whose type changes.
- `SERIAL`, `BIGSERIAL` and `SMALLSERIAL` are written as `INT`, `BIGINT` and
  `SMALLINT` with `AUTO_INCREMENT`, unless the field gives its own MySQL or
  MariaDB type. Both engines take one auto-increment column per table, and only
  as the first column of a key (`ERROR 1075`), so a serial column that breaks
  either rule is refused by name before anything runs.
- A primary key declared on its column and again as a `PRIMARY KEY`
  constraint over the same columns is one key, and the table is created with
  it once. Both engines refuse a `CREATE TABLE` that holds two primary keys
  (`ERROR 1068`). A new table holds its `PRIMARY KEY` constraint inside its
  `CREATE TABLE`, because both engines refuse an `AUTO_INCREMENT` column that
  no key of the same statement covers (`ERROR 1075`).
- A plain `KEY` that is the only key of an `AUTO_INCREMENT` column shares a
  statement with the column for the same reason. A new table holds it inside its
  `CREATE TABLE`. A column that comes or starts counting adds it in the same
  `ALTER TABLE`, and one that goes or stops counting drops it there.
- `KEY_BLOCK_SIZE [=] n` on a secondary, unique or primary index is retained
  by MariaDB. MySQL retains it only on a table with `ROW_FORMAT=COMPRESSED`.
  Ptah reads the hint from `SHOW CREATE TABLE` and compares it where the
  server keeps it. `ptah db read` preserves compression so replay retains the
  hint. Zero selects the engine default. Values above 65,535 on MariaDB or
  4,294,967,295 on MySQL are refused because the servers truncate them.
  Changing or removing only a stored secondary-index hint on MySQL requires
  `ALGORITHM=COPY`: an in-place drop and add of an unchanged key keeps the old
  hint. This can copy the whole table, and the safety classification says so.
  With `online_alter`, `LOCK=NONE` makes the server refuse that blocking
  operation. When the key, the uniqueness or the comment changes too, the one
  rebuild writes the new hint without a copy: measured on MySQL 8.4.11, an
  in-place rebuild that changes the definition stores it. MariaDB 11.8.9
  stores a new hint in place in every case. A primary key behaves the same
  way: replacing it for its hint alone asks MySQL for `ALGORITHM=COPY`, and a
  replacement that also changes the columns or the comment does not. The key
  is compared by its table, so a key declared as `CONSTRAINT pk_t PRIMARY
  KEY` matches the one MySQL names `PRIMARY`. An index or a key a YAML or HCL
  source declares has no hint, so applying one removes a stored hint. Other
  dialects refuse the hint.
- Go index annotations use `key_block_size="8"`. A table annotation can use
  `primary_key="id" primary_key_block_size="8" primary_key_comment="lookup"`;
  the hint needs the key declared with `primary_key`, since a field's
  `primary` writes a column key with no place for it, and is refused without
  one. A `PRIMARY KEY` constraint annotation uses `key_block_size` and
  `comment`. A `UNIQUE` or any other constraint annotation that writes
  `key_block_size` is refused when the source is read, as rendering refuses
  it: MySQL keeps a `UNIQUE` key's hint on the unique index it is, so declare
  a unique index with `unique="true"` instead. These attributes belong to the
  MySQL owner: a parse that does not select it refuses them as unknown. Atlas
  HCL output reports the block-size hint it cannot represent.
- An index the optimizer does not use is `INVISIBLE` on MySQL and `IGNORED` on
  MariaDB, and each engine answers `ERROR 1064` to the other's word. Ptah reads
  it from `STATISTICS.IS_VISIBLE` or `STATISTICS.IGNORED`, writes it in the
  target's word, and changes it in place with `ALTER TABLE t ALTER INDEX k
  INVISIBLE` or `IGNORED`. Neither engine hides a primary key (`ERROR 3522`,
  `ERROR 4174`), and the reader refuses one. CockroachDB has such an index,
  spelled `NOT VISIBLE`; see
  [Invisible indexes](../distributed/#invisible-indexes). A target without
  one, PostgreSQL among them, refuses an invisible index rather than build it
  visible. Atlas HCL has no attribute for it, so `schema inspect --format hcl`
  writes the index and reports on stderr that the document makes it visible.
- Two constraints on one table may share a name, and both engines accept
  `CONSTRAINT same UNIQUE (a)` beside `CONSTRAINT same FOREIGN KEY (a)`. Ptah
  identifies a named constraint by its type as well as its table and name, so
  both survive the read, the desired model, and the comparison. MySQL also lets
  a `CHECK` share a name with a `UNIQUE`, where MariaDB answers `ERROR 1826`.
- `NOT ENFORCED` after a `CHECK` is kept on MySQL 8.0.16+ and read back from
  `information_schema.TABLE_CONSTRAINTS`; MariaDB has no such clause. A foreign
  key's `MATCH FULL` or `MATCH PARTIAL` is kept on MySQL, which records it in
  `REFERENTIAL_CONSTRAINTS.MATCH_OPTION` and leaves it out of `SHOW CREATE
  TABLE`. MySQL 8.4.11 still checks such a key as `MATCH SIMPLE`. MariaDB
  11.8.9 accepts the clause and records `NONE`, so Ptah refuses it there
  rather than read a key the server does not build. A constraint whose
  enforcement or MATCH type changes is dropped and added again.
- A key part's direction is read back from the catalog, so `KEY (a DESC)` and
  `KEY (a)` are told apart rather than both arriving ascending. It also decides
  which index a foreign key owns: MySQL will not back one with a descending
  leading part and builds its own index instead, while MariaDB reuses whatever
  covers the columns. So a same-named index beside a covering one is the
  author's on MariaDB and the engine's on MySQL, and only the first is planned
  for removal.
- Each key part is compared whole: its column or expression, its prefix length
  and its direction, read from `information_schema.STATISTICS`. Neither engine
  changes a key part in place, so a changed part rebuilds the index in one
  `ALTER TABLE`, as a changed comment does.
- A key part may be an expression on MySQL -- `KEY ((a + 1))`, a functional key
  part -- and an unnamed one takes the name the server gives it,
  `functional_index`, then `functional_index_2` and `functional_index_3`. Two
  refusals go with it, and they are different facts rather than one rule:
  MariaDB has no functional key parts at all and answers `ERROR 1064` to every
  spelling, so the dialect decides, and Ptah refuses the index by name before
  the plan reaches the server; MySQL accepts them in an index and refuses
  one in a `PRIMARY KEY` with `ERROR 3756`, so that refusal holds on both
  engines. A functional part in a table-body `UNIQUE KEY` is read as a unique
  index rather than as a constraint, which is what the server builds: MySQL
  reports one index with `NON_UNIQUE=0`, a null column and the expression, and
  a constraint has nowhere to keep an expression.
- An inline `KEY`, `INDEX` or `UNIQUE KEY` the author did not name is read with
  the name its server would assign: the first key part's column, then `_2`,
  `_3` for a name already taken. A prefix length and a `DESC` direction stay
  out of the name. Names are taken in the order the body writes the keys, and
  a column's own `UNIQUE`, or the index its `REFERENCES` builds on MariaDB,
  takes its name at the column's place: `KEY (a), a INT UNIQUE` names the
  index `a` and the column's key `a_2`, and `a INT UNIQUE, KEY (a)` the other
  way round. The name is decided when the SQL is read rather than when it is
  written, because the catalog reports what the server chose and a desired
  schema that guessed differently would never converge with it.
- An unnamed `UNIQUE`, `KEY` or `INDEX` that `ALTER TABLE ... ADD` adds is
  named by the same rule, against every index the table holds at that point of
  the schema, the earlier files of a directory included. The index a foreign
  key built holds a name too: the key's own name, or its first column's for an
  unnamed key. The server drops that index when the added one covers the key,
  and the added one can then take its name, so `FOREIGN KEY (a) REFERENCES
  p(id)` followed by `ALTER TABLE c ADD UNIQUE (a)` leaves one index, `a`.
  MySQL does not let a descending leading part cover a foreign key and MariaDB
  does, so `ADD KEY (a DESC)` is `a_2` on MySQL and `a` on MariaDB.
- A foreign key the author did not name is read with the name the server gives
  it: `<table>_ibfk_<n>`. In `CREATE TABLE` the unnamed keys are numbered from 1
  in the order they are written, and a named key does not move the count. A key
  that `ALTER TABLE` adds takes one more than the highest `<table>_ibfk_<n>` the
  table held before the statement: a key the same statement drops still
  counts, and a key the statement adds under a name does not. The server names
  the index it builds for such a key after the key's first column, then `_2`
  and on, and that index is read as the key's, so a schema file compares equal
  to the database its own SQL built. A derived name that another foreign key of
  the database already holds is refused, because the server refuses it: MySQL
  answers `ERROR 1826` and MariaDB `ERROR 1005`. So is a derived name the
  server does not keep as it is. MySQL answers `ERROR 1059` to a name over 64
  characters. MariaDB through 12.0 answers it to a name of 64 characters in
  `CREATE TABLE`, and in `ALTER TABLE` cuts a longer name to 64 without a
  warning. Ptah refuses the cut name too, because the key would not have the
  name the file implies.
- On MariaDB a column-level `a INT REFERENCES p(id)` builds a key, numbered
  with the table's other unnamed keys in the order the body writes them: a
  table-level key written before the column takes the lower number. MySQL
  8.4 builds nothing from the clause, and Ptah refuses it under
  `--dialect mysql`.
- The name in `FOREIGN KEY idx (a) REFERENCES p(id)` names the key itself on
  MariaDB, and the key's index takes it. On MySQL it names only the key's
  index, and the key is `<table>_ibfk_<n>`.
- The index the server builds for a foreign key follows its own rule, on both
  engines, and Ptah reads a schema file by it. The server builds one only where
  no other index begins with the key's columns: not where the body declares
  such an index, and not for a key whose columns begin a longer key's, or match
  a later key's. The index outlives its key: `ALTER TABLE ... DROP FOREIGN KEY`
  and `DROP CONSTRAINT` leave it, and it keeps an index of the key's name that
  the author declared. And it gives way to any index added later that begins
  with its columns, which then takes its place and can take its name. The index
  a MySQL `FOREIGN KEY idx (a)` clause names is such an index, under that
  name. An index the author declared never gives way. So a file that drops a
  key, or adds a `UNIQUE` over a key's column, compares equal to the database
  it built.
- A plan that removes the last index a foreign key it keeps can stand on, whoever
  built it, gives the key an index of its own in the statement that drops the
  old one: `ALTER TABLE t DROP INDEX cover, ADD INDEX k (a)`. `DROP INDEX` alone
  answers `ERROR 1553`, and an index added first can make the server drop one it
  built by itself, so that a later `DROP INDEX` answers `ERROR 1091`. The new
  index takes the name the server gives the index it builds for that key.
- A plan that adds an index or a key the foreign key can stand on drops the
  index the server built for the key in the statement that adds it: `ALTER
  TABLE t DROP INDEX k, ADD INDEX i (a)`, and in the same way with `ADD
  CONSTRAINT u UNIQUE (a)`, with `MODIFY COLUMN a INT UNIQUE` and with `ADD
  PRIMARY KEY (a, id)`. Once the addition is there, the server drops the index
  it built by itself, also after `DROP FOREIGN KEY`, so a later `DROP INDEX k`
  answers `ERROR 1091`, and the rollback's removal of the addition answers
  `ERROR 1553` while a key the plan kept stands on it alone. An index the
  author named `k` stays, and the catalog does not tell the two apart, so the
  plan drops it either way. Where the plan makes several such additions, the
  first statement drops `k` and the others leave it out. For a key the plan
  removes, a new index comes after the key's drop and a new key comes before
  it; the rollback adds `k` back before it removes the addition. A descending
  index takes nothing over on MySQL, which stands no key on it, and takes the
  index over on MariaDB, which does.
- A foreign key that moves to another column under its name takes that name for
  its new index, so a plan drops the key, then the old index, then adds the key.
  With the old index in place `ADD CONSTRAINT` answers `ERROR 1061: Duplicate key
  name`. The same order serves a new key named after an index that leaves.
  Every other key the plan moves or removes on that table is dropped first as
  well, since the server answers the index's drop with `ERROR 1553` while one of
  them stands on it. Two keys moved onto one column end on one index under the
  second key's name, as the server builds a key an index of its own in place of
  one it built for another key over the same columns. A rollback drops both keys
  and rebuilds that index under its name in one statement.
  Where a key the plan keeps stands on that index, the statement that gives the
  kept key an index of its own takes the old one out at that point, and a rollback
  drops the moved key first, then rebuilds the index under its name in one
  statement. Where that statement takes out several indexes, each under the name
  of a key the plan moves or adds, it goes once, at the first of those keys.
- MariaDB 12.1 and later name an unnamed key `<n>` rather than
  `<table>_ibfk_<n>`, and the name belongs to the table rather than to the
  database. A schema file does not say which line it is for, so Ptah reads the
  older name and compares a key the server numbered with the key of the same
  definition. Two effects of that choice stay on the newer lines
  ([stokaro/ptah#3762](https://github.com/stokaro/ptah/issues/3762)). A
  document whose older name collides with another key, or is too long for the
  older lines, is refused on every line, although 12.1 and later accept it.
  And a key Ptah creates is written with the older name, so the database
  holds `c_ibfk_1` where the file run by hand would hold `1`; a later
  comparison matches either.
- A `CHECK` the author did not name is read with the name its server gives
  it. MySQL names it `<table>_chk_<n>`: in `CREATE TABLE` the unnamed checks
  are numbered from 1 in the order they are written, on a column or on the
  table, and a named check does not move the count. A check that
  `ALTER TABLE` adds takes one more than the highest `<table>_chk_<n>` the
  table holds once the statement's drops are made; a check the statement adds
  under a name does not count. A derived name that another check of the
  database already holds is refused, because MySQL refuses it with
  `ERROR 3822`, and so is a name past 64 characters, which MySQL refuses with
  `ERROR 1059`. MariaDB names a check written on a column after the column,
  and one written on the table `CONSTRAINT_<n>`, with the smallest number no
  check of the table holds, the names the same statement writes included.
  MariaDB takes one check on a column and answers `ERROR 1064` to a second,
  and Ptah refuses it the same way.
- A column `check` declared in YAML or a Go annotation without `check_name`
  is written without a name, and the comparison looks for the name the server
  gives it: `<table>_chk_<n>` on MySQL, numbered in column order among the
  unnamed column checks, and the column's own name on MariaDB. Measured on
  MySQL 8.4.11 and MariaDB 11.8.9, a table whose columns `a` and `b` each carry
  one holds `e_chk_1` and `e_chk_2` on MySQL, and `a` and `b` on MariaDB.
- A `CHECK` written on a column may name only that column on MySQL.
  `CREATE TABLE c (a int, b int CHECK (b > a))` is
  `ERROR 3813 (HY000): Column check constraint ... references other column`
  on MySQL 8.4 and 26.7, and so is the same check on a column that
  `ALTER TABLE` adds or modifies. Ptah refuses such a file when it reads it for
  MySQL, and the message says to write the check at table level. MariaDB
  accepts it.
- Both engines accept `CONSTRAINT` without a name before `PRIMARY KEY`,
  `UNIQUE`, `FOREIGN KEY` and `CHECK`, as in
  `CONSTRAINT FOREIGN KEY (p_id) REFERENCES p(id)`. Ptah reads such a clause as
  the same clause written without the keyword, which is how the server builds
  it: it takes the name that clause takes, so an unnamed foreign key counts in
  the same `<table>_ibfk_<n>` sequence as the other unnamed keys of its table,
  and an unnamed `CHECK` in the same sequence as the table's other checks.
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
  it, as the index's `parser` platform property of MySQL and MariaDB. `KEY` matters as much as `INDEX`: both dump tools normalize to it, so a
  table written with `FULLTEXT INDEX` comes back out of `mysqldump` as
  `FULLTEXT KEY`. An index left unnamed takes the name its server would give
  it, by the rule above.
- `KEY`, `SPATIAL`, `FULLTEXT` and `INDEX` open a table-level index here, and
  both engines reserve all four, so no column carries those names bare. Ptah
  reads the word as an index on this family and in dialect-neutral documents,
  which have no family to ask. A `.sql` source read for another dialect gets a
  column wherever that engine has no table element opening with the word.
  `key`, `spatial` and `fulltext` are columns on PostgreSQL, YugabyteDB,
  CockroachDB, ClickHouse, SQLite and Oracle. SQL Server reserves
  `key`, so only `spatial` and `fulltext` are columns there. `index` is a
  column on PostgreSQL and YugabyteDB, and on CockroachDB unless a
  column list follows it, directly or after a name. SQL Server reads `index` as
  its own inline index and ClickHouse as a data-skipping index, and SQLite and
  Oracle reserve it. All four are columns on an engine a provider serves,
  since no provider states a word that opens an index inside `CREATE TABLE`;
  the Spanner emulator and DuckDB take them so. Read as an index on the other
  dialects, `key text` would be refused, and `spatial nvarchar(32)` has the
  shape of an index named after its type over a column named `32`
  (stokaro/ptah#3089, stokaro/ptah#3299). An index
  names columns, so a column list that is a number is refused, and the refusal
  names the keyword. MySQL and MariaDB answer the same statement with
  `Error 1064`.
- Under `--dialect mysql`, an `ALTER TABLE` in a schema file carrying
  `IF EXISTS` or `IF NOT EXISTS` is refused, because MySQL refuses the guard on
  every operation. MariaDB takes it on `DROP INDEX`, `DROP KEY`,
  `DROP FOREIGN KEY`, `DROP CONSTRAINT`, `DROP COLUMN` and `ADD COLUMN`, and
  Ptah reads it there. MariaDB has no `DROP CHECK` at all, so under
  `--dialect mariadb` a schema file drops a table-level check with
  `DROP CONSTRAINT`. A column-level check is changed with `MODIFY COLUMN`:
  MariaDB refuses `DROP CONSTRAINT` for that check.
- A schema file's `USE app` selects the database the table names after it are
  in, as the server reads it, so a file for a whole server can write
  `USE app;` and then unqualified tables. The table a `REFERENCES` clause
  names without a database is looked up in the database of the table holding
  the key, which is where the server looks, whatever `USE` selected.
- A column carrying both a primary key and a `UNIQUE` is written back the way
  it was read, because the two spellings do not mean the same thing.
  `a INT UNIQUE, PRIMARY KEY (a)` builds the primary key and a secondary unique
  index named `a` on both engines, and it is rendered as the table-level key it
  was; `a INT PRIMARY KEY UNIQUE` builds both on MySQL and the primary key
  alone on MariaDB, and it is rendered inline so each engine gives its own
  answer. Folding the first into the second would lose MariaDB's second index.
- A role is a name and nothing else. `CREATE ROLE` takes no attribute on
  either engine, so a declared `login`, `password`, `superuser`, `createdb`,
  `createrole` or `replication` is refused before any SQL is written: what it
  asks for is a user, and Ptah does not manage users. A role always passes on
  the privileges of the roles granted to it, and there is no `NOINHERIT`
  (`Error 1064` on both engines). So every role reads back as inheriting,
  which is what a declaration defaults to, and a declared `inherit="false"` is
  refused rather than created as a role that inherits anyway.
- DDL commits implicitly on both engines, so a failed migration cannot be
  rolled back by the surrounding transaction.
- A schema file's column `COMMENT 'text'` and table `COMMENT = 'text'`, with
  or without the `=`, hold the text the server stores. A doubled quote and a
  backslash escape are read as the server reads them, and of two `COMMENT`
  clauses on one column the last is kept.

## Table options

A table's storage engine, the first value of its auto-increment column and its
default character set are the options `CREATE TABLE` takes as `ENGINE=`,
`AUTO_INCREMENT=` and `CHARSET=`. A Go annotation declares the engine with the
table's `engine` attribute and the others as platform properties of the
target:

```go
//ptah:schema:table name="orders" engine="InnoDB" platform.mysql.auto_increment="1000" platform.mysql.charset="utf8mb4" platform.mariadb.charset="utf8mb4"
type Order struct{}
```

YAML places `auto_increment` and `charset` in the target's platform group, and
an HCL table block takes `engine`, `auto_increment` and `charset` as
attributes, for MySQL and MariaDB alike. An engine or a character set that is
not a name of letters, digits and underscores, and an auto-increment value
that is not a whole number, is refused where it is written.

The options apply when the table is created. A read reports the character set
and neither the engine nor the next auto-increment value, so a plan never
changes the options of a table that exists. Another target leaves the
auto-increment value and the character set out, as it leaves out any other
target's properties.

## Column character set and ON UPDATE

A column's `CHARACTER SET` and its `ON UPDATE` clause, such as
`ON UPDATE CURRENT_TIMESTAMP(6)`, exist only in MySQL and MariaDB, so the MySQL
owner holds them rather than the common column. Each source binds them to that
family:

- SQL: `CHARACTER SET` or `CHARSET`, and `ON UPDATE`, in a column definition.
- Atlas HCL: the `charset` and `on_update` attributes of a `column` block.
- Go annotations and YAML: the platform properties `charset` and `on_update`,
  as `platform.mysql.charset="latin1"` on a `//ptah:schema:field` directive or
  under a column's `platform: {mysql: {...}}` group. The key is read for the
  target it names, so a MariaDB target needs `platform.mariadb`.

Another target leaves the settings out without a report, because the binding
says they are not meant for it. A value bound to no target is refused there.

The comparison does not compare these settings. A column whose only difference
is its character set or its `ON UPDATE` clause is not changed. A column that
changes for another reason is rewritten with `MODIFY COLUMN`, and that
statement writes the declared settings. A database read reports a character
set for every text column, inherited ones included, and an export writes what
it read.

## FULLTEXT parser

A FULLTEXT index's parser plugin, such as `ngram`, is an option of the MySQL
family, declared as the index's `parser` platform property. A Go annotation
and YAML state it for each target that should write it:

```go
//ptah:schema:index name="ft_bio" fields="bio" type="FULLTEXT" platform.mysql.parser="ngram" platform.mariadb.parser="ngram"
```

An HCL index block takes `parser`, as Atlas writes it, for MySQL and MariaDB
alike, and a schema file's `WITH PARSER` reads the same way. A parser that is
not a name of letters, digits and underscores is refused where it is written.
The parser applies when the index is created. A read does not report it, so a
plan never changes it on an index that exists, and another target leaves it
out.

## What the connecting account needs

`information_schema` lists only what the connecting account holds a privilege on,
and MySQL and MariaDB have no other catalog an ordinary account can read. A
comparison reads a missing row as a missing object, so a read that cannot prove it
saw a kind of object is refused rather than answered. The refusal comes before any
result, at exit 2, and names what is missing:

```text
the account reader@% cannot see everything in database "app": information_schema lists only what an account holds a privilege on, and a comparison reads a hidden object as absent. The account needs:
  - triggers: TRIGGER on `app`.*
  - procedures and functions: SHOW_ROUTINE on *.*
```

Every command that reads a schema gives this answer: `schema diff`, `schema
compare`, `schema drift`, `schema inspect`, `schema plan`, `schema apply`,
`migrations generate` and `db read`. A plan needs it as much as a report does: an
apply that believes an object is absent plans from that belief. `ptah-compat` gives
the same answer at exit 1, the one failure code its surface has.

The documents of `schema plan`, `schema apply` and `schema drift` report a refused
read of the target with the kind `permission_denied` and a `missing_privileges`
list, so a caller need not read the sentence.
[Why a command failed](../../reference/exit-codes/#why-a-command-failed) gives the
fields.

What each kind of object takes, measured on MySQL 8.4 and 26.10 and on MariaDB
11.8 and 12.3:

| Objects | MySQL | MariaDB |
| --- | --- | --- |
| Tables and views, with their columns and indexes | `SELECT`, `INSERT`, `UPDATE` or `REFERENCES` on the database or globally | The same |
| Primary keys, unique keys, foreign keys, `CHECK` constraints | Nothing more | A privilege beyond `SELECT` on the table or above it, such as `SHOW VIEW` |
| View definitions | `SELECT` and `SHOW VIEW` on the view or above it | The same |
| Triggers | `TRIGGER` on each table or above it | The same |
| Procedures and functions | `SHOW_ROUTINE` globally, or `SELECT` globally | `SHOW CREATE ROUTINE` on the database or globally, `SELECT` globally, or `SELECT` on `mysql.proc` |
| Events, which no comparison reads | `EVENT` on the database or globally, for a report to [name them](#events) | The same |

So this account gets the same comparison as `root`, roles and grants aside:

```sql
-- MySQL
GRANT SELECT, SHOW VIEW, TRIGGER ON app.* TO reader;
GRANT SHOW_ROUTINE ON *.* TO reader;

-- MariaDB
GRANT SELECT, SHOW VIEW, TRIGGER, SHOW CREATE ROUTINE ON app.* TO reader;
```

`TRIGGER` also lets the account create and drop triggers. Neither engine has a
privilege that only shows them.

Where a grant applies decides what it proves:

- **A grant on one table says nothing about the others.** The list of tables needs
  a privilege on the database or a global one, however many tables the account holds
  a grant on. `TRIGGER` and the MariaDB constraint privilege are read table by table
  once the list is complete, so a grant on every table counts.
- **A database-level grant is a pattern, and the most specific one applies.**
  ``GRANT TRIGGER ON `a%`.*`` beside ``GRANT SELECT ON `app`.*`` leaves the account
  with `SELECT` on `app` and no `TRIGGER`. An unescaped `_` is a wildcard, so a grant
  on ``my_app`` ranks below a grant on ``my\_app``.
- **`partial_revokes` changes both of those on MySQL.** With it on, a name in a grant is
  literal, so `%` and `_` match only themselves, and `REVOKE TRIGGER ON app.*` takes a
  global `TRIGGER` away on `app`.
- **A role counts while it is active**, through `SET DEFAULT ROLE` or
  `activate_all_roles_on_login`. Connections do not run `SET ROLE`, so a role that is
  granted and not set holds nothing here.
- **`ALL PRIVILEGES` on the database** shows everything on MariaDB 11.4 and newer. On
  MySQL and on MariaDB 10.11 it lists every routine and shows the body only of the
  account's own, as `EXECUTE`, `ALTER ROUTINE` and `CREATE ROUTINE` on the database do:
  a routine of another definer is refused by name until the account holds the routine
  privilege.
- **A URL that names no database** covers the databases the account can see, and each
  of them is held to this table. A database the account holds nothing on is outside the
  comparison, and two servers that differ only by one answer `Schemas are synced`.
  `SHOW DATABASES` lists every database, so an account that holds it has to see them
  all or name the ones it reads with `--schemas`.

### A run that does not depend on a kind

A selector that leaves out a whole kind relieves the read of answering for it, so
the account needs no privilege for it:

| Selector | The account does not need |
| --- | --- |
| `--exclude '*[type=trigger]'` | `TRIGGER` |
| `--exclude '*[type=view]'` | `SHOW VIEW` |
| `--exclude '*[type=function]'` | The routine privilege |
| `--exclude '*[type=constraint]'` | The MariaDB constraint privilege |
| `--exclude '*[type=table]'` | `TRIGGER` and the MariaDB constraint privilege |
| `--include '*[type=table]'` | `SHOW VIEW` and the routine privilege |

A selector that names objects does not count, because a read that cannot see a
trigger cannot say what it is called: `--exclude 'audit_*[type=trigger]'` still needs
`TRIGGER`. Neither does `--exclude '*[type=foreign_key]'`, since the catalog view that
hides a foreign key from a MariaDB account hides a primary key with it.

`diff.skip` relieves nothing. It leaves destructive changes out of a plan, and the
comparison still reads the kind to find what to add.

`schema apply` refuses a selector that matched nothing, and an account that cannot
see a trigger matches none with `--exclude '*[type=trigger]'`. Set
`PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1` to run it.

### A dev database

A dev or shadow database holds what the same run created there, and the account that
created a trigger or a routine is shown it. A read of one needs a privilege that
lists the columns and nothing else from the table above, on top of what
[emptying a dev database](#dev-database-cleanup-privileges) takes. A view or routine
that came back without a body is still refused, because it has another definer.

### Roles and grants

Reading roles and the grants on them needs `SELECT` on the `mysql` database, which
ordinary schema work does not. An account without it records them as not described
and is not refused, so a comparison decides nothing about them in either direction:
two databases that differ only by a grant to a role answer `Schemas are synced` for
that account. An account that compares grants needs `SELECT` on `mysql.*`.

### Events

A read reports an event rather than refuses over one, because Ptah does not model
it. No schema source can declare an event: a SQL file that says `CREATE EVENT` fails
with `unsupported CREATE target: EVENT`, and a dev database replay refuses it, since
what an event does on its schedule cannot be confined to that database. So no
comparison reads events and no plan creates, changes or drops one. `schema apply`
leaves them where they are.

An event made by hand is therefore not a difference, and a report that found nothing
else says so rather than claiming the database matches. `drift`, `failed`, the
findings and the exit code stay what they would be without it:

```text
No schema drift found, but 1 event was not compared.
Failure threshold: all. Failing: false.
Database: mysql://root:***@db/app

Not compared:
- event "nightly_cleanup": Ptah does not model events, so nothing compares or plans this one
```

The JSON report carries the same objects under `not_compared`, each with a `kind`, a
`name` and a `reason`, and the `schema` it is in when the URL names no database.
`github-actions` annotates each one as a warning. `schema compare` opens with
`No schema differences detected, but 1 event was not compared:` over the same list,
and `schema serve` lists them under `Not compared`. `schema diff`, `schema inspect`,
`db read`, `schema apply`, `schema plan` and `migrations plan` keep their output as it
is and name them in a warning on standard error. `migrations generate` writes files
for what differs and does not name them.

What a report says depends on what the read can list. `schema drift`, `schema compare`
and `schema serve` claim to have compared, so they say when the read could not list
events. The verbs that warn on standard error claim nothing about them, so they warn
only about an event the read saw, and an account without `EVENT` runs them without
this warning:

| Read | `schema drift` | The verbs that warn on standard error |
| --- | --- | --- |
| The account holds `EVENT` and the database has none | `No schema drift detected.` | Nothing |
| The account holds `EVENT` and the database has some | Names each one under `Not compared` | Names each one |
| The account holds no `EVENT` on the database | An entry with no `name`, whose `reason` asks for the privilege | Nothing |
| The server refuses the catalog | An entry with no `name`, whose `reason` gives the server's answer | Nothing |

`information_schema.EVENTS` lists a database's events only to an account holding
`EVENT` on it, at the database level, on a pattern or globally. Measured on MySQL
8.4 and 26.10 and on MariaDB 11.8 and 12.3, every other privilege answers no rows
and no error, including `SELECT` on `mysql.*`, so an empty answer alone says nothing.
Nothing the comparison does depends on the answer, so that account is not refused.
MariaDB with `--event-scheduler=DISABLED` refuses the query for every account with
error 1577, and MySQL answers it.

To have a report name the events, add `GRANT EVENT ON app.* TO reader`. `EVENT` also
lets the account create and drop events, and neither engine has a privilege that
only shows them. A run that does not depend on them can say so with
`--exclude '*[type=event]'`, as for any other kind, and nothing reads or reports
events then. This whole-kind selector does not warn that it matched no objects,
even when the account cannot list events. A selector naming one event, such as
`--exclude 'e_tick[type=event]'`, counts as a match and leaves the report as it
is: the database still holds the event, and no selection changes that.

## Dev-database cleanup privileges

Database-realm cleanup requires global `SELECT`, `DROP`, `ALTER`,
`ALTER ROUTINE`, `EVENT`, `LOCK TABLES`, and `PROCESS`. MySQL also requires
global `TRIGGER` and, on MySQL 8.0.20 and newer, `SHOW_ROUTINE`; MariaDB
requires global `SHOW VIEW`. Ptah verifies this privilege set before destructive
DDL. Cleanup fails closed when another user database contains a routine, event,
or trigger because its body can reference the cleanup realm without a catalog
dependency. Grant these privileges only to credentials used with a dedicated
disposable dev database.

When MariaDB runs with `event_scheduler=DISABLED`, `db drop-all --dry-run`
reads event names from `mysql.event` if `information_schema.EVENTS` refuses the
query. Existing events remain in the plan. MariaDB also refuses `DROP EVENT`
while the scheduler is disabled, so executing that plan requires a server that
can drop the events.

A dev server, a `--dev-url` that names no database, is emptied database by
database and needs none of these. The run found no user database there before
it replayed, so it drops only the databases the session sees, which are the
ones it created; see
[A whole dev server](../../atlas/migrate-commands/#a-whole-dev-server).

## Dropping a table read by a view

MySQL accepts `DROP TABLE` when a view still reads the table. The view stays in
the catalog, but querying it fails. When a plan drops a table that a declared
view still reads, the generated SQL warns and names the view. The warning does
not block apply. Remove or replace the view, or keep the table, before applying
the plan. A view the plan removes is dropped before the table. The warning
cannot identify a view absent from the declaration and hidden from the read.

## Replacing a trigger

A plan runs every trigger change in one block, after the columns a new trigger
body uses are added and before the columns an old body reads are dropped. Both
servers accept a trigger whose body writes a column that does not exist yet,
and a column dropped while a trigger reads it through `NEW`; in both cases
every write to the table fails with error 1054 until the plan moves on.

MySQL has no `CREATE OR REPLACE TRIGGER`, so a changed trigger is replaced with
`DROP TRIGGER` and `CREATE TRIGGER`. A renamed trigger is created before the
old one is dropped, so a body the server refuses leaves the old trigger in
place. A write between two such statements sees no trigger or both of them, so
when the block both removes or replaces a trigger and creates one on a table,
it runs under `LOCK TABLES ... WRITE` and the write waits instead:

```sql
ALTER TABLE `secondtable` ADD COLUMN `NewColumn` VARCHAR(100);
-- Writes to these tables wait until their triggers are replaced
LOCK TABLES `mytable` WRITE;
CREATE TRIGGER `mytable_after_insert_v2` AFTER INSERT ON `mytable` FOR EACH ROW
  INSERT INTO secondtable (tbl_time, NewColumn) VALUES (NOW(), NEW.name);
DROP TRIGGER IF EXISTS `mytable_after_insert_v1`;
UNLOCK TABLES;
```

MariaDB replaces a trigger that keeps its name with one
`CREATE OR REPLACE TRIGGER`, which keeps the old trigger when the server
refuses the new body and takes no lock. A rename takes the lock there too.

The plan check rates the `DROP TRIGGER` of a trigger the block creates again as
the change it is, a warning, so `--check-destructive` passes a changed
trigger. The `DROP TRIGGER` of a renamed trigger's old name removes that
trigger, and is destructive.

- The statements run in order on one session, as `migrations up`,
  `schema apply` and `ptah-compat migrate apply` run them. A program that runs
  planned statements on a connection pool can leave a statement waiting behind
  the lock another connection holds.
- The migration account needs the `LOCK TABLES` privilege.
- `LOCK TABLES` waits for a metadata lock while another transaction still
  holds the table, and later writes queue behind it. A migration's
  `lock_timeout` bounds that wait, as it bounds every DDL statement's, and the
  migration then stops before it touches a trigger. Without one the wait lasts
  as long as the server's `lock_wait_timeout`: a year on MySQL and a day on
  MariaDB.
- The migrator refuses `CREATE TRIGGER` and `LOCK TABLES` inside a migration
  transaction. `migrations generate` writes such a file with
  `-- +ptah no_transaction` and `ptah-compat migrate diff` with
  `-- atlas:txmode none`, so it applies with the default `--tx-mode`. A file
  written by hand needs one of them, or `--tx-mode none`.
- When MySQL refuses the new body of a trigger it replaces by name, the old
  trigger is already gone, the migration is recorded dirty, and the lock ends
  with the session. `migrations up --allow-dirty` takes the lock again before
  it retries the failed statement.
- A dev-database replay runs the lock too, and refuses a lock on a table in
  another database unless the dev server belongs to the run.
- `schema apply --dev-url` rehearses a view or trigger body whose columns are
  qualified with a table or alias the body reads, as in
  `SELECT u.id FROM users u`, and a trigger body's `NEW.col` and `OLD.col`.
  Where a table name is read, as in `INSERT INTO new.audit`, a qualifier names
  a database, and one other than the target's refuses the rehearsal. So do a
  routine call such as `other.f()`, a sequence such as `NEXTVAL(other.s)` and
  a three-part path such as `other.t.v`.

## A column's fractional seconds

`timestamp`, `datetime` and `time` hold no fractional-second digits where a column
declares none, on both servers. So `datetime` and `datetime(0)` are one type and plan
nothing, and `datetime(6)` declared `datetime` plans a `MODIFY COLUMN` that rounds the
digits away. The plan rates that destructive. See
[how a column change is rated](../../direct/plan-and-approve/#how-a-column-change-is-rated).

## A column's size

`decimal` and `numeric` are `decimal(10,0)` where a column declares no precision, on
both servers. So `decimal` and `decimal(10,0)` are one type and plan nothing, and
`decimal(10,2)` declared `decimal` plans a `MODIFY COLUMN` that rounds `1.50` to `2`,
rated destructive. `char`, `binary` and `bit` are the same types with a length of one.
`float(p)` is `float` up to a `p` of 24 and `double` above it, and `real` is `double`
unless `REAL_AS_FLOAT` is set.

`text` and `blob` hold 65535 bytes, the `tiny` types 255, the `medium` types 16 MB and the
`long` types 4 GB, so `longtext` declared `text` is rated destructive, and in strict mode
the server refuses a value that does not fit. `text(n)` and `blob(n)` read as the type the
server builds for them, so `blob(100)` and `tinyblob` are one type, and `text(60)` and
`tinytext` are one type. `text(n)` and a `varchar(n)` under a text type are read at four
bytes a character, which is utf8mb4. `double` declared `float` reads
`1.23456789012` back as `1.23457`, without a word, and is rated destructive. So are a lost
sign (`int` declared `int unsigned`), half a range lost (`int unsigned` declared `int`),
and a member an `enum` or a `set` stops naming.

On MariaDB a `json` column is a `longtext` with `CHECK (json_valid(c))` under the column's
own name, and a column declared `json` plans nothing against that shape. A `longtext` with
no such `CHECK` plans the `ADD CONSTRAINT`, and any other type plans a `MODIFY COLUMN` that
writes the `CHECK` with the type. Declaring a checked `int` as plain `int`, or a `json`
as plain `longtext`, removes the column's check with `MODIFY COLUMN`.
The plan rates removal or replacement of a column check as destructive because
the new definition may stop enforcing the old condition.

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
