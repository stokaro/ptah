---
title: MySQL and MariaDB
description: MySQL and MariaDB in Ptah - the dialect differences that reach generated SQL, the foreign-key and routine rules, TLS with a private CA, cleanup privileges, and online-DDL routing.
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
  change the connected account used for this check. A view of another definer
  is refused the same way; see [View security](#view-security).
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
  writes the drop and the add as two statements. Primary-key replacements
  also use one `ALTER TABLE` so `AUTO_INCREMENT` and incoming foreign keys
  keep their supporting index.
- A column's `AUTO_INCREMENT` is compared, and a change is written as
  `MODIFY COLUMN`. A key column is restated `NOT NULL` without `PRIMARY KEY`,
  because both engines refuse a `MODIFY COLUMN ... PRIMARY KEY` on a table that
  has a key (`ERROR 1068`); the same holds for a key column whose type changes.
- An expression default is compared as the operations its operators group,
  by the precedence both engines share, and read back as an expression on
  every column type. MySQL 26.10 stores `(2 * 3 + 1)` as ``((2 * 3) + 1)``,
  `(a + b * 2)` as ``(`a` + (`b` * 2))``, `(CURRENT_TIMESTAMP + INTERVAL 1
  DAY)` as `(now() + interval 1 day)` and `(concat('a', 'b'))` as
  `concat(_utf8mb4'a',_utf8mb4'b')`. MariaDB 12.3 writes `7 % 4` as
  `7 MOD 4` and `NOW()` as `current_timestamp()`. Each pair is one default,
  so a second apply plans nothing, and `db read` writes the default in
  parentheses. Only a current-time function on a `DATETIME` or `TIMESTAMP`
  column stays bare: MySQL 8.4 and 26.10 refuse `DEFAULT uuid()` with
  `ERROR 1064` and `DEFAULT now()` on a `VARCHAR` with `ERROR 1067`. A
  `default_expr` in a Go struct or a YAML schema is written by the same rule,
  so `default_expr="1 + 2"` renders `DEFAULT (1 + 2)`. A
  parenthesis that changes the grouping is part of the default:
  `((1 + 2) * 3)` and `(1 + 2 * 3)` differ. `NOT`, `IS`, `IN`, `BETWEEN`,
  `LIKE` and `REGEXP` are read the way the server writes them back: MySQL
  stores `(NOT 1 + 1)` as `(0 = (1 + 1))`, and the two are one default. An
  expression that holds
  `CASE`, `LIKE ... ESCAPE`, `IS UNKNOWN` or a qualified name is compared as
  written.
- A string default is not an expression default, even where its characters
  spell the expression. `DEFAULT '1 + 2'` stores five characters and
  `DEFAULT (1 + 2)` stores 3, so a plan between the two changes the column in
  either direction, and the same holds for `'now()'` and `(now())`. MySQL
  reports both of those as `now()` and tells them apart by
  `DEFAULT_GENERATED` in `EXTRA`, and `db read` writes the string in quotes.
  A `default` in a Go struct or a YAML schema is such a string, so
  `default="1 + 2"` against a column that holds `(1 + 2)` changes it too.
- A date or time default of `'now'`, `'today'`, `'tomorrow'` or
  `'yesterday'` is sent as written. Ptah refuses one only on the PostgreSQL
  family, where the server stores the moment it creates the table (see
  [PostgreSQL](../postgresql/#how-a-declaration-is-compared-with-what-the-server-stored)).
  MySQL 26.10 and MariaDB 12.3 refuse `DATETIME DEFAULT 'now'` themselves,
  with `ERROR 1067`.
- `DEFAULT ''` and no default are different defaults, and so are
  `DEFAULT 'NULL'` and `DEFAULT NULL`. MySQL reports the string `NULL`
  without quotes and SQL NULL as no value, and the empty string as an empty
  value. `db read` writes each as the string it is.
- `db read` writes a backslash in a MySQL string default doubled, so
  `DEFAULT 'a\\b'` replays as the three characters it stores, and a control
  character as its escape, so `DEFAULT 'a\nb'` replays with its line feed.
  A quote escaped with a backslash and a doubled quote compare as one
  character. The catalog reports a default the same way under every
  `sql_mode`, so a read in a session whose `sql_mode` names
  `NO_BACKSLASH_ESCAPES` writes the same file.
- A schema file may write a default as a bit string (`b'101'`, `0b101`), a
  hexadecimal string (`X'6A'`, `0x6a`), a string with a character set
  introducer (`_utf8mb4'x'`) or `N`, adjacent strings (`'a' 'b'`) or a
  double-quoted string (`"a"`). Each is read as the literal the server
  stores. On a `BINARY`, `VARBINARY` or `BLOB` column a string and the
  hexadecimal string of its bytes are one default, since MySQL reports
  `DEFAULT 'xy'` as `0x7879` and MariaDB `DEFAULT ('xy')` as `x'7879'`.
  On a text column a hexadecimal or bit string is the string of its bytes,
  so `0x41`, `b'1000001'` and `'A'` are one default. On an integer column
  `0x1F` and the `31` the server reports are one default, and so are
  `b'101'` and `5`; on a `BIT` column `0b110` and `b'110'` are one default.
  Both engines take `ucase` and `lcase` for `upper` and `lower`, and MariaDB
  prints `upper` and `lower` as `ucase` and `lcase`. The comparison reads a
  call to either name as a call to `upper` or `lower`, and `db read` writes
  `upper` and `lower`.
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
  index takes the name the server gives the index it builds for that key. A key
  an exclude selector keeps out of scope gets its index the same way, with a
  comment above the statement that names the key; the selector leaves the new
  index out with the key, so the next run plans nothing.
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
- A `CHECK` name the declaration writes is held where the server holds it.
  MySQL holds it once per schema, across its tables, and compares it without
  case but with accents: `ck` and `CK` are one name, `ek` and `ék` two.
  MariaDB holds it once per table. Ptah folds ASCII letters on both, and on
  MySQL also the letters the server was measured to fold: Latin-1, Latin
  Extended-A and part of Latin Extended-B, basic Greek, Cyrillic and Armenian capitals, the polytonic Greek
  block, the Kelvin and Ångström signs, Roman numerals, and the circled and
  fullwidth capitals. It never refuses a pair the server accepts, such as `ẞ`
  and `ß`. A pair the server refuses that Ptah does not recognize, such as
  `Ḁ` and `ḁ` on MySQL or `é` and `É` on MariaDB, reaches the server, which
  refuses it; MySQL DDL is not transactional, so the statements before it stay
  applied. Two `CHECK` constraints of one name are
  refused before any statement runs, and the refusal ends with the server's
  own error, such as `Error 3822 (HY000): Duplicate check constraint name
  'CK'.` on MySQL and `Error 1826 (HY000): Duplicate CHECK constraint name
  'DD'` on MariaDB. Against a live server the scope is that of the server that
  answers, so a `mysql://` URL that reaches MariaDB holds a name per table.
  Measured on MySQL 26.10 and MariaDB 12.3.
- A column `check` declared in YAML or a Go annotation without `check_name`
  is written without a name, and the comparison looks for the name the server
  gives it: `<table>_chk_<n>` on MySQL, numbered in column order among the
  unnamed column checks, and the column's own name on MariaDB. Measured on
  MySQL 8.4.11 and MariaDB 11.8.9, a table whose columns `a` and `b` each carry
  one holds `e_chk_1` and `e_chk_2` on MySQL, and `a` and `b` on MariaDB.
- A `CHECK` condition and the expression of a functional index are read as
  SQL. MySQL's `information_schema` reports both escaped once more, as the body
  of a string: for `CHECK (c <> 'it''s')` it holds
  ``(`c` <> _utf8mb4\'it\\\'s\')``. Ptah takes that escaping off and reads
  ``(`c` <> _utf8mb4'it\'s')``, the condition `SHOW CREATE TABLE` prints.
  MariaDB reports the condition as SQL, `` `c` <> 'it\'s' ``, and Ptah reads it
  as reported. The comparison reads each string by the value it stands for, so
  `'it''s'`, `'it\'s'` and `"it's"` are one string, and so are `'x\ty'` and the
  same string holding a tab. A generated column's expression and an expression
  default are read the same way.
- MySQL's `information_schema` also reports a character beyond ASCII in these
  expressions encoded once more: each byte of its UTF-8 form arrives as a
  character of its own, so `CHECK (c <> 'é')` holds
  ``(`c` <> _utf8mb4\'Ã©\')``. Ptah takes that layer off where what is left is
  UTF-8, and keeps the text as reported where it is not. A string written in
  another character set, such as `_latin1 x'e9'`, is reported with one layer
  and read as reported. MariaDB reports each character as it is, apart from
  the `CHECK_CLAUSE` case below.
- A `CHECK` condition and a generated column's expression are compared as
  written first, and where the text differs, as the operations their
  operators group, by the precedence an expression default is read with.
  MySQL 26.10 stores `CHECK (id between 1 and 10 and c is not null)` as
  ``((`id` between 1 and 10) and (`c` is not null))``, `CHECK (NOT a OR id >
  0)` as ``((0 = `a`) or (`id` > 0))``, `CHECK (c NOT LIKE 'x%')` as
  ``(not((`c` like _utf8mb4'x%')))`` and `CHECK (b <> X'41')` as
  ``(`b` <> 0x41)``. MariaDB 12.3 writes `NOT a` as `` `a` = 0 `` and keeps
  `X'41'` compared with a `varbinary` column. Each pair is one condition, so a
  second apply plans nothing, and a
  changed value is still a change. A condition holding `CASE` or
  `LIKE ... ESCAPE` is compared as written, and so is one the server rewrites
  further: MariaDB stores `CHECK (NOT TRUE)` as `0`. Each of these plans the
  condition again on every run.
- MariaDB converts a hexadecimal, bit, national or other character set string
  in a `CHECK` to the character set of the column it is compared with, and
  stores the string that results. Measured on MariaDB 12.3 with a `utf8mb4`
  `varchar` column, `x <> x'41'`, `x <> 0x41` and `x <> b'1000001'` are stored
  as `` `x` <> 'A' ``, and `x <> N'abc'` as `` `x` <> 'abc' ``. The stored
  condition compares by the column's collation rather than by bytes: under
  `utf8mb4_uca1400_ai_ci` it refuses the value `'a'`, which `'a' <> x'41'` in a
  query accepts. The declaration and the condition the server enforces differ,
  and Ptah cannot tell a declared `x'41'` from a declared `'A'` in what the
  server reports. It compares such a `CHECK` as written, so the `CHECK` is
  planned again on every run. Write the string the server stores, `x <> 'A'`,
  and the `CHECK` converges and states the condition the server enforces.
- MariaDB reports `CHECK_CLAUSE` in `utf8mb3`, so a character beyond U+FFFF
  reads as one `?` per byte of its UTF-8 form, while `SHOW CREATE TABLE`
  prints it whole. A `CHECK` holding such a character, such as U+1F600, is
  compared as reported and planned again on every run.
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
- A Go or YAML literal default, a column, table or index comment, an enum
  value and a declared row's value are written with each quote doubled. Under
  the default `sql_mode` each backslash is doubled too, so `a\b` stores those
  three characters. A text default is compared by the string it stores, so
  `'it\'s'` and `'it''s'` are one default. A literal `NULL` on a text column is
  the string `'NULL'`; a schema file writes SQL `NULL` without quotes.
- A session whose `sql_mode` names `NO_BACKSLASH_ESCAPES` reads a backslash in
  a string as the character, so `'a\\b'` stores two backslashes there. Ptah
  reads the mode once, when it connects, and writes the literals above for the
  session that runs them, with each backslash single on such a session:
  `schema apply`, its declared rows, `--dry-run`, and what a dev database
  builds for `schema diff` and for the `--dev-url` rehearsal. Text Ptah writes
  for later is written for the default `sql_mode` whatever the session's: a
  migration file from `migrations generate`, `migrations data`,
  `migrations checkpoint` or `ptah-compat migrate diff`, a saved plan file, and
  the SQL that `schema diff`, `migrations plan`, `schema compare`, `db read` and
  `schema inspect` print.
- SQL a user wrote is sent byte for byte: a view or routine body from a schema
  file, the plan's SQL after `--edit`, and a migration file. `schema apply`
  cuts the `--edit` text into statements the way the session reads it, so the
  prompt rates each statement the session receives. `migrations up` sends a
  migration file as written and cuts it the way the default `sql_mode` reads
  it, so a file Ptah generated stores each such backslash twice on a
  `NO_BACKSLASH_ESCAPES` session. `migrations up` on such a session also needs
  `--tx-mode none`; see [Apply migrations](../../versioned/apply/).
- Two refusals keep text off a session that reads it differently, before
  anything runs. `schema apply --plan` refuses a saved plan that holds a
  backslash on a `NO_BACKSLASH_ESCAPES` session; apply the desired schema
  without `--plan` there. The `--dev-url` rehearsal refuses a plan that holds a
  backslash when the dev session and the target session disagree on
  `NO_BACKSLASH_ESCAPES`; give the dev database the target's `sql_mode`. A plan
  without a backslash runs on either session. With `--json`, both refusals end
  the run as `refused` with the code `backslash-mode`.
- `ptah-compat` writes these literals the same way, so on a
  `NO_BACKSLASH_ESCAPES` session it stores the declared value. The community
  binary stores four backslashes where the declaration holds one (measured
  with the community binary v1.3.0 and v1.4.0). We are stricter here,
  deliberately: that spelling changes the data the user declared; see
  [Compatibility differences](../../atlas/retained-divergences/#a-backslash-on-a-no_backslash_escapes-session).

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

The engine and the auto-increment value apply when the table is created. A
read reports neither, so a plan never changes them on a table that exists. A
read reports the table's default collation, which names its character set, so
a plan changes the default character set of a table that exists through its
collation; see [Column collations](#column-collations). Another target leaves
the auto-increment value and the character set out, as it leaves out any other
target's properties.

A description of a read keeps the table's character set and collation, so SQL
written from it creates the table as the server holds it. In HCL a table
leaves out the ones its schema block states, as the Atlas community binary
writes them; SQL writes both on every table, and JSON lists both on every
table.

## Table partitioning

Ptah does not model MySQL or MariaDB partitioning
([stokaro/ptah#6357](https://github.com/stokaro/ptah/issues/6357)): nothing
renders or compares partitions. A declaration that partitions a table is
refused by name rather than created as one ordinary table, or compared as
synced with a table that has no partitions:

- `PARTITION BY` in a `CREATE TABLE` of a SQL schema file read for MySQL or
  MariaDB, and an `ALTER TABLE` that opens with a partition operation, such as
  `PARTITION BY`, `ADD PARTITION` or `REMOVE PARTITIONING`;
- the `partition_by` platform property of a Go annotation or a YAML table;
- an HCL table's `partition` block.

```text illustration
unsupported feature: PARTITION BY on table sales: Ptah does not model MySQL or MariaDB partitioning (stokaro/ptah#6357); leave it out of the desired schema and manage the partitions outside Ptah
```

A table partitioned outside Ptah is read and compared without its partitions,
so a schema that declares it without them reports synced. The read names the
table rather than let that read as covering the partitions, in the same places
it names an [event](#events): `schema compare` lists it under the report, and
`db read`, `schema apply` and `schema diff` warn on standard error:

```text illustration
warning: table partitioning "sales" in the database is left out: Ptah does not model MySQL or MariaDB partitioning (stokaro/ptah#6357), so no comparison sees the partitions, and SQL written from this read creates the table without them
```

The read does not refuse the table. A change Ptah plans for it, such as
`ADD COLUMN`, keeps its partitions, because the server keeps them through the
change. SQL that `db read` writes from the read creates the table without
partitions. A run whose `--exclude` or `--include` leaves the table out does
not name it, since the run is not about that table.

`ptah-compat` refuses the same declarations. The community binary writes a SQL
file's clause as an option the server refuses with ERROR 1064, reports a table
that exists without partitions as synced, and fails to parse a MySQL HCL
table's `partition` block. We are stricter here, deliberately: a declaration Ptah cannot
carry out is refused rather than lost.

## A column default with an operator

MySQL and MariaDB take an expression default only in parentheses. MySQL 26.10
and MariaDB 12.3 answer ERROR 1064 (42000) to `DEFAULT 1 + 2`, `DEFAULT 5 - 2
NOT NULL` and `DEFAULT (1) + 2`, and take `DEFAULT (1 + 2)`. A SQL schema file
read with `--dialect mysql` or `--dialect mariadb` is refused at the operator,
before anything is planned, and the refusal names the parenthesized form,
`write DEFAULT (1 + 2)`. A version gate after the default, such as
`/*!80023 INVISIBLE */`, stays the column's.

Both engines read `||` as `OR` under their default SQL mode and as a
concatenation only under `PIPES_AS_CONCAT`. MySQL 8.4 to 26.10 store
`DEFAULT ('a' || 'b')` as `((0 <> 'a') or (0 <> 'b'))` and answer
ERROR 1292 (22007) to the INSERT that takes it; MariaDB 11.8 to 12.3 store
`('a' or 'b')` and insert 0. A schema file read with either dialect is
refused at the first `||`, in parentheses or without, and the refusal writes
the default with a call that concatenates under every mode where the
operands stand: `write DEFAULT (CONCAT('a', 'b'))`, and
`write DEFAULT (coalesce(CONCAT('a', 'b'), 'c'))` for
`(coalesce('a' || 'b', 'c'))`. Write `OR` where a logical or is meant.

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

## Column collations

A column's `COLLATE` is written on the column, in `CREATE TABLE` and in
`MODIFY COLUMN`, and compared with the collation the column holds. A read
reports that collation whether the column declares it or takes it from its
table, so `COLLATE utf8mb4_unicode_ci` on a column of a table whose default is
`utf8mb4_unicode_ci` is no change. Names compare ignoring case, and
`COLLATE 'utf8mb4_bin'` is the same name as `COLLATE utf8mb4_bin`. A changed
collation is planned as `MODIFY COLUMN`, which restates the column with the
declared collation and its declared character set.

A name the server stores under another name is compared as the stored name.
MySQL stores a collation of the `utf8` character set under `utf8mb3`, so
`COLLATE utf8_bin` holds as `utf8mb3_bin`. MariaDB does the same while its
`old_mode` holds `UTF8_IS_UTF8MB3`, which is the default, and stores it under
`utf8mb4` when `old_mode` leaves that flag out. Ptah asks the server what
`utf8` names in the session it reads with, so a URL that sets `old_mode`, such
as `mariadb://user:pass@host:3306/app?old_mode=%27%27`, is compared the way the
server stores what that session writes.

MariaDB also takes a collation without its character set, so
`COLLATE uca1400_ai_ci` on a `utf8mb4` column holds as `utf8mb4_uca1400_ai_ci`,
and `COLLATE DEFAULT`, which selects the
default collation of the column's character set and is compared as no
collation. Both servers accept a collation on a type that holds none, such as
`int COLLATE utf8mb4_bin`, and keep none, so such a collation is not compared.

A column that declares no collation takes one from the server: the default
collation of a character set the column names, or else its table's default
collation. `CHARACTER SET utf8mb4` in a table whose default is
`utf8mb4_unicode_ci` gives `utf8mb4_0900_ai_ci` on MySQL 8.4 and
`utf8mb4_uca1400_ai_ci` on MariaDB 12.3, and `CHARACTER SET latin1` gives
`latin1_swedish_ci` on both. Ptah reads each character set's default from the
server it compares with, in the session it reads with: MySQL's
`default_collation_for_utf8mb4` and MariaDB's `character_set_collations` are
honored.

The column is compared with the collation it takes, and one that holds
another is reset with a `MODIFY COLUMN` that names no collation, so the server
resolves it the same way. A column that names no character set is not compared
where the declaration names neither a character set nor a collation for its
table, since the database's default decides it. Nor is a column whose type
carries an attribute that selects a collation of its own, such as
`varchar(20) BINARY`.

A table's default collation is compared too, where the declaration names it or
names the table's character set. A changed one is planned in one statement
with the reset of each column that takes it,
``ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `n` varchar(20)``,
which names the character set too where it changes:
``ALTER TABLE `t` CHARSET latin1, COLLATE latin1_bin, MODIFY COLUMN `n` varchar(20)``.
The table option alone leaves the collation of every existing column as it
is. In the same statement the server gives a reset column the new default, so
one apply converges. A change of a table's default collation is rated a
warning, because a column added or restated without a collation takes it.

The pinned Atlas community binary plans the same statement when it is given a
dev database, and the table option alone without one; see
[A changed MySQL table collation](../../atlas/retained-divergences/#a-changed-mysql-table-collation).
MariaDB lists its UCA 14.0.0 collations, such as `utf8mb4_uca1400_ai_ci`, with
no character set, and the binary reads no character set for a table that
holds one. `ptah-compat` writes the change between two of them as the binary
does, `COLLATE utf8mb4_uca1400_ai_ci` without `CHARSET utf8mb4`. The collation
names its character set, so the server makes the same change.

Where the other side is not a UCA 14.0.0 collation, `ptah-compat` writes the
character set the collation names, and a column that takes the table's new
collation is restated without a character set, so it takes the collation the
table declares. The binary writes the dev database's character set on the
table and `CHARSET utf8mb3` on the column, which MariaDB can refuse, or which
gives the column the default collation of `utf8mb3`; see
[A MariaDB table changed to a UCA 14.0.0 collation](../../atlas/retained-divergences/#a-mariadb-table-changed-to-a-uca-1400-collation).

A foreign key stands on the collation of both its ends, so the server refuses
a change of either end's collation while the key stands: MySQL with Error 3780
and MariaDB with Error 1832. Ptah drops the key, changes both ends, and adds
the key back, as it does for a change of the columns' type, so one apply
changes a key and the column it references together. A change of the
character set is a change of the collation. The statements are not one
transaction: where the referenced table fails with Error 1366 after the
referencing one converted, the key stays dropped, and the next plan finishes
the change and adds it back, as it does after a failed change of type.

A change of a column's character set converts every value the column holds,
and is rated destructive: a character the new character set cannot hold fails
the statement with Error 1366, or is lost where the session is not strict.
`ptah schema drift` reports it as `column_charsets_converted`. The table's
change and the column resets are one statement, so a value that fails the
conversion leaves the table's default and its columns as they were.

A diff of two schema files predicts what the server gives the tables of the
`--from` file, as a read of the tables built from it would report: each
table's default collation, and the collation of each column that names none.
The character set defaults come from the dev database's session where
`--dev-url` names one, so a dev URL that sets `default_collation_for_utf8mb4`
is honored, and otherwise from the defaults of the newest release line Ptah
tests: `utf8mb4_0900_ai_ci` on MySQL and `utf8mb4_uca1400_ai_ci` on MariaDB
for `utf8mb4`. The `--to` file is resolved from the same defaults, so a file
compared with itself plans nothing, and a changed table default or column
character set is planned as it is against a server.

A character type with `CHARACTER SET binary` is stored as the binary type of
the same length: `varchar(10) CHARACTER SET binary` as `varbinary(10)`,
`char(4) CHARACTER SET binary` as `binary(4)`, and `text CHARACTER SET binary`
as `blob`. The comparison accepts the stored type, so such a column is not
planned again, and a `MODIFY COLUMN` that changes it restates it as declared.
A `--from` schema file holds its columns at the stored type too, so two files
that spell one column both ways diff as synced in either direction.

A `mysql://`, `mariadb://` or `maria://` URL cannot say whether the server is
MySQL or MariaDB, and the server can. A `schema diff` or `schema inspect` that
creates a schema file on the dev database reads the file again for the dev
server's dialect where the URL named the other, so the table options the file
states, such as `COLLATE=utf8mb4_unicode_ci`, reach the tables it creates, as
`schema apply` and `migrations generate` read a file from the start. The second
read resolves the source again: a desired schema that a `data "external_schema"`
program writes runs that program a second time, so the program has to print the
same schema on each run and change nothing else.

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

## TLS with a private certificate authority

A server whose certificate a private certificate authority signed is verified
against that authority with the `ssl-ca` URL parameter. It names a PEM file of
one or more CA certificates:

```text
mysql://app:secret@db.internal:3306/shop?ssl-ca=/etc/ptah/mysql-ca.pem
```

`ssl-ca` requires TLS and verifies two properties: the server's certificate
chains to a certificate in the bundle, and the certificate names the URL's
host. That is what the MySQL client's `--ssl-mode=VERIFY_IDENTITY` checks. The
bundle replaces the system roots for this connection only. The process trust
store is neither read nor changed, so two URLs naming different bundles connect
side by side. `mariadb://`, `maria://` and the Go-driver form
`mysql://app:secret@tcp(db.internal:3306)/shop?ssl-ca=...` take the parameter
the same way, and `tls=true` beside it changes nothing. The name is read
without regard to case, and `ssl_ca` is the same parameter, as in MySQL option
files.

The name is the MySQL and MariaDB client option for the same file, and the
name the Atlas URL documentation gives a custom CA. `ptah-compat` reads it in
the same URLs. The pinned Atlas CLI community binary does not apply it.
Without `tls`, it sends `ssl-ca` to the server as a session variable, and the
server answers `Error 1064`. With `tls=true`, the handshake fails with
`x509: certificate signed by unknown authority` whichever file `ssl-ca` names.
`PTAH_ATLAS_STRICT_COMPAT=1` refuses the parameter for that reason (see [Strict Community Edition mode](../../atlas/strict-ce-mode/)).

### Try it locally

These commands create a certificate authority and a server certificate for
`127.0.0.1` and `localhost`, start MySQL with them, and read the database
through the authority. They need `openssl`, Docker and a free port 3306.

```sh
mkdir tls
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=Local MySQL CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout tls/ca-key.pem -out tls/ca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=127.0.0.1" \
  -keyout tls/server-key.pem -out tls/server.csr
printf 'subjectAltName=IP:127.0.0.1,DNS:localhost\n' > tls/server.ext
openssl x509 -req -in tls/server.csr -CA tls/ca.pem -CAkey tls/ca-key.pem \
  -CAcreateserial -days 30 -extfile tls/server.ext -out tls/server-cert.pem
chmod 0644 tls/*.pem
```

The server runs as another user inside the container, so the files are made
readable to it. These keys exist for this example only.

```sh
docker run -d --name mysql-tls -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=secret -e MYSQL_DATABASE=app \
  -v "$PWD/tls:/etc/mysql-tls:ro" \
  mysql:8.4 \
  --ssl-ca=/etc/mysql-tls/ca.pem \
  --ssl-cert=/etc/mysql-tls/server-cert.pem \
  --ssl-key=/etc/mysql-tls/server-key.pem
until docker exec mysql-tls mysql -uroot -psecret -h127.0.0.1 -e 'SELECT 1' >/dev/null 2>&1; do sleep 2; done
docker exec mysql-tls mysql -uroot -psecret -h127.0.0.1 app -e 'CREATE TABLE notes (id int PRIMARY KEY)'
ptah db read --db-url 'mysql://root:secret@127.0.0.1:3306/app?ssl-ca=tls/ca.pem'
```

The read prints the `notes` table. A bundle that did not sign the server's
certificate stops the command at exit 2:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -subj "/CN=Another CA" \
  -addext "basicConstraints=critical,CA:TRUE" -keyout other-key.pem -out other-ca.pem
ptah db read --db-url 'mysql://root:secret@127.0.0.1:3306/app?ssl-ca=other-ca.pem'
```

```text
Error connecting to database: failed to ping database: tls: failed to verify certificate: x509: certificate signed by unknown authority
```

Remove the server with `docker rm -f mysql-tls`.

### A bundle Ptah cannot use

The bundle is read when the connection opens, before the server is contacted.
Every PEM block in it must be a certificate that parses; text between blocks,
such as the comments a distribution bundle carries, is ignored. A missing or
unreadable file, a file holding no PEM certificate, a damaged certificate, and
a block that is not a certificate, such as a private key, each stop the command
at exit 2 and name the file by its absolute path:

```text
the ssl-ca parameter names a CA bundle Ptah cannot use: CA bundle /work/tls/server-key.pem holds a PEM block that is not a certificate: block 1 is "PRIVATE KEY", and the bundle takes CA certificates only
```

A server certificate issued for another host name fails the handshake with
`x509: certificate is valid for ..., not ...`. No failure falls back to an
unverified or an unencrypted connection, and no message repeats the password.

Ptah refuses a URL that contradicts `ssl-ca`, before it connects:

- `tls=false`, `tls=skip-verify`, `tls=preferred` and
  `allowFallbackToPlaintext=true`, each of which leaves the connection
  unverified or unencrypted;
- a socket address, `mysql+unix://` or `unix(...)`, which names no host to
  verify;
- a port with no host, as in `mysql://app@:3306/shop`. A URL that names no
  address at all dials `127.0.0.1:3306`, and the certificate must name
  `127.0.0.1`;
- an empty `ssl-ca`, or `ssl-ca` given twice, in any spelling.

Every URL parameter whose name starts with `ssl-` or `ssl_`, in any case,
belongs to Ptah, whether or not the URL names `ssl-ca`. Ptah reads `ssl-ca` and
refuses each of the others, such as `ssl-mode`, `ssl-cert` and `ssl-key`,
before it connects. The driver would send such a parameter to the server as a
session variable, which the server refuses with an error that carries the
value. So the URL cannot set a session variable whose name starts with `ssl_`.

### Where the path is read

A relative path is read from the directory the command runs in, not from the
directory of a configuration file that holds the URL. Percent-encode a `%`,
`&`, `+` or `#` in the path; a `/` may be written as is or as `%2F`.

The Ptah process reads the file, so when Ptah runs in a container the bundle
has to be inside that container, and the URL names the path there:

```sh
docker run --rm -v "$PWD/tls/ca.pem:/tls/ca.pem:ro" ghcr.io/stokaro/ptah:latest \
  db read --db-url 'mysql://app:secret@db.internal:3306/shop?ssl-ca=/tls/ca.pem'
```

In Kubernetes, mount the CA certificate from a ConfigMap or a Secret and name
the mounted file. A CA certificate is public, and a ConfigMap is enough for it.

A dev database or shadow database URL takes `ssl-ca` the same way, and a
scratch database Ptah creates on the server a URL names is reached with that
URL's `ssl-ca`.

### A CA certificate is not a client certificate

`ssl-ca` decides which servers Ptah trusts. It sends nothing to the server, and
the account still logs in with its password. An account created with
`REQUIRE SSL` needs TLS alone, and `ssl-ca` meets it. An account created with
`REQUIRE X509`, `REQUIRE ISSUER` or `REQUIRE SUBJECT` needs a client
certificate, which is mutual TLS. Ptah does not support mutual TLS: `ssl-cert`
and `ssl-key` are refused rather than ignored. The bundle holds CA certificates
only, and a private key in it is refused.

A MySQL server started without certificates generates its own, and the
generated server certificate names no host. Host name verification cannot pass
against it, so give the server a certificate whose `subjectAltName` holds the
host name or address the URL uses. Ptah does not read a host name from the
certificate's common name.

### Tools Ptah starts

The `mysqldump` pre-flight hook and the online-DDL tools, gh-ost and
pt-online-schema-change, open their own connections, and Ptah gives them no TLS
settings. A URL with `ssl-ca` is refused for them, because the tool would
connect without verifying the server.

A provider manifest can claim a MySQL-family engine, such as `mysql`, and a
command can name it with `--provider-engine`. The provider is then handed the URL as
text, and Ptah's connector does not take part. A URL with `ssl-ca` is refused
before the provider receives it, because nothing says the provider verifies
the server against the bundle.

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

Ptah reads `SHOW GRANTS` for the connecting account before it decides which
objects the account can see. If the server or proxy rejects this statement, the
read fails. The privilege visibility described below was measured on direct
MySQL and MariaDB servers. It has not been verified on AWS RDS, Aurora, Google
Cloud SQL, Azure Database for MySQL, ProxySQL, or Vitess. If one of these
services gives a different result, open an issue with the product and version,
account grants, and a reproducible read failure.

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
  comparison. `schema diff` names this visible-scope limit instead of claiming
  that two servers are fully synced.
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

MariaDB can hide constraint rows from an account with only `SELECT`. The read
still gets each primary and unique key from its visible index, and each foreign
key's columns and referenced table from `KEY_COLUMN_USAGE`, which MariaDB shows
that account. The selector leaves each key out as it does for an account that
sees the row, so both accounts plan the same. The index the server built for a
foreign key leaves the comparison with its key, and a plan that drops an index
the author declared under the key adds one for the key in the same statement,
so no plan drops an index the server refuses to drop while the key stands.

A selector that names objects does not count, because a read that cannot see a
trigger cannot say what it is called: `--exclude 'audit_*[type=trigger]'` still needs
`TRIGGER`. Neither does `--exclude '*[type=foreign_key]'` or `--exclude '*[type=check]'`,
since the catalog view that hides a foreign key or a CHECK constraint from a MariaDB
account hides a primary key with it.

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

`schema diff` compares a migration directory's views with views in the target
database even when the dev and target databases have different names. A view
whose body is the same reports no change; a changed body still produces a plan.

`migrations drift` and `migrations up --check-drift` replay the applied history in a
shadow database and compare it with the target. A view, a routine, or on MariaDB a
sequence that the migrations create there is the target's own object, whatever the
two databases are named, and a changed view body is still reported. So is a grant the
migrations write there: `GRANT SELECT ON t` compares as the target's grant on its own
table, and `GRANT SHOW VIEW ON *` as the target's grant on its own database. An object
a migration creates in another database is compared under that database's name. A
`schema apply --dev-url` rehearsal writes the target's grants onto the dev database's
objects in the same way.

### View security

A MySQL or MariaDB view runs with its definer's privileges under `SQL SECURITY
DEFINER`, and with its caller's under `SQL SECURITY INVOKER`. Set
`security="INVOKER"` on a `//ptah:schema:view` annotation, or `security:
INVOKER` on a YAML view, to run it with its caller's privileges. A SQL schema
file can declare `CREATE [OR REPLACE] SQL SECURITY DEFINER VIEW` or `CREATE [OR
REPLACE] SQL SECURITY INVOKER VIEW`; reading the file preserves that mode.

The read keeps `information_schema.VIEWS.SECURITY_TYPE`. A declaration that
omits `security` states the server default, `DEFINER`, because a `CREATE OR
REPLACE VIEW` without the clause selects it. A new view declared without
`security` takes `DEFINER`, and so does a view held as `DEFINER`. A plan that
creates a view writes no `DEFINER` clause. A plan that replaces a view held as
`DEFINER` names the view's definer when a database read answers for the plan,
as the definer paragraphs below describe. A plan made from a replay or from
two descriptions names no definer.

When a view holds `SQL SECURITY INVOKER` and the declaration names no mode, the
plan refuses before the server changes, whatever else changes on the view.
Without the clause the view would run with its definer's privileges, not its
caller's, and no statement would fail. A view altered by hand, or a declaration
written before the mode was set, is the usual case:

```text
unsupported feature: view my_docs holds SQL SECURITY INVOKER and the schema does not state it. Without it the view runs with its definer's privileges, not its caller's. Declare SQL SECURITY INVOKER to keep it, or SQL SECURITY DEFINER to change it. A Go annotation states the mode as security="INVOKER", a YAML view as security: INVOKER, and an HCL view as security = INVOKER
```

Declare `INVOKER` to keep the mode: `security="INVOKER"` on the annotation,
`security: INVOKER` in YAML, `CREATE SQL SECURITY INVOKER VIEW` in a SQL file, or
`security = INVOKER` in HCL. The plan then writes the clause with any other
change. Declare `DEFINER` to change it, and the plan runs as usual. Keeping the
mode in silence would ignore an author who removed it on purpose. `schema drift`
plans nothing, so it reports the difference under `views_modified` and does not
refuse, and a rollback migration that restores the earlier mode is not refused
either. Under `--json`, `schema plan` and `schema apply` report the refusal as
`outcome: refused` with the code `view-security-option`, and `view_options`
names the view with the option `sql_security`. The rule matches the one for
[PostgreSQL view options](../postgresql/#a-security-option-the-declaration-leaves-out).

A `CREATE OR REPLACE VIEW` that names no `DEFINER` makes the account that runs
it the view's definer, whatever the mode. The read keeps
`information_schema.VIEWS.DEFINER` and the connected account,
`CURRENT_USER()` with its host, and compares them as it does for a routine.
When a view runs under `SQL SECURITY DEFINER` before and after the change and
its definer is another account, the plan refuses before the server changes.
The replacement would run the view with the connected account's privileges,
and no statement would fail:

```text
unsupported feature: view my_docs runs with the privileges of its definer owner@% under SQL SECURITY DEFINER, and replacing it as app@% would make app@% its definer. Connect as owner@% to replace it, declare SQL SECURITY INVOKER if it should run with its caller's privileges, or leave the view unchanged
```

A read that did not record the definer or the connected account is refused
too, since nothing says the account stays the same. A view held as `INVOKER`
runs with its caller's privileges until the change, and a view declared
`INVOKER` runs with them after it, so the plan replaces either one and the
connected account becomes the definer. Under `--json` the refusal is `outcome:
refused` with the code `view-definer`, and `view_definers` names each `view`
with its `definer` and the `account` the plan would make its definer.
`schema drift` reports the difference and does not refuse.

When the definer is the connected account, the replacement names it, so the
view keeps its definer whichever account applies the plan later:

```sql
CREATE OR REPLACE DEFINER=`app`@`%` SQL SECURITY DEFINER VIEW `my_docs` AS ...
```

A rollback migration is checked on the same terms. It names the definer the up
migration's read recorded whenever it writes the view under `DEFINER`, since
that is the view it restores. That includes a view the up migration moves from
`DEFINER` to `INVOKER`: the rollback restores the `DEFINER` view for its
definer, whichever account that is, and an account that cannot name the
definer is refused when the rollback runs.

#### A plan made from a replay

`migrations generate --replay`, `ptah-compat migrate diff` and `ptah-compat
schema diff` with a migration directory as `--from` plan from a dev database
that a replay built. Its account and its definers are the dev server's, so
the plan names no definer and refuses no change of definer. The replayed
state still records each view's mode, so a replacement of a view that runs as
its definer warns on standard error, once per view, and the exit code stays
the same:

```text
warning: view my_docs runs with its definer's privileges under SQL SECURITY DEFINER, and the migration replaces it without a DEFINER clause, so the account that applies the migration becomes its definer. To keep a definer, write DEFINER= into the file, or apply it as that definer
```

The warning is not written into the file or to standard output.

A history that names definers needs a dev account that may name them: on
MySQL `SET_ANY_DEFINER`, with `ALLOW_NONEXISTENT_DEFINER` for an account the
dev server lacks, and on MariaDB `SET USER`. An administrative account holds
them. Without them the replay fails with error 1227.

#### Naming another account as the definer

A statement that writes `DEFINER=` for another account fails with error 1227
unless the account that runs it holds a global privilege. MySQL and MariaDB
run DDL outside a transaction, so a failure in the middle of a run would
leave the statements before it applied. `schema apply`, with a computed plan
or with `--plan`, `ptah-compat schema apply` and `migrations up` and `down`
therefore ask before the first statement, and `--dry-run` refuses what the run
would refuse. They scan the statements, and for `migrations` every pending
file, for a `CREATE` or `ALTER` of a view, routine, trigger, event or MariaDB
package that names another account, and refuse when the connected account
cannot name it.

A plan that also holds a provider-native operation is scanned over its SQL
statements. A clause inside an executable comment the connected server runs is
read as part of the statement: `/*!50013 DEFINER=... */` as `mysqldump` writes
it, a comment gated on a version no later than the server's, and on MariaDB
`/*M! ... */`. The message names the object, the definer, the account and the
privilege. Grant the privilege, or apply as the definer:

```sql
-- MySQL
GRANT SET_ANY_DEFINER ON *.* TO 'deploy'@'%';
-- MySQL, when the definer account does not exist on the server
GRANT ALLOW_NONEXISTENT_DEFINER ON *.* TO 'deploy'@'%';
-- MySQL 8.0 and 8.1, which have neither of the two
GRANT SET_USER_ID ON *.* TO 'deploy'@'%';
-- MariaDB
GRANT SET USER ON *.* TO 'deploy'@'%';
```

`SUPER` covers both on MySQL. Under `--json` the refusal is `outcome:
refused` with the code `definer-privilege`, and `definer_privileges` names
each `object` with its `definer`, the `account` and the `privilege`. A
statement that names the connected account or `CURRENT_USER` needs no
privilege, and neither does `CURRENT_ROLE` on MariaDB, where it is the current
role. MySQL reads `CURRENT_ROLE` as the account `CURRENT_ROLE@%`, and the check
treats it as one.

The check reads plain `SHOW GRANTS`, which on both servers includes the
privileges of an active role and leaves out a role that is granted but not
active, as the server does. The grants are read on a session of their own,
so a role that a file activates with `SET ROLE` is not counted. This is the one
case where the check refuses a statement the server would accept: an account
that holds the privilege only through such a role is refused, although the
server would accept the statement after the `SET ROLE`.

Where the check cannot decide, it refuses nothing, and the server answers with
error 1227 as it would without the check:

- `SHOW GRANTS` fails;
- on MySQL before 8.0, the account lacks `SUPER`, the only privilege that
  names another account there;
- on MariaDB, the account holds `SUPER` without `SET USER`: MariaDB 10.11
  accepts it, 11.4 and later refuse it;
- on MariaDB, the definer is written without a host, so it may name a role;
- on MySQL, the account holds `SET_ANY_DEFINER` without
  `ALLOW_NONEXISTENT_DEFINER`, and it cannot read `mysql.user` to learn whether
  the definer exists, or the run creates or renames an account, since the
  server is read before the run starts;
- a quoted name holds a backslash, whose meaning depends on `sql_mode`.

The plan records the mode before and after each statement on a view, as the
[`sql_security` effect](../../reference/statement-effects/).

### Roles and grants

Reading roles and the grants on them needs `SELECT` on the `mysql` database, which
ordinary schema work does not. An account without it records them as not described
and is not refused, so a comparison decides nothing about them in either direction.
When a SQL diff finds no changes in the readable objects, it names roles and
grants as a comparison limit instead of claiming the schemas are fully synced.
Native `schema diff --format json` includes these limits in `comparison_limits`.
An account that compares grants needs `SELECT` on `mysql.*`.

A grant may name columns of a table or a view, as in
``GRANT SELECT (id, name) ON t1 TO `reader` ``. The server holds it as a
privilege on each column and on nothing else: the role cannot read a column the
grant leaves out. A read takes column privileges from
`information_schema.COLUMN_PRIVILEGES`, and a plan grants and revokes them one
column at a time. `SELECT`, `INSERT`, `UPDATE` and `REFERENCES` take a column
list; any other privilege with one is refused by name before anything runs,
because the server answers it with a syntax error.

A declared `GRANT ALL ON t1` is held once the role holds every privilege the
server's `ALL` grants on a table: `ALTER`, `CREATE`, `CREATE VIEW`, `DELETE`,
`DROP`, `INDEX`, `INSERT`, `REFERENCES`, `SELECT`, `SHOW VIEW`, `TRIGGER` and
`UPDATE`, and `DELETE HISTORY` on MariaDB. `ALL` on a database adds `ALTER
ROUTINE`, `CREATE ROUTINE`, `CREATE TEMPORARY TABLES`, `EVENT`, `EXECUTE` and
`LOCK TABLES`. MariaDB 11.4 and later also grant `SHOW CREATE ROUTINE` there,
which a role is not required to hold, since 10.11 has none. A role the schema
creates keeps every one of them. Measured on MySQL 8.0, 8.4, 9.7 and 26.10 and
on MariaDB 10.11, 11.4, 11.8 and 12.3.

A declared `REVOKE ALL ON t1` takes every privilege the role holds on `t1` and
on its columns, whatever the server calls it: `ALTER`, `CREATE`, `DROP`,
`INDEX`, `CREATE VIEW` and `SHOW VIEW`, and `DELETE HISTORY` on MariaDB, as well
as the privileges PostgreSQL also has. A grant after the revoke in the file is
what the role keeps, and it keeps the grant option only where that grant says
`WITH GRANT OPTION`.

On MySQL and MariaDB a revoke reaches past the row it names. `REVOKE SELECT ON
t1` takes every column `SELECT` the role holds on `t1`, and `REVOKE GRANT OPTION
ON t1` takes the grant option of every privilege the role holds on `t1`,
columns included. So a plan grants a kept column privilege again right after
the revoke on its table that takes it, plans no revoke of a column privilege
that a revoke on its table already takes, since the server refuses a revoke of
a privilege nothing holds, and takes away the grant option a revoked privilege
leaves behind where the role keeps no privilege with it.

The server keeps a table or column privilege after `DROP TABLE` and `DROP
COLUMN`, and the privilege applies again to a table or column created under
the same name. So a plan runs every revoke first, before it creates a table or
adds a column; `REVOKE` on a name nothing holds succeeds. A revoke placed after
them leaves the role the new object while the statements before it run, and
for good when one of them fails.

A `REVOKE` on a table takes only a privilege granted on that table, and the
server refuses a revoke of a privilege the role does not hold there with ERROR
1147. A declared revoke on a table the plan creates therefore has a statement
only where the read reports a privilege under the table's name, which a
dropped table of that name leaves behind.

A grant on the table's database (``GRANT SELECT ON d2.* TO `r` ``), on every
database (`ON *.*`), or on MariaDB to `PUBLIC`, gives the role the privilege on
the table as well, and a revoke on the table cannot take it away. The server
answers that revoke with ERROR 1147, and a plan that left it out would read as
synced while a member of the role keeps reading the table. So Ptah refuses a
declared revoke on a table, existing or created, that such a grant keeps, and
names the grant: revoke it where it was granted, or grant the privilege on
each table that needs it. Measured on 8.4.13, 10.11.19 and 11.8.9.

A database grant the schema declares under `grants:` with `on_schema` keeps
the privilege too, before the server holds it, because the plan grants it.
Where the role holds a database grant the schema also declares, the refusal
does not advise revoking it, because the plan would grant it again. A grant on
another database keeps nothing on the table: a table named without its
database is in the database the connection selected. On a server that keeps
names apart by case, a database whose name differs only in case still counts,
so such a revoke is refused. Measured on 8.4.13, 26.10.0, 10.11.19, 11.8.9 and
12.3.3.

A database grant the plan removes keeps nothing: one the schema revokes under
`revokes:` with `on_schema`, and an undeclared one held by a role the schema
declares. A revoke of `ALL` on a table names what `ALL` grants on a table, so a
database grant of a privilege no table carries, such as `EXECUTE` or
`LOCK TABLES`, keeps nothing it names. Measured on 8.4.13, 26.10.0, 10.11.19,
11.8.9 and 12.3.3.

A kept column privilege that a revoke on its table takes comes back right
after the revokes where its column exists, and after the `CREATE TABLE`,
`CREATE VIEW`, `CREATE OR REPLACE VIEW` or `ADD COLUMN` that brings the column
back where it does not: the server refuses a grant on a missing column with
ERROR 1054 and on a missing table or view with ERROR 1146. Until that statement
runs, the role holds nothing there. An apply that completes leaves the role
with what the declaration states, and the next plan is empty.

On MariaDB the column of a column privilege is compared without regard to
case, as the server compares column names. A declaration that spells `name`
where the database holds a privilege on `Name` matches it, and a privilege the
plan adds on that column names it `Name`. MariaDB 11.8.9 and 12.3.3 serve
either spelling to a user holding the role.

On MySQL the column of a column privilege is compared by its spelling. A
column privilege a role gives serves only a statement that spells the column
as the grant does: measured on MySQL 8.4.13 and 26.10.0, with `SELECT (Name)`
granted to the role, a user holding it runs `SELECT Name` and is refused
`SELECT name` with ERROR 1143. So a declaration of `SELECT (name)` does not
match a held privilege on `Name`: the plan revokes the privilege on `Name`
first, then grants the one on `name`, as the declaration spells it. The grant
runs right after the revokes where the column exists, and after the statement
that adds the column where it does not. A statement that fails later in the
plan leaves the role with the column under the declared spelling.

This holds for a role the schema does not create too. A declared grant of
`SELECT (name)` replaces a held `SELECT (Name)`, and a declared
`REVOKE SELECT (name)` takes the privilege under every spelling the server
accepts for the column. A privilege the declaration says nothing about stays.
So where such a role keeps `UPDATE (Name)`, a plan that grants it
`SELECT (name)` would leave the column under two spellings, and it is refused
before it runs. Declare the role's privileges on the column under one
spelling, or revoke them as the catalog spells them: `REVOKE UPDATE (Name)`
beside `GRANT SELECT (name)` names no privilege twice.

The server keeps a column privilege in memory under one name and in
`mysql.columns_priv` under each spelling, so a grant under one spelling and a
revoke under the other leave the two disagreeing: measured on MySQL 8.4.13 and
26.10.0 and MariaDB 11.8.9 and 12.3.3, a user holding the role reads a column
the plan revoked and cannot update one it granted. So these are refused before
anything runs:

- a declaration that grants one column of a table under two spellings to one
  role;
- a `REVOKE` that takes back, under another spelling, a privilege a grant gives
  on the column: `GRANT SELECT (Name)` beside `REVOKE SELECT (name)` asks for
  one privilege both held and absent;
- a plan that changes a role's privileges on a table where the database holds
  one of its columns under two spellings. Revoke the privileges under both
  spellings by hand, then plan again.

A revoke of another privilege on the column, and two revokes, are accepted
under any spelling, because a revoke takes the privilege under every spelling.

A privilege on a function or procedure is refused by name when a plan grants
it, and the read does not report routine privileges, so a comparison cannot see
an `EXECUTE` granted or revoked outside the schema.

A database name is matched exactly. On a server started with
`lower_case_table_names=0`, the Linux default, `app` and `APP` are two
databases, and a read of one reports nothing of the other, although
`information_schema` compares names without regard to case in its privilege
views on both engines, and in `TRIGGERS` and `EVENTS` on MariaDB. On a server
started with `lower_case_table_names=1`, a URL may spell the database in any
case: the server folds the name, and the read describes the database under the
name the server gives it.

On a server started with `lower_case_table_names` 1 or 2, a schema file may
spell a database or a table in any case too. The comparison matches each name
to the spelling the server holds: a table, view, routine, trigger, foreign key,
grant or revoke that the file qualifies with `FA` is in `fa` when the URL names
`fa`, and one that names `Items` is on `items`. So `REVOKE SELECT ON FA.items
FROM r` and `REVOKE SELECT ON Items FROM r` revoke the privilege, `CREATE TABLE
Items` beside `items` declares that table, and a plan made a second time is
empty. On a URL that names no database, a name is matched the same way against
every database the server holds: `CREATE DATABASE FA` beside `fa` declares
`fa`, and the plan drops nothing.

On a server started with `lower_case_table_names=0`, `FA` is another database
and `Items` another table. A table in `FA` is refused as outside the URL's
database, and a revoke on `FA.items` leaves the grant on `fa.items`.

A comparison that no connection to the server makes, such as a `schema diff`
between two files, does not know the setting. It refuses a database or table
name that matches another only without regard to case, rather than plan under
either answer, and the error names both spellings. Compare against the
database, or spell both names the same way.

With `lower_case_table_names=2`, which needs a file system that ignores case,
MariaDB answers `DATABASE()` in the spelling of the URL while its privilege
views spell the database in lower case. The read describes the database and its
grants under the first spelling, so a plan converges. Measured on MariaDB 11.8.9 and 12.3.3 and MySQL 26.10.0 with that
setting; Ptah's CI runs no server with it.

### Role memberships

On MySQL 8, `member_of` on a role names the roles it joins, and a plan runs
``GRANT `group` TO `member` `` and `REVOKE`. A membership may state the `admin`
option, which is `WITH ADMIN OPTION`. MySQL has no per-membership `inherit` or
`set` option, so a declaration that states one is refused.

A `GRANT WITH ADMIN OPTION` on a membership that exists adds the option. MySQL
has no statement that withdraws the option alone (`REVOKE ADMIN OPTION FOR` is a
syntax error on 8.4), and a plain `GRANT` leaves it on, so a plan that removes
the option revokes the membership and grants it again. A declaration that states
no option means the default, no admin option, so a membership that holds the
option is brought back to it. The server commits each statement as it runs,
so the member holds no role between the `REVOKE` and the `GRANT`, and keeps none
if the `GRANT` fails. The plan carries a warning comment before the pair.

Which memberships a plan revokes follows the rule on the PostgreSQL page: only
a role that states `member_of` loses one, and deleting the key revokes nothing.
See [Role memberships](../postgresql/#role-memberships), which also shows the
`UNMANAGED` line for a membership the plan keeps. MariaDB refuses a declared
membership.

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
`--exclude 'e_tick[type=event]'`, counts as a match and takes that event out of
the report, since the run is not about it. An `--include` selection leaves out
of the report every event it does not name. An event is not something to
compare, so an `--include` that names only events matched no objects, and
`schema apply` refuses it as it refuses any empty selection.

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

## Views that read other views

The server checks a view's query when the view is created, so a view has to be
created after every view it reads. Ptah reads the body of each view the plan
adds or replaces, and creates a view after the views of the plan it reads,
whatever order the schema file or the names give. A name is a read where a
relation can appear, as after `FROM` or a `JOIN`; a column alias, a table alias
or a common table expression that has the name of another view is not.
`depends_on` adds an order the body does not show. A name inside a string is
not a reference, and a string is read as the server reads it: `"x\"y"` is one
string.

Views the plan adds that read one another in a ring are refused before anything
is applied, because no order creates them:

```text
error: generate schema apply operations: the views a_v, b_v read one another in a cycle, so none of them can be created first: a_v reads b_v reads a_v
```

## Dropping a table read by a view

MySQL accepts `DROP TABLE` when a view still reads the table. The view stays in
the catalog, but querying it fails. When a plan drops a table that a declared
view still reads, the generated SQL warns and names the view. The warning does
not block apply. Remove or replace the view, or keep the table, before applying
the plan. A view the plan removes is dropped before the table. The warning
cannot identify a view absent from the declaration and hidden from the read.

A dev database can hold such a view after a replay, for example when
`migrate lint` replays a migration that drops a table a view reads. Both
servers refuse to lock that view with error 1356, so cleanup drops the views
the server cannot open first, then locks and drops the others. The checks for
views and stored programs in other databases run again before that first drop.

A `SQL SECURITY DEFINER` view whose definer account does not exist on the dev
server cannot be opened either. MySQL refuses the lock with error 1449 and
MariaDB with error 1446. A replayed file that writes `DEFINER=` for an account
of the target server leaves such a view, because an account with the
privileges to name another definer creates it with a note. Cleanup drops it
with the other views the server cannot open. A `SQL SECURITY INVOKER` view
naming the same account opens and goes through the usual lock.

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
