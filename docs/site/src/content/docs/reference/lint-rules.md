---
title: Lint rules
description: Every rule Ptah's linters report, with its meaning, dialects, surface, and whether the name is Atlas's or Ptah's.
type: reference
audience:
  - "all-users"
readerQuestion: "Which lint rule identifiers can Ptah report?"
goal: "Look up every lint rule identifier Ptah can report."
sourceOfTruth:
  - "migration/lint"
  - "internal/sqllint"
  - "internal/lintcatalog"
generated: false
overlaps: []
disposition: keep
owns:
  - cli-ptah-sql-lint
---

Ptah lints in two places, and both report findings under stable identifiers.
Migration lint reads a migration directory; SQL lint reads standalone `.sql`
files. This page enumerates every identifier either one can report.

The tables below are generated from the rule registries themselves.
`scripts/check-docsync.sh` fails when a rule exists in the code and not on
this page, or when a row here names a rule that no longer exists.

## Which commands lint

| Command | Surface | Rules it reports |
| --- | --- | --- |
| `ptah migrations lint` | native | every migration lint rule |
| `ptah migrations up` | native | blocking `DS` findings only |
| `ptah sql lint` | native | every SQL lint rule |
| `ptah-compat migrate lint` | compatibility | every rule marked `both` |
| `ptah-compat schema plan lint` | compatibility | every rule marked `both` |
| `ptah-compat schema apply` | compatibility | only what `atlas.hcl` names |

Both surfaces read one registry, so a rule reaches both unless the Surface
column in the tables below marks it `native only`. Native may carry rules the
compatibility surface does not; the reverse would mean a rule that exists only
to match another tool, and there are none.

The two apply gates are the rows to read carefully. Neither runs the whole rule
set, so a rule appearing in the tables below is not by itself a check standing
between an apply and a database. The migration lint section states what each
gate does run, generated from the gates themselves.

`ptah-compat schema plan lint` is a third row to read carefully, for the
opposite reason: it runs the whole `both` set over a saved plan file's SQL, and
then reports rather than gates. Findings there do not change the exit code
unless `PTAH_ATLAS_PLAN_LINT_FAIL_ON_ERROR=1` asks for a threshold, so a rule
appearing below is a check that will be *reported* on a plan, not one standing
between that plan and a database.

## What a rule reads

Most rules decide from the migration SQL. A few cannot: the type of a column a
`RENAME` introduces, for instance, lives in an earlier file or in the base
schema and never in the rename statement. Those rules read a second input — the
schema state the analyzed version starts from, replayed onto the dev database
`--dev-url` names.

Which of the two a rule takes is declared on the rule rather than inferred,
because the failure mode of getting it wrong is silence: the rule runs,
resolves nothing, and reports less than it should while the command still exits
0.

| Input | What the rule sees |
| --- | --- |
| statement text | The migration SQL, and nothing else. The default. |
| baseline schema | The above, plus the schema state the version starts from, read from the replayed dev database. |

Two things follow from the declaration:

- **Only the versions a rule asks about are read.** A directory with nothing to
  resolve costs no introspection, and a rule silenced by `--disable`, by
  `atlas:nolint`, or by a reviewed-schema scope that excludes its statements
  costs none either.
- **A rule that asks and gets nothing says so.** When the run supplies no
  starting state for a version a rule asked about, both surfaces print a
  warning on **stderr** naming the rule. Stdout bytes and the exit code are
  unchanged, so nothing that parses the report is affected:

  ```text
  warning: DD101 ran without the baseline schema it reads, so this analysis is
  thinner than the same directory would get against a dev database the run can read
  ```

  A clean report with that line on stderr means the analysis was narrower than
  the directory allows, not that the directory is clean.

`DD101` reads the baseline for the add side of a column
rename on the compatibility surface. The native surface models a rename as a
rename, so it asks for no starting state and never prints the notice.

## How identifiers are spelled

An identifier is a two- or three-letter family prefix and a number. The prefix
says whose namespace the rule lives in, and that decides how the rest is
spelled:

- A rule that reports an Atlas analyzer check carries the identifier Atlas
  uses, unchanged.
- A rule of Ptah's own inside a family Atlas also uses carries the same scheme
  with a trailing `P`. A Ptah-only PostgreSQL destructive-change rule numbered
  112 is `PG112P`. The suffix lets a reader inside a shared family tell which
  member is ours, and keeps a check Atlas adds later from colliding with one of
  ours.
- A rule inside a family Atlas does not use carries no suffix. The prefix
  already says the rule is ours, so a `P` would be noise. `SQL001`, `DDL001`
  and `CAP001` are this case.

The suffix marks an extension of someone else's family, not authorship. The
identifiers chosen before the convention existed are counted and listed at the
bottom of this page rather than left for a reader to notice.

## How the analysis is performed

Three kinds of analyzer are worth telling apart, because they fail differently
and because two of the three are not built. The table records the behavior the
rule registry and runners implement.

| Kind | Ptah behavior |
| --- | --- |
| **Builtin** | Every rule on this page. It decides from what it is handed — the migration SQL, or the SQL file — and reaches no server and no other process. |
| **Server-assisted** | One: the `baseline schema` input above, replayed onto the dev database `--dev-url` names. It is optional, and its absence is announced rather than absorbed. |
| **Optional provider** | None. No external analyzer is integrated, and no analysis path starts another process. The only process any lint path runs is `git`, to resolve `--git-base`. |

The server-assisted one is optional in the strict sense: without a dev database
the rules that wanted it resolve nothing and report less, the command still
exits 0, and **the run says so on stderr, naming each rule that asked** — the
report on stdout is left byte-identical so a `--format json` consumer and a
compatibility consumer both keep parsing it. A gap that only shows as a smaller
report is the hardest kind to notice from CI.

That leaves one thing to know about a clean lint result: it means every builtin
rule looked and found nothing. It does not mean a routine body was read — see
`AC101` and `SQL004`, which exist to keep those two apart.

<!-- BEGIN GENERATED LINT RULES -->
## Identifier families

An identifier's prefix says whose namespace it lives in. Atlas owns a prefix when the Atlas analyzer documentation uses it; the rest are Ptah's.

| Prefix | Namespace | What the family covers |
| --- | --- | --- |
| `AC` | Ptah | analysis coverage: what the linter did not read, so a clean result is not mistaken for a checked one |
| `BC` | Atlas | changes that break code already deployed against the old schema |
| `CAP` | Ptah | the target server version lacks a capability the statement needs |
| `CD` | Atlas | constraint deletions, split by the constraint type the SQL names |
| `DD` | Ptah | changes whose outcome depends on the rows already in the table |
| `DDL` | Ptah | the shape of a DDL statement the SQL linter modeled |
| `DS` | Atlas | destructive changes: statements that delete data or drop objects |
| `LT` | Atlas | SQLite-specific hazards |
| `MF` | Atlas | Atlas: changes that may fail. Ptah: migration file form |
| `MY` | Atlas | MySQL and MariaDB-specific rebuild and blocking-DDL hazards |
| `NM` | Atlas | naming conventions, checked against the patterns a project configures |
| `OW` | Atlas | ownership policy; Atlas documents these, Ptah emits none |
| `PG` | Atlas | PostgreSQL-specific locking, rewrite, and transaction hazards |
| `SA` | Atlas | static analysis of routine bodies a migration defines |
| `SQL` | Ptah | the SQL linter could not read or model the statement |
| `TX` | Atlas | transaction shape of a migration |

## Migration lint rules

88 rules, registered in `migration/lint`. `ptah migrations lint` reports the whole registry, and `ptah-compat migrate lint` reports all of it but `BC101`, which only native `ptah` emits. Neither apply gate reports even that much, so a rule listed below is not by itself a check that stands between an apply and a database: `ptah migrations up` disables the `MF`, `BC`, `PG` and `MY` families and refuses only on blocking `DS` findings unless the policy's `gate` section names more families, and `ptah-compat schema apply` runs only the rules an `atlas.hcl` `lint` block names, which means a project without such a block gets no lint pass there at all. The tables are grouped by the dialects each rule applies to, which is why they carry no dialect column.

### Every dialect

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `AC101` | the migration defines a routine whose body is not analyzed, so a clean result says nothing about what the body does | both | Ptah |
| `BC101` | a rename retires a name deployed code still refers to | native only | Atlas |
| `BC103` | dropping a table retires a name deployed clients still query, which is a rollout break a backup does not mitigate | both | Atlas |
| `BC104` | dropping a column retires a name deployed clients still select and insert, whether or not the column held rows | both | Atlas |
| `CD101` | dropping a foreign key removes referential-integrity enforcement | both | Atlas |
| `CD102` | dropping a check constraint removes a value-validation guarantee | both | Atlas |
| `CD103` | dropping a primary key removes row identity and can break replication | both | Atlas |
| `DD101` | adding a NOT NULL column without a default fails or blocks on a populated table | both | Atlas |
| `DD102` | a routine declared immutable calls something whose result changes between two calls with the same arguments | both | Ptah |
| `DS101` | DROP TABLE destroys the table and every row in it; a rename reports here on the compatibility surface, retiring the old name without moving the rows | both | Atlas |
| `DS102` | DROP COLUMN destroys the column and every value stored in it | both | Atlas |
| `DS103` | a column type change can truncate or reject existing values and may rewrite the table under a lock; a clause that restates the column's current type, as the dev database records it, is not reported | both | Ptah |
| `DS104` | DROP NOT NULL removes a column-level data protection | both | Ptah |
| `DS105` | an untyped DROP CONSTRAINT removes a data protection the SQL does not name | both | Ptah |
| `DS106` | removing an enum value can invalidate rows that still hold it | both | Ptah |
| `DS107` | dropping a schema, type, extension, function, procedure, trigger, role, or policy removes behavior | both | Ptah |
| `DS108` | TRUNCATE deletes every row in the table | both | Ptah |
| `DS109` | DISABLE ROW LEVEL SECURITY removes an access-control protection | both | Ptah |
| `DS110P` | a column a view or routine reads is dropped, and the finding names what breaks | both | Ptah |
| `MF101` | a unique index built over existing rows fails on the first duplicate | both | Atlas |
| `MF101P` | no matching .down.sql exists, so a failed deploy cannot be rolled back mechanically | both | Ptah |
| `MF102` | an index dropped and rebuilt as unique fails on the first duplicate and leaves the table without it | both | Atlas |
| `MF102P` | the migration carries no executable statements | both | Ptah |
| `MF103` | the file name does not follow the migration file-name convention | both | Ptah |
| `NM101` | a schema this migration creates or renames to violates the configured naming convention | both | Atlas |
| `NM102` | a table this migration creates or renames to violates the configured naming convention | both | Atlas |
| `NM103` | a column this migration declares, adds, or renames to violates the configured naming convention | both | Atlas |
| `NM104` | an index or unique key this migration names violates the configured naming convention | both | Atlas |
| `NM105` | a foreign key this migration names violates the configured naming convention | both | Atlas |
| `NM106` | a check constraint this migration names violates the configured naming convention | both | Atlas |
| `SA101` | a routine builds and runs a statement from a value it does not quote | both | Atlas |

### mariadb

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `MY146` | DROP SYSTEM VERSIONING deletes every historical row version permanently, and no rollback restores it | both | Atlas |

### mysql

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `MY145` | enforcing a CHECK constraint revalidates every row; unenforcing one stops the server refusing the values it was there to refuse | both | Atlas |

### mysql, mariadb

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `MY101` | this ALTER TABLE form usually rebuilds the table and blocks writes for the duration | both | Ptah |
| `MY102` | an inline REFERENCES clause on a column is ignored by MySQL and enforced by MariaDB | both | Atlas |
| `MY110` | removing an ENUM member copies the table, and a row still holding it fails the copy in strict mode | both | Atlas |
| `MY111` | reordering ENUM members renumbers every row and copies the table | both | Atlas |
| `MY112` | inserting an ENUM member before the end renumbers the members after it and copies the table | both | Atlas |
| `MY113` | growing an ENUM past 255 members widens each value to two bytes and copies the table | both | Atlas |
| `MY120` | removing a SET member copies the table, and a row still holding it fails the copy in strict mode | both | Atlas |
| `MY121` | reordering SET members renumbers every row and copies the table | both | Atlas |
| `MY122` | inserting a SET member before the end renumbers the members after it and copies the table | both | Atlas |
| `MY123` | growing a SET across a multiple of eight members adds a byte to each value and copies the table | both | Atlas |
| `MY130` | a column type change InnoDB cannot apply in place copies the table and blocks writes | both | Atlas |
| `MY130P` | a MODIFY, CHANGE or CONVERT the dev database shows InnoDB applies without copying the table, and by which algorithm; info only | both | Ptah |
| `MY131` | adding a foreign key can copy or lock the table and block writes | both | Atlas |
| `MY132` | adding a primary key rebuilds the table in place around the new clustered index | both | Atlas |
| `MY133` | dropping a primary key without adding one in the same statement copies the table | both | Atlas |
| `MY134` | adding a FULLTEXT index can rebuild the table and block writes | both | Atlas |
| `MY135` | adding a SPATIAL index can rebuild the table and block writes | both | Atlas |
| `MY136` | converting a table's character set re-encodes its columns and copies the table | both | Atlas |
| `MY137` | replacing a primary key rebuilds the table and every secondary index that stores it as a row pointer | both | Atlas |
| `MY138` | changing the storage engine copies the table and blocks writes for the duration | both | Atlas |
| `MY139` | a partitioning change rewrites every row and accepts no ALGORITHM or LOCK clause to soften it | both | Atlas |
| `MY140` | adding a STORED generated column computes a value for every row, copying the table and blocking writes | both | Atlas |
| `MY141` | adding an AUTO_INCREMENT column rebuilds the table in place and still blocks writes | both | Atlas |
| `MY143` | changing a STORED generated column recomputes it for every row, copying the table and blocking writes | both | Atlas |
| `MY144` | adding a CHECK constraint validates every existing row, and one that fails the predicate fails the migration | both | Atlas |
| `MY147` | declaring a column NOT NULL rebuilds the table; whether an existing NULL fails the statement is DD103's question | both | Atlas |

### mysql, mariadb, sqlserver, clickhouse

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `DD103` | a nullable column made NOT NULL fails on a row holding NULL, or rewrites it to the type's default | both | Ptah |

### postgres

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `PG101` | CREATE INDEX without CONCURRENTLY blocks writes for the whole build | both | Atlas |
| `PG102` | ALTER TYPE ... ADD VALUE cannot run inside a transaction before PostgreSQL 12, and the value stays unusable in the same transaction after it | both | Ptah |
| `PG103` | CONCURRENTLY cannot run inside the migration's transaction | both | Atlas |
| `PG104` | adding a primary key takes an ACCESS EXCLUSIVE lock and can scan existing rows | both | Atlas |
| `PG105` | adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates rows | both | Atlas |
| `PG106` | DROP INDEX without CONCURRENTLY blocks writes while the index is removed | both | Atlas |
| `PG108` | an index on a partitioned table locks the parent and every partition at once, and CONCURRENTLY is refused there | both | Atlas |
| `PG109` | adding an EXCLUDE constraint holds an ACCESS EXCLUSIVE lock while it builds the index and validates every row | both | Atlas |
| `PG110` | the declared column order can waste tuple padding | both | Atlas |
| `PG301` | a column type change PostgreSQL cannot prove safe rewrites the table and its indexes | both | Atlas |
| `PG301P` | an ALTER COLUMN TYPE the dev database shows PostgreSQL applies as a catalog edit, with no rewrite, rebuild or scan; info only | both | Ptah |
| `PG302` | a volatile DEFAULT on an added column rewrites or evaluates every existing row | both | Atlas |
| `PG304` | a primary key over nullable columns sets them NOT NULL and scans every row on top of the index build | both | Atlas |
| `PG305` | adding a CHECK constraint validates existing rows and can hold locks | both | Atlas |
| `PG306` | adding a foreign key validates existing rows and can block writes on both tables | both | Atlas |
| `PG307` | changing LOGGED or UNLOGGED rewrites the table under heavyweight locks | both | Atlas |
| `PG308` | CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block writes | both | Atlas |
| `PG309` | adding a STORED generated column computes and stores a value for every row | both | Atlas |
| `PG310` | adding an identity column can rewrite existing rows | both | Atlas |
| `PG311` | changing a table's access method rewrites the table | both | Atlas |
| `PG312` | replacing a primary key builds the new unique index under an ACCESS EXCLUSIVE lock | both | Atlas |
| `PG312P` | a SECURITY DEFINER routine that does not pin search_path resolves unqualified names through the caller's | both | Ptah |
| `PG314` | REPLICA IDENTITY FULL or NOTHING changes what logical replication can carry for the table | both | Atlas |
| `PG320` | disabling autovacuum leaves dead rows for nothing to reclaim; the statement is cheap and the cost is paid later | both | Atlas |
| `TX101` | the migration mixes statements that cannot share one transaction | both | Atlas |
| `TX201` | an explicit BEGIN/COMMIT block fights the migrator's transaction management | both | Atlas |

### postgres, cockroachdb, yugabytedb, spanner

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `PG303` | SET NOT NULL scans the table to validate existing rows | both | Atlas |

### sqlite

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `LT101` | SQLite cannot enforce NOT NULL on existing nullable data without a rebuild | both | Atlas |

## SQL lint rules

7 rules, reported by `ptah sql lint` over standalone SQL files, on every dialect. The compatibility surface has no verb that reaches them.

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `CAP001` | the statement needs a capability the target server version does not have | native only | Ptah |
| `DDL001` | the created table declares no primary key | native only | Ptah |
| `DDL002` | an index names a column the schema does not declare | native only | Ptah |
| `SQL001` | the SQL parser could not build an AST, so no rule could inspect the statement | native only | Ptah |
| `SQL002` | the statement uses a sub-language `ptah sql lint` does not model yet | native only | Ptah |
| `SQL003` | a routine body builds SQL at run time, so static analysis of that routine stops there | native only | Ptah |
| `SQL004` | the file carried statement kinds no rule examined, so a clean result would not mean it was checked | native only | Ptah |

## Default severities

17 rules report at error severity by default: `CAP001`, `CD101`, `CD102`, `CD103`, `DDL002`, `DS101`, `DS102`, `DS104`, `DS105`, `DS106`, `DS107`, `DS108`, `DS109`, `DS110P`, `MY146`, `SQL001`, `SQL002`. The other 78 default to warning. A committed `.ptah-lint.yaml` replaces either, per rule or per family. `ptah sql lint` reads the same file and now reads the `rules:` severities it sets for `CAP001`, `DDL001`, `DDL002`, `SQL001`, `SQL002`, `SQL003` and `SQL004`, so the severities above are the defaults. `--disable` refuses a selector covering `SQL001` or `SQL002`: those report that the file could not be analyzed, and a run that analyzed nothing must not report clean.

## What ptah-compat prints

Every migration lint finding reports under an analyzer name and a code on the compatibility surface. Rules not listed here keep their own code under the `ptah` analyzer.

| Native rule | Analyzer | Code |
| --- | --- | --- |
| `DD101` | `data_depend` | `MF103` |
| `DS101` | `destructive` | `DS102` |
| `DS102` | `destructive` | `DS103` |

## Atlas analyzer checks

Every check code in the reviewed snapshot of the [Atlas analyzer documentation](https://atlasgo.io/lint/analyzers) (`internal/lintcatalog/atlasreference.txt`, reviewed 2026-09-06), and what Ptah does about it: 72 covered, 2 partial, 1 not implemented, 2 waived, of 77. That page presents its table as highlights rather than an inventory -- it says Atlas "runs and reports dozens of additional checks" beyond them -- so a complete row here is completeness against the snapshot, not full behavioral parity with the analyzer. A code Atlas marks as an Atlas Pro feature is marked here too, and the ones Ptah implements are reported through both surfaces except `BC101` and `BC102`, whose Ptah rule the compatibility surface does not report.

<div class="ptah-wide-table">

| Atlas check | Meaning | Pro | Ptah rule | Status |
| --- | --- | --- | --- | --- |
| `DS101` | schema was dropped | no | `DS107` | covered |
| `DS102` | table was dropped | no | `DS101` | covered |
| `DS103` | non-virtual column was dropped | no | `DS102` | covered |
| `MF101` | adding a unique index to an existing column | no | `MF101` | covered — structural: the build fails on the first duplicate; the message names the query that settles it and what a failed CONCURRENTLY build leaves behind, and a unique index the dev database already holds over the columns silences it |
| `MF102` | modifying a non-unique index to unique | no | `MF102` | covered — an index dropped earlier in the file and rebuilt as unique under the same name, or under a new name over the columns the dev database records for it; the message adds that the failure leaves the table without the index it had |
| `MF103` | adding a non-nullable column to an existing table | no | `DD101` | covered |
| `MF104` | modifying a nullable column to non-nullable might fail | no | `PG303`, `LT101`, `DD103` | covered — PG303 on PostgreSQL, CockroachDB, YugabyteDB and Spanner, LT101 on SQLite, DD103 on MySQL, MariaDB, SQL Server and ClickHouse; every engine measured, each with its own failure named |
| `BC101` | renaming a table | no | `BC101` | covered |
| `BC102` | renaming a column | no | `BC101` | covered — one rule reports both object kinds |
| `BC103` | dropping a table | no | `BC103` | covered — the rollout break, not the row loss DS101 reports on the same statement: separately suppressible, because an operator accepting the data loss has not accepted deployed clients failing; reported by `ptah migrations lint`, not by the compatibility surface and not by an apply gate |
| `BC104` | dropping a column | no | `BC104` | covered — as BC103, beside DS102, and reported on the same surface; a column of a table this migration itself created is exempt, which DS102 is deliberately not |
| `MY101` | adding a non-nullable column without a DEFAULT to an existing table | no | `DD101` | covered — DD101 applies to every dialect |
| `MY102` | an inline REFERENCES clause in ADD COLUMN has no effect | no | `MY102` | covered |
| `MY110` | removing enum values from a column requires a table copy | no | `MY110` | covered |
| `MY111` | reordering enum values requires a table copy | no | `MY111` | covered |
| `MY112` | inserting enum values other than at the end requires a table copy | no | `MY112` | covered |
| `MY113` | exceeding 256 enum values changes storage size and requires a table copy | no | `MY113` | covered |
| `MY120` | removing set values from a column requires a table copy | no | `MY120` | covered |
| `MY121` | reordering set values requires a table copy | no | `MY121` | covered |
| `MY122` | inserting set values other than at the end requires a table copy | no | `MY122` | covered |
| `MY123` | exceeding a set-size boundary changes storage size and requires a table copy | no | `MY123` | covered |
| `MY130` | changing a column type requires a table copy | yes | `MY130` | covered — fires only for a change InnoDB refuses to apply in place, with the old and new type and the boundary, character set, collation or key that decides it; MY130P is the info finding for the change applied in place |
| `MY131` | adding a foreign key blocks DML | yes | `MY131` | covered |
| `MY132` | adding a primary key requires a table rebuild | yes | `MY132` | covered |
| `MY133` | dropping a primary key without adding one requires a table copy | yes | `MY133`, `CD103` | covered — MY133 names the copy and CD103 the lost uniqueness guarantee; the message names the MariaDB case where another NOT NULL UNIQUE key keeps the change in place |
| `MY134` | adding a FULLTEXT index blocks DML | yes | `MY134` | covered |
| `MY135` | adding a SPATIAL index blocks DML | yes | `MY135` | covered |
| `MY136` | changing the table character set requires a table rebuild | yes | `MY136` | covered — names the columns whose re-encoding forces the copy; a conversion that touches no column, or only utf8mb3 to utf8mb4 on short VARCHAR and CHAR columns no key covers, is not reported |
| `MY137` | modifying the primary key rebuilds the table and its secondary indexes | yes | `MY137` | covered — one statement that drops and adds; measured in place with writes allowed, so it subsumes MY132 rather than repeating it, and leaves CD103 to report the identity loss |
| `MY138` | changing the storage engine requires a table copy and blocks DML | yes | `MY138` | covered — any ENGINE= clause, including one naming the engine the table already uses, which costs the same because it is the documented way to force a rebuild |
| `MY139` | partitioning or removing partitioning requires a table copy and blocks DML | yes | `MY139` | covered — measured, this statement form accepts no ALGORITHM or LOCK clause at all, so the finding says there is no online-DDL negotiation rather than naming a copy |
| `MY140` | adding a STORED generated column requires a table copy and blocks DML | yes | `MY140` | covered |
| `MY141` | adding an AUTO_INCREMENT column rebuilds the table and blocks DML | yes | `MY141` | covered — the rebuild is in place and writes still stop: LOCK=NONE is refused, which is why the finding reports cost and locking separately |
| `MY142` | adding a column before existing columns prevents an instant operation on older versions | yes | — | not implemented — measured absent on every MySQL release line this repository declares: ADD COLUMN ... FIRST accepts ALGORITHM=INSTANT on 8.4.11 and on 8.0.46, andthe lowest declared line is 8.4, so a rule would be a false positive wherever Ptah is tested |
| `MY143` | modifying a generated column requires a table copy and blocks DML | yes | `MY143` | covered — the MODIFY and CHANGE side; adding one is MY140, and DS103 still reports the type change where the declared type moves |
| `MY144` | adding a CHECK constraint scans all existing rows | yes | `MY144` | covered |
| `MY145` | modifying or enforcing a CHECK constraint re-validates all existing rows | yes | `MY145` | covered — MySQL only: MariaDB has no ENFORCED or NOT ENFORCED syntax, so a mariadb finding would describe a statement that server refuses to parse |
| `MY146` | dropping system versioning permanently deletes all history rows | yes | `MY146` | covered — MariaDB only: MySQL 8.4 has no system versioning at all; reported as an error because the history is deleted rather than made expensive |
| `MY147` | changing column nullability requires a table rebuild | yes | `MY147` | covered — the cost; measured in place with writes allowed. Whether an existing NULL fails the statement is the separate question DD103 answers from the baseline |
| `MY148` | changing a column character set or collation requires a table copy and blocks DML | yes | `MY130` | partial — MY130 names this consequence where it can prove the copy from the dev database; without that baseline, or on a spelling it cannot resolve to a before-and-after, the statement is not reported |
| `LT101` | modifying a nullable column to non-nullable without a DEFAULT | no | `LT101` | covered |
| `PG101` | index created without CONCURRENTLY | yes | `PG101` | covered |
| `PG102` | index dropped without CONCURRENTLY | yes | `PG106` | covered |
| `PG103` | concurrent operation without the atlas:txmode none header | yes | `PG103` | covered — the atlas:txmode none header and Ptah's own directive both silence it |
| `PG104` | PRIMARY KEY creation acquires an ACCESS EXCLUSIVE lock | yes | `PG104` | covered |
| `PG105` | UNIQUE constraint creation acquires an ACCESS EXCLUSIVE lock | yes | `PG105` | covered |
| `PG110` | creating a table with non-optimal data alignment | no | `PG110` | covered |
| `PG301` | a column type change requires a table and index rewrite | yes | `PG301` | covered — fires for a change PostgreSQL rewrites for, naming the abort a value can cause, and for a collation change on an indexed column, naming the indexes it rebuilds; PG301P is the info finding for a catalog edit |
| `PG302` | a volatile DEFAULT on an added column rewrites the table | yes | `PG302` | covered |
| `PG303` | SET NOT NULL scans existing rows | yes | `PG303` | covered |
| `PG304` | PRIMARY KEY on nullable columns requires a full scan | yes | `PG304`, `PG104` | covered — PG304 names the columns the key sets NOT NULL and the extra scan that costs, for a column list and for USING INDEX alike; PG104 names the lock every ADD PRIMARY KEY takes |
| `PG305` | a CHECK constraint requires a full table scan | yes | `PG305` | covered |
| `PG306` | a FOREIGN KEY requires a full scan and blocks writes | yes | `PG306` | covered |
| `PG307` | a logging-mode change rewrites the table | yes | `PG307` | covered |
| `PG308` | trigger creation acquires a SHARE ROW EXCLUSIVE lock | yes | `PG308` | covered |
| `PG309` | a STORED generated column rewrites the table | yes | `PG309` | covered |
| `PG310` | an identity column rewrites the table | yes | `PG310` | covered |
| `PG311` | an access-method change rewrites the table | yes | `PG311` | covered |
| `PG108` | an index on a partitioned table blocks writes on all its partitions | yes | `PG108` | partial — reported where the migration itself declares the parent PARTITION BY; the statement alone cannot say a table is partitioned, so an index on aparent created in an earlier release is left to PG101, whose CONCURRENTLY remedy the server refuses here |
| `PG109` | an EXCLUDE constraint takes an ACCESS EXCLUSIVE lock and scans the table | yes | `PG109` | covered |
| `PG312` | redefining a primary key builds its unique index under an ACCESS EXCLUSIVE lock | yes | `PG312` | covered — distinct from Ptah's own PG312P, which is about a SECURITY DEFINER routine and keeps its trailing P; the USING INDEX form builds nothing under the lock and is not reported |
| `PG314` | changing REPLICA IDENTITY to FULL or NOTHING risks the logical replication setup | yes | `PG314` | covered — FULL and NOTHING carry different consequences and are reported with different messages; DEFAULT and USING INDEX keep a usable row identity and are not reported |
| `PG320` | disabling autovacuum lets dead rows accumulate | yes | `PG320` | covered — the one rule here whose hazard is not a lock or a rewrite: the statement takes only a SHARE UPDATE EXCLUSIVE lock and the cost is paid later |
| `CD101` | a foreign-key constraint was dropped | yes | `CD101` | covered |
| `CD102` | a check constraint was dropped | yes | `CD102` | covered |
| `CD103` | a primary-key constraint was dropped | yes | `CD103` | covered |
| `TX101` | statements cannot run in a single transaction | yes | `TX101` | covered |
| `TX201` | a nested transaction was detected | yes | `TX201` | covered |
| `NM101` | a schema name violates the naming convention | no | `NM101` | covered — needs a configured naming convention |
| `NM102` | a table name violates the naming convention | no | `NM102` | covered — needs a configured naming convention |
| `NM103` | a column name violates the naming convention | no | `NM103` | covered — needs a configured naming convention |
| `NM104` | an index name violates the naming convention | no | `NM104` | covered — needs a configured naming convention; a unique or primary key constraint counts as an index, as it does for Atlas |
| `NM105` | a foreign-key constraint name violates the naming convention | no | `NM105` | covered — needs a configured naming convention |
| `NM106` | a check constraint name violates the naming convention | no | `NM106` | covered — needs a configured naming convention |
| `SA101` | a possible SQL injection vulnerability was detected | no | `SA101` | covered — reports a routine body that builds its statement from an unquoted value; a literal text, quote_ident/quote_literal, format's %I and %L, QUOTENAME, and parameters are the safe forms it leaves alone |
| `OW101` | a user is not authorized to modify a resource | yes | — | waived — binds to a schema-ownership annotation set and an account model Ptah does not have |
| `OW102` | a user is explicitly denied access to a resource | yes | — | waived — same reason as OW101 |

</div>

## Identifiers that predate the convention

14 identifiers were chosen before the convention above existed. Renaming one changes what `ptah-compat` prints, what a `.ptah-lint.yaml` selector matches, and what a SARIF consumer keys on, so they are recorded rather than rewritten. The list is pinned in `internal/lintcatalog`: a rule added from now on that does not follow the convention fails the check instead of joining it.

| Rule | Why it does not follow the convention |
| --- | --- |
| `DD101` | reports Atlas `MF103`, which the convention spells `MF103` |
| `DS101` | reports Atlas `DS102`, which the convention spells `DS102` |
| `DS102` | reports Atlas `DS103`, which the convention spells `DS103` |
| `DS103` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS103P` |
| `DS104` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS104P` |
| `DS105` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS105P` |
| `DS106` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS106P` |
| `DS107` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS107P` |
| `DS108` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS108P` |
| `DS109` | Ptah rule inside the Atlas `DS` family, which the convention spells `DS109P` |
| `MF103` | Ptah rule inside the Atlas `MF` family, which the convention spells `MF103P` |
| `MY101` | Ptah rule inside the Atlas `MY` family, which the convention spells `MY101P` |
| `PG102` | Ptah rule inside the Atlas `PG` family, which the convention spells `PG102P` |
| `PG106` | reports Atlas `PG102`, which the convention spells `PG102` |

<!-- END GENERATED LINT RULES -->
### Writing an Atlas code in a config

Where Ptah reports an Atlas hazard under a rule of its own name, the Atlas code
is accepted anyway — in `disabled-rules`, in `--disable`, and as a key under
`rules:` for a severity override. `BC102` reaches `BC101`, and `MF104` reaches
`PG303`, `LT101` and `DD103`. A code Ptah reports under the same name, every
`MY` and `PG3` code for one, needs no alias: it selects that rule directly.

An alias reaches its Ptah rule only for the engine the Atlas code belongs to,
so a policy shared across engines cannot weaken one engine's run through an
entry written for another. The two aliases above name no engine and apply
everywhere. A selector is still *accepted* on every dialect, so one policy file
can carry entries for several engines.

Six Atlas codes are deliberately **not** aliased, because Ptah uses the same
spelling for a rule of its own meaning something else: `DS101`, `DS102`,
`DS103`, `MF103`, `MY101` and `PG102`. Atlas `DS103` reports under Ptah `DS102`
while Ptah's own `DS103` is a different hazard, so an alias would make
`--disable DS103` silence two rules where you asked for one. In a config those
six select the Ptah rule of that name.


## Changing what a rule does

This section is about migration lint. Severity and per-path exclusions are
configured per rule; see [Configuration](../configuration/) for
`.ptah-lint.yaml` and the `lint` block of an Atlas-compatible project file. A
single statement is silenced with a `ptah:nolint` comment, and the
compatibility surface accepts `atlas:nolint` with the analyzer names and codes
it prints.

`ptah sql lint` takes none of that. It reads no policy file, honors no
`nolint` comment, and offers one control: `--disable`, repeatable, taking a
code or a family prefix. Its severities are the defaults listed above.
