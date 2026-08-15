---
title: Lint rules
description: Every rule Ptah's linters report, with its meaning, dialects, surface, and whether the name is Atlas's or Ptah's.
---

Ptah lints in two places, and both report findings under stable identifiers.
Migration lint reads a migration directory; SQL lint reads standalone `.sql`
files. This page enumerates every identifier either one can report.

The tables below are generated from the rule registries themselves.
`scripts/check-lint-rules.sh` fails when a rule exists in the code and not on
this page, or when a row here names a rule that no longer exists.

## Which commands lint

| Command | Surface | Rules it reports |
| --- | --- | --- |
| `ptah migrations lint` | native | every migration lint rule |
| `ptah migrations up` | native | blocking `DS` findings only |
| `ptah sql lint` | native | every SQL lint rule |
| `ptah-compat migrate lint` | compatibility | every migration lint rule |
| `ptah-compat schema apply` | compatibility | only what `atlas.hcl` names |

Both surfaces read one registry, so a rule reaches both unless its row says
otherwise. Native may carry rules the compatibility surface does not; the
reverse would mean a rule that exists only to match another tool, and there are
none.

The two apply gates are the rows to read carefully. Neither runs the whole rule
set, so a rule appearing in the tables below is not by itself a check standing
between an apply and a database. The migration lint section states what each
gate does run, generated from the gates themselves.

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

<!-- BEGIN GENERATED LINT RULES -->
## Identifier families

An identifier's prefix says whose namespace it lives in. Atlas owns a prefix when the Atlas analyzer documentation uses it; the rest are Ptah's.

| Prefix | Namespace | What the family covers |
| --- | --- | --- |
| `BC` | Atlas | changes that break code already deployed against the old schema |
| `CAP` | Ptah | the target server version lacks a capability the statement needs |
| `CD` | Atlas | constraint deletions, split by the constraint type the SQL names |
| `DD` | Ptah | changes whose outcome depends on the rows already in the table |
| `DDL` | Ptah | the shape of a DDL statement the SQL linter modeled |
| `DS` | Atlas | destructive changes: statements that delete data or drop objects |
| `LT` | Atlas | SQLite-specific hazards |
| `MF` | Atlas | Atlas: changes that may fail. Ptah: migration file form |
| `MY` | Atlas | MySQL and MariaDB-specific rebuild and blocking-DDL hazards |
| `NM` | Atlas | naming conventions; Atlas documents these, Ptah emits none |
| `OW` | Atlas | ownership policy; Atlas documents these, Ptah emits none |
| `PG` | Atlas | PostgreSQL-specific locking, rewrite, and transaction hazards |
| `SA` | Atlas | static analysis; Atlas documents these, Ptah emits none |
| `SQL` | Ptah | the SQL linter could not read or model the statement |
| `TX` | Atlas | transaction shape of a migration |

## Migration lint rules

42 rules, registered in `migration/lint`. `ptah migrations lint` and `ptah-compat migrate lint` report the whole registry. Neither apply gate does, so a rule listed below is not by itself a check that stands between an apply and a database: `ptah migrations up` disables the `MF`, `BC`, `PG` and `MY` families and refuses only on blocking `DS` findings, and `ptah-compat schema apply` runs only the rules an `atlas.hcl` `lint` block names, which means a project without such a block gets no lint pass there at all. The tables are grouped by the dialects each rule applies to, which is why they carry no dialect column.

### Every dialect

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `BC101` | a rename retires a name deployed code still refers to | native only | Atlas |
| `CD101` | dropping a foreign key removes referential-integrity enforcement | both | Atlas |
| `CD102` | dropping a check constraint removes a value-validation guarantee | both | Atlas |
| `CD103` | dropping a primary key removes row identity and can break replication | both | Atlas |
| `DD101` | adding a NOT NULL column without a default fails or blocks on a populated table | both | Atlas |
| `DS101` | DROP TABLE, or a rename that retires the name, destroys the table and every row in it | both | Atlas |
| `DS102` | DROP COLUMN destroys the column and every value stored in it | both | Atlas |
| `DS103` | a column type change can truncate or reject existing values and rewrite the table | both | Ptah |
| `DS104` | DROP NOT NULL removes a column-level data protection | both | Ptah |
| `DS105` | an untyped DROP CONSTRAINT removes a data protection the SQL does not name | both | Ptah |
| `DS106` | removing an enum value can invalidate rows that still hold it | both | Ptah |
| `DS107` | dropping a schema, type, extension, function, role, or policy removes behavior | both | Ptah |
| `DS108` | TRUNCATE deletes every row in the table | both | Ptah |
| `DS109` | DISABLE ROW LEVEL SECURITY removes an access-control protection | both | Ptah |
| `MF101` | no matching .down.sql exists, so a failed deploy cannot be rolled back mechanically | both | Ptah |
| `MF102` | the migration carries no executable statements | both | Ptah |
| `MF103` | the file name does not follow the migration file-name convention | both | Ptah |

### mysql, mariadb

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `MY101` | this ALTER TABLE form rebuilds the table and blocks writes for the duration | both | Ptah |
| `MY102` | MySQL ignores an inline REFERENCES clause in ADD COLUMN | both | Atlas |
| `MY131` | adding a foreign key can copy or lock the table and block writes | both | Atlas |
| `MY132` | adding a primary key rebuilds the table and blocks DML | both | Atlas |
| `MY134` | adding a FULLTEXT index can rebuild the table and block writes | both | Atlas |
| `MY135` | adding a SPATIAL index can rebuild the table and block writes | both | Atlas |

### postgres

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `PG101` | CREATE INDEX without CONCURRENTLY blocks writes for the whole build | both | Atlas |
| `PG102` | ALTER TYPE ... ADD VALUE cannot run inside a transaction block | both | Ptah |
| `PG103` | CONCURRENTLY cannot run inside the migration's transaction | both | Atlas |
| `PG104` | adding a primary key takes an ACCESS EXCLUSIVE lock and scans existing rows | both | Atlas |
| `PG105` | adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates rows | both | Atlas |
| `PG106` | DROP INDEX without CONCURRENTLY blocks writes while the index is removed | both | Atlas |
| `PG110` | the declared column order wastes tuple padding | both | Atlas |
| `PG302` | a volatile DEFAULT on an added column rewrites or evaluates every existing row | both | Atlas |
| `PG303` | SET NOT NULL scans the table to validate existing rows | both | Atlas |
| `PG305` | adding a CHECK constraint validates existing rows and holds locks | both | Atlas |
| `PG306` | adding a foreign key validates existing rows and can block writes on both tables | both | Atlas |
| `PG307` | changing LOGGED or UNLOGGED rewrites the table under heavyweight locks | both | Atlas |
| `PG308` | CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block writes | both | Atlas |
| `PG309` | adding a STORED generated column computes and stores a value for every row | both | Atlas |
| `PG310` | adding an identity column can rewrite existing rows | both | Atlas |
| `PG311` | changing a table's access method rewrites the table | both | Atlas |
| `TX101` | the migration mixes statements that cannot share one transaction | both | Atlas |
| `TX201` | an explicit BEGIN/COMMIT block fights the migrator's transaction management | both | Atlas |

### sqlite

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `LT101` | SQLite cannot enforce NOT NULL on existing nullable data without a rebuild | both | Atlas |

## SQL lint rules

4 rules, reported by `ptah sql lint` over standalone SQL files, on every dialect. The compatibility surface has no verb that reaches them.

| Rule | Meaning | Surface | Origin |
| --- | --- | --- | --- |
| `CAP001` | the statement needs a capability the target server version does not have | native only | Ptah |
| `DDL001` | the created table declares no primary key | native only | Ptah |
| `SQL001` | the SQL parser could not build an AST, so no rule could inspect the statement | native only | Ptah |
| `SQL002` | the statement uses a sub-language `ptah sql lint` does not model yet | native only | Ptah |

## Default severities

14 rules report at error severity by default: `CAP001`, `CD101`, `CD102`, `CD103`, `DS101`, `DS102`, `DS104`, `DS105`, `DS106`, `DS107`, `DS108`, `DS109`, `SQL001`, `SQL002`. The other 32 default to warning. A committed `.ptah-lint.yaml` replaces either, per rule or per family.

## What ptah-compat prints

Every migration lint finding reports under an analyzer name and a code on the compatibility surface. Rules not listed here keep their own code under the `ptah` analyzer.

| Native rule | Analyzer | Code |
| --- | --- | --- |
| `DD101` | `data_depend` | `MF103` |
| `DS101` | `destructive` | `DS102` |
| `DS102` | `destructive` | `DS103` |

## Atlas analyzer checks

Every check code the [Atlas analyzer documentation](https://atlasgo.io/lint/analyzers) carries, and what Ptah does about it: 33 covered, 7 partial, 16 not implemented, 2 waived, of 58. A code Atlas marks as an Atlas Pro feature is marked here too, and the ones Ptah implements are reported through both surfaces except `BC101` and `BC102`, whose Ptah rule the compatibility surface does not report.

| Atlas check | Meaning | Pro | Ptah rule | Status |
| --- | --- | --- | --- | --- |
| `DS101` | schema was dropped | no | `DS107` | covered |
| `DS102` | table was dropped | no | `DS101` | covered |
| `DS103` | non-virtual column was dropped | no | `DS102` | covered |
| `MF101` | adding a unique index to an existing column | no | — | not implemented |
| `MF102` | modifying a non-unique index to unique | no | — | not implemented |
| `MF103` | adding a non-nullable column to an existing table | no | `DD101` | covered |
| `MF104` | modifying a nullable column to non-nullable might fail | no | `PG303`, `LT101` | partial — reported on PostgreSQL and SQLite; the other dialects have no equivalent rule |
| `BC101` | renaming a table | no | `BC101` | covered |
| `BC102` | renaming a column | no | `BC101` | covered — one rule reports both object kinds |
| `MY101` | adding a non-nullable column without a DEFAULT to an existing table | no | `DD101` | covered — DD101 applies to every dialect |
| `MY102` | an inline REFERENCES clause in ADD COLUMN has no effect | no | `MY102` | covered |
| `MY110` | removing enum values from a column requires a table copy | no | `DS106` | partial — reported as a destructive enum change, without the table-copy cost |
| `MY111` | reordering enum values requires a table copy | no | — | not implemented |
| `MY112` | inserting enum values other than at the end requires a table copy | no | — | not implemented |
| `MY113` | exceeding 256 enum values changes storage size and requires a table copy | no | — | not implemented |
| `MY120` | removing set values from a column requires a table copy | no | — | not implemented |
| `MY121` | reordering set values requires a table copy | no | — | not implemented |
| `MY122` | inserting set values other than at the end requires a table copy | no | — | not implemented |
| `MY123` | exceeding a set-size boundary changes storage size and requires a table copy | no | — | not implemented |
| `MY130` | changing a column type requires a table copy | yes | `MY101`, `DS103` | partial — MODIFY and CHANGE are reported as lock-heavy DDL; the table-copy cost has no code |
| `MY131` | adding a foreign key blocks DML | yes | `MY131` | covered |
| `MY132` | adding a primary key requires a table rebuild | yes | `MY132` | covered |
| `MY133` | dropping a primary key without adding one requires a table copy | yes | `CD103` | partial — the drop is reported; the table-copy cost has no code |
| `MY134` | adding a FULLTEXT index blocks DML | yes | `MY134` | covered |
| `MY135` | adding a SPATIAL index blocks DML | yes | `MY135` | covered |
| `MY136` | changing the table character set requires a table rebuild | yes | `MY101` | partial — only the CONVERT TO CHARACTER SET and CONVERT TO CHARSET spellings are scanned |
| `LT101` | modifying a nullable column to non-nullable without a DEFAULT | no | `LT101` | covered |
| `PG101` | index created without CONCURRENTLY | yes | `PG101` | covered |
| `PG102` | index dropped without CONCURRENTLY | yes | `PG106` | covered |
| `PG103` | concurrent operation without the atlas:txmode none header | yes | `PG103` | covered — the atlas:txmode none header and Ptah's own directive both silence it |
| `PG104` | PRIMARY KEY creation acquires an ACCESS EXCLUSIVE lock | yes | `PG104` | covered |
| `PG105` | UNIQUE constraint creation acquires an ACCESS EXCLUSIVE lock | yes | `PG105` | covered |
| `PG110` | creating a table with non-optimal data alignment | no | `PG110` | covered |
| `PG301` | a column type change requires a table and index rewrite | yes | `DS103` | partial — reported as a data-safety risk, without rewrite and lock analysis |
| `PG302` | a volatile DEFAULT on an added column rewrites the table | yes | `PG302` | covered |
| `PG303` | SET NOT NULL scans existing rows | yes | `PG303` | covered |
| `PG304` | PRIMARY KEY on nullable columns requires a full scan | yes | `PG104` | partial — every ADD PRIMARY KEY is reported; the nullable-column refinement needs schema state |
| `PG305` | a CHECK constraint requires a full table scan | yes | `PG305` | covered |
| `PG306` | a FOREIGN KEY requires a full scan and blocks writes | yes | `PG306` | covered |
| `PG307` | a logging-mode change rewrites the table | yes | `PG307` | covered |
| `PG308` | trigger creation acquires a SHARE ROW EXCLUSIVE lock | yes | `PG308` | covered |
| `PG309` | a STORED generated column rewrites the table | yes | `PG309` | covered |
| `PG310` | an identity column rewrites the table | yes | `PG310` | covered |
| `PG311` | an access-method change rewrites the table | yes | `PG311` | covered |
| `CD101` | a foreign-key constraint was dropped | yes | `CD101` | covered |
| `CD102` | a check constraint was dropped | yes | `CD102` | covered |
| `CD103` | a primary-key constraint was dropped | yes | `CD103` | covered |
| `TX101` | statements cannot run in a single transaction | yes | `TX101` | covered |
| `TX201` | a nested transaction was detected | yes | `TX201` | covered |
| `NM101` | a schema name violates the naming convention | no | — | not implemented |
| `NM102` | a table name violates the naming convention | no | — | not implemented |
| `NM103` | a column name violates the naming convention | no | — | not implemented |
| `NM104` | an index name violates the naming convention | no | — | not implemented |
| `NM105` | a foreign-key constraint name violates the naming convention | no | — | not implemented |
| `NM106` | a check constraint name violates the naming convention | no | — | not implemented |
| `SA101` | a possible SQL injection vulnerability was detected | no | — | not implemented |
| `OW101` | a user is not authorized to modify a resource | yes | — | waived — binds to a schema-ownership annotation set and an account model Ptah does not have |
| `OW102` | a user is explicitly denied access to a resource | yes | — | waived — same reason as OW101 |

## Identifiers that predate the convention

16 identifiers were chosen before the convention above existed. Renaming one changes what `ptah-compat` prints, what a `.ptah-lint.yaml` selector matches, and what a SARIF consumer keys on, so they are recorded rather than rewritten. The list is pinned in `internal/lintcatalog`: a rule added from now on that does not follow the convention fails the check instead of joining it.

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
| `MF101` | Ptah rule inside the Atlas `MF` family, which the convention spells `MF101P` |
| `MF102` | Ptah rule inside the Atlas `MF` family, which the convention spells `MF102P` |
| `MF103` | Ptah rule inside the Atlas `MF` family, which the convention spells `MF103P` |
| `MY101` | Ptah rule inside the Atlas `MY` family, which the convention spells `MY101P` |
| `PG102` | Ptah rule inside the Atlas `PG` family, which the convention spells `PG102P` |
| `PG106` | reports Atlas `PG102`, which the convention spells `PG102` |

<!-- END GENERATED LINT RULES -->

## Changing what a rule does

Severity and per-path exclusions are configured per rule; see
[Configuration](../configuration/) for `.ptah-lint.yaml` and the `lint` block
of an Atlas-compatible project file. A single statement is silenced with a
`ptah:nolint` comment, and the compatibility surface accepts `atlas:nolint`
with the analyzer names and codes it prints.
