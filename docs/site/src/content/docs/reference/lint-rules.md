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
  - "internal/providerpublished"
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
this page, or when a row here names a rule that no longer exists. The page
reads the engine owners' rules from the bundled runtime. For an official
provider's engine the bundle does not serve, it reads the rule catalog the
provider publishes, from a copy recorded from a provider build under
`internal/providerpublished`. The generator can also read them from a runtime
that holds an executable provider; the meanings stay on this page either way.

## Which commands lint

| Command | Surface | Rules it reports |
| --- | --- | --- |
| `ptah migrations lint` | native | every migration lint rule |
| `ptah migrations up` | native | blocking data-safety findings only (`destructive/drop-table` and the rules listed below) |
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
| extension advisory assessment | The migration SQL, plus what each extension statement installs on the dev database and what reviewed advisories say about it; see [below](#extensions-a-migration-installs). |

The declaration decides what a run reads, and what it says when a read is
missing:

- **Only the versions a rule asks about are read.** A directory with nothing to
  resolve costs no introspection, and a rule silenced by `--disable`, by
  `atlas:nolint`, or by a reviewed-schema scope that excludes its statements
  costs none either.
- **A rule that asks and gets nothing says so.** When the run supplies no
  starting state for a version a rule asked about, both surfaces print a
  warning on **stderr** naming the rule. Stdout bytes and the exit code are
  unchanged, so nothing that parses the report is affected:

  ```text
  warning: data-dependent/add-not-null-column ran without the baseline schema it reads, so this analysis is
  thinner than the same directory would get against a dev database the run can read
  ```

  A clean report with that line on stderr means the analysis was narrower than
  the directory allows, not that the directory is clean.

`data-dependent/add-not-null-column` reads the baseline for the add side of a column
rename on the compatibility surface. The native surface models a rename as a
rename, so it asks for no starting state and never prints the notice.

`locking/create-index` reads the starting state to learn whether the indexed table is a
TimescaleDB hypertable. TimescaleDB refuses `CREATE INDEX CONCURRENTLY` on one,
so with the state in hand the finding names the extension's per-chunk build
instead. It still reports from the statement text without a dev database, and
the notice above then names it on both surfaces.

`locking/partitioned-index` reads the same state to learn whether a table an earlier migration
created is partitioned, where PostgreSQL refuses `CONCURRENTLY` and the build
locks every partition. It reports from the text alone when the migration creates
the parent itself. Where it fires it replaces `locking/create-index`, and with the state in
hand `locking/create-index` also leaves `CREATE INDEX ... ON ONLY` a partitioned parent alone,
because that creates the parent's index without reading a row.

`destructive/drop-table` recognizes a table rebuild from the text: a new table, a copy of every
row of the old one into it, and the copy renamed over the old name, as
`migrations generate` writes one for SQLite, and for YDB through its provider,
with `--allow-table-rebuild`. On YDB the steps may have the copy's index settings,
the old table's changefeeds dropped before it moves and the copy's added after,
and their consumers between them, none of which writes a row or moves a table.
The drop that ends it loses no row, so `destructive/drop-table`,
`rollout/rename` and `rollout/drop-table` do not report its steps on the native surface.

A ClickHouse rebuild is not one of those shapes: it creates the copy, fills it,
swaps the two tables with `EXCHANGE TABLES` and drops the old table under the
copy's name. `destructive/drop-table` reads the swap, so it reports that drop as
the drop of a table holding rows, and the report is meant to stand: the plan
marks the rebuild destructive because the steps are not atomic and rows written
during them are lost, which a reviewer has to see. The swap moves no name
deployed code reads, so `rollout/drop-table` does not report it.

A copy that keeps every row can still lose a column's values. The `INSERT`
pairs its columns with the `SELECT` list by position, so each item has to be
the old column the `INSERT` names in that place: bare, qualified by the table
or its alias, or inside `IFNULL` or `COALESCE`, which replace only a `NULL`.
An `INSERT` with no column list fills the copy's columns in the order its
`CREATE TABLE` declares them. `SELECT *` fills them in the old table's order,
so it keeps a column only where the copy declares it in the same place. An
item that writes `NULL`, a constant, another column, a `CAST` or any other
expression loses the column, and so does a column the copy leaves out. A word
after a column is read as its alias only when it is a name: `label ISNULL` is
the expression `label IS NULL`.

Only the old table's columns show which columns the copy has to carry:
`destructive/drop-table` reads them from the starting state and reports the drop when the copy
loses one. Without a dev database, a column the copy fills from anything but
itself may be one of the old table's, and so may a column `SELECT *` moves, so
`destructive/drop-table` reports that drop too, and the notice above names `destructive/drop-table`. On YDB the provider reads the old table's columns
from the directory's earlier migrations and reports a copy that leaves one
out; it does not read the `SELECT` list.

A drop of something that never held data destroys nothing and retires no name a
deployed client read. The community binary compares the schema a version starts
from with the one it ends with and reports nothing for it, and the drop rules
leave it alone on both surfaces:

- `destructive/drop-column` and `rollout/drop-column` skip a column of a table the same migration created, and
  a column an earlier `ALTER TABLE ... ADD` of the migration added. A column
  added with `IF NOT EXISTS` may have been there already, and a rename may give
  a table or a column a name that held data, so neither is skipped.
- `destructive/drop-column` and `rollout/drop-column` also skip a generated column the migration drops and
  adds back to the same table as a generated column, `GENERATED ALWAYS AS
  (...)`. It holds no value a client wrote, the server computes every row
  again on the add, and the column keeps its name. A plan writes this shape
  to change the type of a column a PostgreSQL generated column reads. Only the
  starting state says the column was generated: the same statements over a
  column that stores what clients wrote delete every value, and a deployed
  client that writes the column fails, so that drop is reported. The state
  comes from the dev database; the apply gate of `ptah migrations up` reads
  the target's generated columns before it refuses. A run with neither, or a
  statement before the drop that may have changed the column, leaves the drop
  reported. A column added back as an identity column is still a drop, since
  the add numbers the rows again. Under `ptah-compat` the add has to be in the
  drop's own `ALTER TABLE`: the community binary reports the drop where the
  add is a statement of its own.
- `destructive/drop-table` and `rollout/drop-table` skip a `DROP TABLE IF EXISTS` of a table the starting
  state does not hold. The summary of `ptah-compat migrate lint` counts no
  schema change for it. A schema the state does not hold, and a qualified name
  against a state that names no schema, leave the drop reported.

The rules that exempt a table the migration created (`destructive/drop-table`, `destructive/drop-column`,
`rollout/rename`, `rollout/drop-table`, `rollout/drop-column`, `data-dependent/add-not-null-column`, `data-dependent/add-unique-index` and `data-dependent/rebuild-index-as-unique`) count a
`CREATE TABLE IF NOT EXISTS` as a creation only when the starting state holds
no such table, since the statement may find the table there already, holding
rows. Where the state holds it, the summary counts no schema change for the
statement.

Both answers about a table's absence come from the dev database, so without
one the statement is reported as any other and the notice above names the
rules. They also need the name to be untouched before the statement: an
earlier statement that names the table may have created it or renamed another
table to it, and a statement whose effect its words do not show, such as a
`DO` block, a `CALL`, a prepared statement or a `SET` of the search path, may
have done either out of sight. After either, the statement is reported too.

### Extensions a migration installs

`security/vulnerable-extension` and `coverage/unassessed-extension` read another
input, the extension advisory assessment. Lint reads each `CREATE EXTENSION`
and `ALTER EXTENSION ... UPDATE` of an up file, with its name and version
decoded as the server decodes them: a quoted or `U&` identifier, and an escape,
`U&` or dollar-quoted string. After the replay, the run asks the dev database
what each statement installs: the version the statement names or the server's
default, and with `CASCADE` each extension that version requires. It matches
each install against the reviewed advisories that
[`ptah db vulnerabilities`](../../operate/vulnerability-scan/) reads, from the
same [advisory source](../../operate/advisory-source/), and reports at the
statement's line:

- `security/vulnerable-extension`, a warning, for an extension an advisory
  affects on the dev database's server, with the advisory and its fixed release;
- `coverage/unassessed-extension`, info, for one the data cannot call clean: no
  reviewed data covers it, the data is past its age, the banner names a vendor
  build, or a run that named the source could not read it. A statement lint
  recognizes and cannot read, such as a version written as a number or an
  option the statement does not take, is reported the same way, with the
  reason.

A statement is reported for what it adds to the database. Before each version
that installs an extension, the run reads which extensions the dev database
holds, and it follows the file's statements in order from there:

- `CREATE EXTENSION IF NOT EXISTS` of an extension the database holds installs
  nothing and is not reported;
- `CASCADE` reports each extension it adds, and leaves out one the database
  holds or an earlier statement installed;
- `ALTER EXTENSION ... UPDATE` to the version the database holds is not
  reported;
- `DROP EXTENSION` takes what it names out of the held set, and with `CASCADE`,
  which may drop more than it names, the held set is emptied;
- a `DO` block empties the held set too, since it may have dropped anything.

Lint does not read a `DO` block or a routine body, so an extension installed
inside one is not assessed.

With `--latest`, the versions before the window are replayed and not
reported. An extension one of them installs is held when the window starts, so
a `CREATE EXTENSION IF NOT EXISTS` inside the window reports nothing for it.
Lint the whole directory to assess every extension it installs.

The run assesses the dev database's server, so run it at the release the
migration will meet. PostgreSQL is the only engine assessed. The official
`postgres` images, on Debian and on Alpine, carry an upstream build, so a
release line the advisory data covers gets an assessed result. A server built from a distribution's own package, such as
Ubuntu's, may carry a backported fix, so the assessment reads it as a vendor
build and each install there is reported as unassessed. Without a dev database
neither rule reports, and the notice above names both. A default run that
cannot read the advisory source matches the snapshot built into the binary and
says so on stderr once, in the line `ptah db vulnerabilities` writes. An
`ALTER EXTENSION ... UPDATE` replays only on a server Ptah provisions from a
`docker://` or `docker+<driver>://` dev URL. `ptah-compat migrate lint` reports
neither rule.

## How rules are named

A rule is identified by its Ptah name, `category/name`, such as
`destructive/drop-table`. The category says what the statement risks, not which
engine it runs on, so one name can cover the same hazard on several engines,
each checked by its own owner, and a name can appear once in each linter. The
rule tables list the names, and a finding carries its name as the rule that
reported it. Ptah rules have no numeric code.

Wherever a selector names a rule, it is a name or a category; [Rule names and
categories](#rule-names-and-categories) says how they match. A word written as
a code, such as `DS102`, is refused with `lint rules are named category/name;
DS102 is not a rule name`. The Atlas check codes belong to the compatibility
surface, and the [Atlas check codes](#atlas-check-codes) table below maps each
rule to the check it reports.

## How the analysis is performed

Three kinds of analyzer are worth telling apart, because they fail differently
and because two of the three are not built. The table records the behavior the
rule registry and runners implement.

| Kind | Ptah behavior |
| --- | --- |
| **Builtin** | Every rule on this page. It decides from what it is handed — the migration SQL, or the SQL file — and reaches no server and no other process. |
| **Server-assisted** | The `baseline schema` and `extension advisory assessment` inputs above, read from the dev database `--dev-url` names after the replay. Each is optional, and its absence is announced rather than absorbed. |
| **Optional provider** | None. No external analyzer is integrated, and no analysis path starts another process. The only process any lint path runs is `git`, to resolve `--git-base`. |

The server-assisted one is optional in the strict sense: without a dev database
the rules that wanted it resolve nothing and report less, the command still
exits 0, and **the run says so on stderr, naming each rule that asked** — the
report on stdout is left byte-identical so a `--format json` consumer and a
compatibility consumer both keep parsing it. A gap that only shows as a smaller
report is the hardest kind to notice from CI.

That leaves one thing to know about a clean lint result: it means every builtin
rule looked and found nothing. It does not mean a routine body was read, or that a
statement is SQL at all — see `coverage/routine-body`, `coverage/unparsed-statement` and `coverage/unanalyzed-statements`, which exist to keep
those apart.

`TX201` judges a file by its directory's layout. In Atlas's and Ptah's layouts
the migrator opens a transaction around each file, or runs the file without one
when it carries `-- atlas:txmode none`, and refuses a statement that controls
transaction state in either mode: `BEGIN`, `START TRANSACTION`, `COMMIT`,
`ROLLBACK`, `SAVEPOINT`, `SET TRANSACTION` and the like. The rule reports the
statement that opens each such block, in an up file and in a down file, on every
engine the migrator applies, so the failure shows before a deploy. A run that
names no dialect reads the statements with the generic grammar and reports
them the same way. The pinned Atlas community binary lints
such a file clean and applies a `txmode none` one, so `ptah-compat` is stricter
here ([Compatibility differences](../../atlas/retained-divergences/#a-txmode-none-file-that-controls-its-own-transaction)).

golang-migrate opens no transaction around a file, and on PostgreSQL the
migrator runs an up file of its directory with the transactions the file opens
itself, and rolls a down file back the same way. `transaction/explicit-transaction` reads either file the
same way. It reports a form the migrator refuses before any migration runs or
is rolled back, such as `ROLLBACK` or a block the file leaves open, and a
`BEGIN` inside an open block, which PostgreSQL ignores. A block the file opens
and closes is not reported. `transaction/concurrent-index` passes over a file of one statement, which
runs without a transaction.

On every other engine the migrator keeps its transaction around the file, so
`transaction/explicit-transaction` reports a block there as in any other layout. A run that names no
dialect reads a file the PostgreSQL way, since it cannot know the engine; name
the dialect to judge the file for another engine. Lint does not see the run's
transaction mode, and an apply with `--tx-mode all` refuses every file that
opens its own transaction;
[Transaction blocks in a golang-migrate directory](../../atlas/migrate-commands/#transaction-blocks-in-a-golang-migrate-directory)
has the measurement. Every other layout is judged as one whose tool wraps each
file.

`ptah migrations lint --runner golang-migrate`, or `migration.runner:
golang-migrate` in `ptah.yaml`, judges a directory in Ptah's or Atlas's layout
the same way, for a team that applies it with golang-migrate.

### A statement the parser could not read

`coverage/unparsed-statement` reports a statement the SQL parser could not read and no server ran. Past a
statement's opening words, text that is no SQL and valid SQL the parser does not model
read alike to it: it refuses `CREATE TABLE b BOGUS`, and it also refuses
`CREATE COLLATION` and `ALTER TABLE t ENGINE=InnoDB`, which servers run. So the rule
reports at `info` severity and keeps the exit code. A refusal would fail correct files.

`--dev-url` settles it for the up files, since a server runs them and refuses what
is not SQL, and `coverage/unparsed-statement` then reports only the down files, which no server runs.
With no dev database, `rules: { coverage/unparsed-statement: { severity: error } }` in `.ptah-lint.yaml`
or `--fail-on any` makes it a gate, and `-- ptah:nolint coverage/unparsed-statement` above a statement
silences it.

`ptah-compat migrate lint` with a dev database reports none, because the pinned
binary says nothing about a statement that replayed.

`ptah sql lint` has no replay to defer to, so a statement nothing read is an error
there. The parser tells two of the failures apart:

| The statement | Example | Finding | Exit code |
| --- | --- | --- | --- |
| Reads to its end | `DROP VIEW v`, `TRUNCATE t`, Spanner `CREATE VIEW v SQL SECURITY INVOKER AS SELECT 1` | `coverage/unanalyzed-statements` at `info`, for no rule looked at it | 0 |
| Opens with words no engine has, or is malformed | `DROP TABEL t`, `DROP VIEW @@@`, SQLite `DROP TABLE t, u` | `correctness/syntax-error` | 1 |
| Nothing read it, and a server may accept it | `CREATE COLLATION c (...)`, `CREATE TABLE b BOGUS`, `(SELECT 1)` | `coverage/unsupported-statement` | 1 |

`coverage/unsupported-statement` keeps the exit code because nothing checked the statement. A form the parser
does not hold is also where a real mistake sits, and only a server can tell them apart.
A dialect's form decides what reads: `TRUNCATE t` reads on PostgreSQL, and is `coverage/unsupported-statement` on
SQLite, whose server refuses it.
PostgreSQL `DROP TABLE t SELECT 1` also exits `1`: the parser refuses the
trailing clause instead of treating `SELECT` as a second statement.

<!-- BEGIN GENERATED LINT RULES -->
## Rule names and categories

Every rule has a Ptah name, `category/name`, and no numeric code. A selector in `--disable`, `disabled-rules`, a `rules:` key, the `gate` section or a `ptah:nolint` comment is a rule name, or a category written as `locking` or `locking/`, which selects every rule named under it. A name or a category matches exactly, never as a prefix, and a name or category no rule has is refused, as is a word written as a code, such as `DS102`. One name may cover a hazard checked on several engines, each by its own owner, or read by both linters.

| Category | Rules | What it covers |
| --- | --- | --- |
| `capability` | 4 | the target server version lacks a capability the statement needs |
| `correctness` | 3 | the statement is wrong as written: malformed, naming a column the schema does not declare, or declaring a property its body breaks |
| `coverage` | 6 | something the linter did not read, so a clean result does not mean it was checked |
| `data-dependent` | 5 | the statement succeeds, fails or changes values depending on the rows the table already holds |
| `dependency` | 4 | the statement drops or renames something another object still uses |
| `design` | 3 | a schema choice that applies cleanly and costs later, in row padding, table bloat or rows with no key to address them by; silencing it hides advice, never a statement that fails or blocks |
| `destructive` | 9 | the statement deletes rows, values, objects or history for good |
| `file-form` | 4 | the migration file or its directory does not have the form the run reads |
| `integrity` | 5 | the statement removes a constraint or a NOT NULL the database enforces on the data |
| `locking` | 17 | the statement holds a lock that blocks reads or writes while it runs |
| `naming` | 10 | a name breaks the configured convention, or is longer than the server keeps |
| `online` | 2 | online mode, which the user selects, could not prove the statement runs without a lock that blocks reads or writes; its rules fire only in that mode, so `--disable locking` leaves them on |
| `rewrite` | 24 | the statement rewrites or copies every row of the table; an info rule here reports that the rewrite was checked for and is absent |
| `rollout` | 4 | the change breaks a client or a consumer deployed against the current schema |
| `security` | 8 | the statement weakens access control, writes a secret or an unquoted value into SQL, or installs an extension a published advisory affects |
| `side-effect` | 14 | the statement resets, ignores or leaves behind something its text does not show; silenced, a migration can succeed and leave the database different from what it says |
| `transaction` | 6 | the statement cannot run, or runs differently, inside the migration's transaction or query |
| `ydb` | 1 | a statement form only YDB refuses |

## Migration lint rules

120 rules, registered in `migration/lint`. `ptah migrations lint` reports the whole registry, and `ptah-compat migrate lint` reports all of it but `coverage/unassessed-extension`, `file-form/refused-file`, `naming/identifier-collision`, `naming/identifier-too-long`, `rollout/rename` and `security/vulnerable-extension`, which only native `ptah` emits.

Neither apply gate reports even that much, so a rule listed below is not by itself a check that stands between an apply and a database. `ptah migrations up` disables 60 advisory rules (migration-file form, backward compatibility, and the locking and rewrite rules of PostgreSQL, MySQL and MariaDB) and refuses only on blocking findings of `destructive/drop-table`, `destructive/drop-column`, `dependency/drop-column-in-use`, `data-dependent/alter-column-type`, `integrity/drop-not-null`, `integrity/drop-untyped-constraint`, `destructive/remove-enum-value`, `destructive/drop-object`, `destructive/truncate-table`, `security/disable-row-level-security` and `security/no-force-row-level-security`, unless the policy's `gate` section names more rules or categories.

`ptah-compat schema apply` runs only the rules an `atlas.hcl` `lint` block names, which means a project without such a block gets no lint pass there at all. The tables are grouped by the dialects each rule applies to, which is why they carry no dialect column.

### Every dialect

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `correctness/immutable-calls-volatile` | a routine declared immutable calls something whose result changes between two calls with the same arguments | both | — |
| `coverage/routine-body` | the migration defines a routine whose body is not analyzed, so a clean result says nothing about what the body does | both | — |
| `coverage/unparsed-statement` | the SQL parser could not read a statement and no server ran it, so a clean result says nothing about it | both | — |
| `data-dependent/add-not-null-column` | adding a NOT NULL column without a default fails or blocks on a populated table | both | `MF103` |
| `data-dependent/add-unique-index` | a unique index built over existing rows fails on the first duplicate, and a failed CONCURRENTLY build leaves an invalid index behind | both | `MF101` |
| `data-dependent/alter-column-type` | a column type change can truncate or reject existing values and may rewrite the table under a lock; a clause that restates the column's current type, as the dev database records it, is not reported | both | — |
| `data-dependent/rebuild-index-as-unique` | an index dropped and rebuilt as unique fails on the first duplicate and leaves the table without it | both | `MF102` |
| `dependency/drop-column-in-use` | a column a view or routine still reads is dropped, and the finding names what breaks; a view dropped or rebuilt with a plain projection before the column drop is left out | both | — |
| `destructive/drop-column` | DROP COLUMN destroys the column and every value stored in it; an IF EXISTS drop of a column the dev database shows absent is not reported | both | `DS103` |
| `destructive/drop-object` | dropping a schema, type, extension, function, procedure, trigger, role or policy, or a YDB object the YDB table below names, removes behavior or data; an IF EXISTS drop of a type or a trigger the dev database shows absent is not reported | both | — |
| `destructive/drop-table` | DROP TABLE, and on MariaDB CREATE OR REPLACE TABLE, destroys the table and its rows. On the compatibility surface a rename reports here, retiring the old name without moving the rows. The native surface skips the drop that ends a table rebuild, whose copy keeps every row | both | `DS102` |
| `destructive/remove-enum-value` | removing an enum value can invalidate rows that still hold it | both | — |
| `destructive/truncate-table` | TRUNCATE deletes every row in the table | both | — |
| `file-form/empty-migration` | the migration carries no executable statements | both | — |
| `file-form/file-name` | the file name does not follow the migration file-name convention | both | — |
| `file-form/missing-down` | no matching .down.sql exists, so a failed deploy cannot be rolled back mechanically | both | — |
| `file-form/refused-file` | a run refuses the migration directory for this file: the directory's format does not read its name, another file carries its version and direction, or the revision table cannot keep its version | native only | — |
| `integrity/drop-check` | dropping a check constraint removes a value-validation guarantee | both | `CD102` |
| `integrity/drop-foreign-key` | dropping a foreign key removes referential-integrity enforcement | both | `CD101` |
| `integrity/drop-not-null` | DROP NOT NULL removes a column-level data protection | both | — |
| `integrity/drop-primary-key` | dropping a primary key removes row identity and can break replication | both | `CD103` |
| `integrity/drop-untyped-constraint` | an untyped DROP CONSTRAINT removes a data protection the SQL does not name | both | — |
| `naming/check-name` | a check constraint this migration names violates the configured naming convention | both | `NM106` |
| `naming/column-name` | a column this migration declares, adds, or renames to violates the configured naming convention | both | `NM103` |
| `naming/foreign-key-name` | a foreign key this migration names violates the configured naming convention | both | `NM105` |
| `naming/index-name` | an index or unique key this migration names violates the configured naming convention | both | `NM104` |
| `naming/schema-name` | a schema this migration creates or renames to violates the configured naming convention | both | `NM101` |
| `naming/table-name` | a table this migration creates or renames to violates the configured naming convention | both | `NM102` |
| `rollout/drop-column` | dropping a column retires a name deployed clients still select and insert, whether or not the column held rows | both | `BC104` |
| `rollout/drop-table` | dropping a table retires a name deployed clients still query, which is a rollout break a backup does not mitigate | both | `BC103` |
| `rollout/rename` | a rename retires a name deployed code still refers to | native only | `BC101` |
| `security/disable-row-level-security` | DISABLE ROW LEVEL SECURITY removes an access-control protection | both | — |
| `security/dynamic-sql-from-value` | a routine builds and runs a statement from a value it does not quote | both | `SA101` |
| `security/no-force-row-level-security` | NO FORCE ROW LEVEL SECURITY exempts the table owner from every policy on the table | both | — |

</div>

### mariadb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `destructive/drop-system-versioning` | DROP SYSTEM VERSIONING deletes every historical row version permanently, and no rollback restores it | both | `MY146` |

</div>

### mysql

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `locking/enforce-check` | enforcing a CHECK constraint revalidates every row; unenforcing one stops the server refusing the values it was there to refuse | both | `MY145` |

</div>

### mysql, mariadb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `locking/add-auto-increment-column` | adding an AUTO_INCREMENT column rebuilds the table in place and still blocks writes | both | `MY141` |
| `locking/add-fulltext-index` | adding a FULLTEXT index can rebuild the table and block writes | both | `MY134` |
| `locking/add-spatial-index` | adding a SPATIAL index can rebuild the table and block writes | both | `MY135` |
| `locking/blocking-alter-table` | this ALTER TABLE form usually rebuilds the table and blocks writes for the duration | both | — |
| `rewrite/add-column-before-existing` | adding a column FIRST or AFTER another cannot be instant on MySQL before 8.0.29 and MariaDB before 10.4, so the server rebuilds the table | both | `MY142` |
| `rewrite/add-primary-key` | adding a primary key rebuilds the table in place around the new clustered index | both | `MY132` |
| `rewrite/change-engine` | changing the storage engine copies the table and blocks writes for the duration | both | `MY138` |
| `rewrite/change-partitioning` | a partitioning change rewrites every row and accepts no ALGORITHM or LOCK clause to soften it | both | `MY139` |
| `rewrite/convert-charset` | converting a table's character set re-encodes its columns and copies the table | both | `MY136` |
| `rewrite/drop-primary-key` | dropping a primary key without adding one in the same statement copies the table | both | `MY133` |
| `rewrite/insert-enum-value` | inserting an ENUM member before the end renumbers the members after it and copies the table | both | `MY112` |
| `rewrite/insert-set-member` | inserting a SET member before the end renumbers the members after it and copies the table | both | `MY122` |
| `rewrite/modify-generated-column` | changing a STORED generated column recomputes it for every row, copying the table and blocking writes | both | `MY143` |
| `rewrite/remove-enum-value` | removing an ENUM member copies the table, and a row still holding it fails the copy in strict mode | both | `MY110` |
| `rewrite/remove-set-member` | removing a SET member copies the table, and a row still holding it fails the copy in strict mode | both | `MY120` |
| `rewrite/reorder-enum-values` | reordering ENUM members renumbers every row and copies the table | both | `MY111` |
| `rewrite/reorder-set-members` | reordering SET members renumbers every row and copies the table | both | `MY121` |
| `rewrite/replace-primary-key` | replacing a primary key rebuilds the table and every secondary index that stores it as a row pointer | both | `MY137` |
| `rewrite/widen-enum` | growing an ENUM past 255 members widens each value to two bytes and copies the table | both | `MY113` |
| `rewrite/widen-set` | growing a SET across a multiple of eight members adds a byte to each value and copies the table | both | `MY123` |
| `side-effect/inline-reference` | an inline REFERENCES clause on a column is ignored by MySQL and enforced by MariaDB | both | `MY102` |

</div>

### mysql, mariadb, clickhouse, sqlserver, spanner

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `data-dependent/set-not-null` | a nullable column made NOT NULL meets the rows the table already holds: MySQL, MariaDB, SQL Server and ClickHouse fail on a row holding NULL or rewrite it to the type's default, and Spanner validates every existing row in a schema change, which a NULL fails and leaves the column nullable | both | — |

</div>

### mysql, mariadb, postgres

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `locking/add-check` | adding a CHECK constraint validates every existing row and can hold locks, and a row that fails the predicate fails the migration | both | `MY144`, `PG305` |
| `locking/add-foreign-key` | adding a foreign key validates existing rows and can copy or lock the table and block writes, on both tables on PostgreSQL | both | `MY131`, `PG306` |
| `online/unproven` | the statement is outside the set measured to take no lock that conflicts with reads and writes: on PostgreSQL it is unmeasured, and on MySQL and MariaDB it does not ask the server to apply it without blocking writes, so the server was never given the chance to refuse | both | — |
| `rewrite/add-stored-generated-column` | adding a STORED generated column computes and stores a value for every row; on MySQL and MariaDB it copies the table and blocks writes | both | `MY140`, `PG309` |
| `rewrite/alter-column-type` | a column type change the engine cannot apply in place rewrites the table: PostgreSQL rewrites it and its indexes when it cannot prove the change safe, and InnoDB copies it and blocks writes | both | `MY130`, `PG301` |
| `rewrite/in-place-column-change` | a column change the dev database shows the engine applies without rewriting the table, info only: on PostgreSQL an ALTER COLUMN TYPE applied as a catalog edit with no rebuild or scan, and on MySQL and MariaDB a MODIFY, CHANGE or CONVERT InnoDB applies in place, with the algorithm | both | — |

</div>

### postgres

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `coverage/unassessed-extension` | an install the advisory data cannot call clean on the dev database's server: no data covers it, the data is stale or unreadable, the server is a vendor build or outside the data, or lint cannot read the statement | native only | — |
| `design/column-alignment` | the declared column order can waste tuple padding | both | `PG110` |
| `design/disable-autovacuum` | disabling autovacuum leaves dead rows for nothing to reclaim; the statement is cheap and the cost is paid later | both | `PG320` |
| `locking/add-exclusion-constraint` | adding an EXCLUDE constraint holds an ACCESS EXCLUSIVE lock while it builds the index and validates every row | both | `PG109` |
| `locking/add-primary-key` | adding a primary key takes an ACCESS EXCLUSIVE lock and can scan existing rows | both | `PG104` |
| `locking/add-primary-key-on-nullable` | a primary key over nullable columns sets them NOT NULL and scans every row on top of the index build | both | `PG304` |
| `locking/add-unique-constraint` | adding a unique constraint takes an ACCESS EXCLUSIVE lock and validates rows | both | `PG105` |
| `locking/create-index` | CREATE INDEX without CONCURRENTLY blocks writes for the whole build | both | `PG101` |
| `locking/create-trigger` | CREATE TRIGGER takes a SHARE ROW EXCLUSIVE lock and can block writes | both | `PG308` |
| `locking/drop-index` | DROP INDEX without CONCURRENTLY blocks writes while the index is removed | both | `PG102` |
| `locking/partitioned-index` | an index on a partitioned table locks the parent and every partition at once, and CONCURRENTLY is refused there | both | `PG108` |
| `locking/replace-primary-key` | replacing a primary key builds the new unique index under an ACCESS EXCLUSIVE lock | both | `PG312` |
| `naming/identifier-collision` | a name PostgreSQL cuts to a name already in use where it refuses the second declaration, so the migration fails when it runs | native only | — |
| `naming/identifier-too-long` | a name longer than the 63 bytes PostgreSQL keeps, which the server cuts with a notice and stores under its first bytes | native only | — |
| `online/lock-held-across-scan` | a statement validates a constraint behind a lock an earlier statement in the same transaction took, so the scan runs with readers and writers waiting | both | — |
| `rewrite/add-identity-column` | adding an identity column can rewrite existing rows | both | `PG310` |
| `rewrite/set-access-method` | changing a table's access method rewrites the table | both | `PG311` |
| `rewrite/set-persistence` | changing LOGGED or UNLOGGED rewrites the table under heavyweight locks | both | `PG307` |
| `rewrite/volatile-default` | a volatile DEFAULT on an added column rewrites or evaluates every existing row | both | `PG302` |
| `rollout/weaken-replica-identity` | REPLICA IDENTITY FULL or NOTHING changes what logical replication can carry for the table | both | `PG314` |
| `security/definer-without-search-path` | a SECURITY DEFINER routine that does not pin search_path resolves unqualified names through the caller's | both | — |
| `security/vulnerable-extension` | `CREATE EXTENSION` or `ALTER EXTENSION ... UPDATE` installs an extension, at the version it names or the dev database's default and with what `CASCADE` adds, that a reviewed advisory affects on the dev database's server | native only | — |
| `transaction/add-enum-value` | ALTER TYPE ... ADD VALUE cannot run inside a transaction before PostgreSQL 12, and the value stays unusable in the same transaction after it | both | — |
| `transaction/concurrent-index` | CONCURRENTLY cannot run inside the migration's transaction | both | `PG103` |
| `transaction/mixed-statements` | the migration mixes statements that cannot share one transaction | both | `TX101` |
| `transaction/per-chunk-index` | a TimescaleDB per-chunk index build cannot run inside the migration's transaction | both | — |

</div>

### postgres, cockroachdb, yugabytedb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `locking/set-not-null` | SET NOT NULL scans the table to validate existing rows | both | `PG303` |

</div>

### postgres, mysql, mariadb, clickhouse, sqlite, sqlserver, cockroachdb, yugabytedb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `transaction/explicit-transaction` | a statement that controls transaction state, which the migrator refuses in a wrapped file and in one that runs without a transaction; a golang-migrate file on PostgreSQL, up or down, is read as the migrator runs it | both | `TX201` |

</div>

### spanner

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `side-effect/drop-row-deletion-policy` | `ALTER TABLE ... DROP TTL` drops a table's row deletion policy, so rows the policy would delete stay | both | — |

</div>

### sqlite, mysql, mariadb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `rewrite/set-not-null` | declaring a column NOT NULL rebuilds the table: SQLite cannot enforce it on existing nullable data without a rebuild, and MySQL and MariaDB rebuild in place; whether an existing NULL fails the statement is the question data-dependent/set-not-null answers | both | `MY147`, `LT101` |

</div>

### ydb

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `capability/analyze` | `ANALYZE` in a migration, which YDB refuses unless `EnableColumnStatistics` is on, and on 25.1 refuses on a row table | both | — |
| `capability/unique-index-on-existing-table` | a unique index added to a table that exists, which a YDB line without `unique_index_on_existing_table` refuses | both | — |
| `capability/vector-index` | a vector index a YDB line does not build: any where `vector_indexes` is false, and one over bit vectors where `vector_bit_type` is false | both | — |
| `dependency/drop-indexed-column` | a `DROP COLUMN` of a column an index keys or covers, or the TTL reads, which YDB refuses until the index or the TTL is gone | both | — |
| `dependency/drop-table-read-by-view` | a `DROP TABLE` or `ALTER TABLE ... RENAME TO` of a table a view reads: YDB keeps the view, which reads its table by path, and every read of the view fails | both | — |
| `dependency/rename-table-with-changefeed` | a `RENAME TO` of a table that carries a changefeed, which YDB refuses until every changefeed is dropped | both | — |
| `destructive/discard-streaming-query-state` | replacing a streaming query or changing its body discards checkpoint state that rollback cannot restore | both | — |
| `destructive/drop-backup-collection` | `DROP BACKUP COLLECTION` deletes every backup the collection holds, and stops a YDB 25.1 server | both | — |
| `destructive/drop-default-resource-pool` | `DROP RESOURCE POOL default` on YDB, after which every query of the database fails with `Resource pool default not found` | both | — |
| `security/deprecated-secret-object` | an external data source that names its credential by a deprecated secret object (`..._SECRET_NAME`), whose value the database administrator reads in clear | both | — |
| `security/secret-in-clear` | a password or a token written in clear in an async replication or a transfer, which YDB keeps without reading it back while the migration file holds it | both | — |
| `security/secret-value-in-migration` | a secret's value written into the migration: a `CREATE SECRET` or `ALTER SECRET` whose value is not a `$PTAH_SECRET_...` reference, or a statement on the deprecated `OBJECT ... (TYPE SECRET)` | both | — |
| `side-effect/drop-replication-without-failover` | a `DROP ASYNC REPLICATION` without `CASCADE` of a replication not failed over, whose replica tables YDB keeps read-only for good | both | — |
| `side-effect/dropped-principal-keeps-permissions` | `DROP USER` or `DROP GROUP` on YDB leaves the principal's permissions behind, and a principal created later under the name holds them | both | — |
| `side-effect/ignored-topic-setting` | a topic or consumer setting YDB accepts and keeps nothing of, such as a storage limit or a codec it does not know | both | — |
| `side-effect/implicit-column-family` | an `ALTER TABLE` that names a column family the table does not have, which YDB creates with its own settings rather than refusing, and YQL cannot drop | both | — |
| `side-effect/orphaned-comment` | a dropped or renamed column or index whose comment stays on the YDB table as an attribute under its old name | both | — |
| `side-effect/replay-sequence-restart` | an `ALTER SEQUENCE` without a `RESTART` on a sequence an earlier statement restarted, which YDB replays, so the next insert takes a key a row holds | both | — |
| `side-effect/reset-min-partition-count` | turning auto partitioning by size or by load on resets the minimum partition count to 1 unless the same statement sets it; silent where the directory's own migrations left the minimum at 1 | both | — |
| `side-effect/reset-partition-size` | turning auto partitioning by size on resets the partition size to 2048 MB unless the same statement sets it | both | — |
| `side-effect/reset-topic-setting` | an `ALTER TOPIC` that resets a topic setting, or a consumer setting other than its availability period, which YDB 26.2 accepts and keeps as it was, and 25.1 refuses | both | — |
| `side-effect/revoke-grant-option` | `REVOKE GRANT OPTION FOR` on YDB takes the permission it names as well, since YDB keeps the grant option as the permission `ydb.access.grant` | both | — |
| `side-effect/vector-index-misses-writes` | rows written into a table holding a vector index on a YDB line where `vector_index_maintained_on_write` is false, which the index does not find | both | — |
| `side-effect/widen-serial-sequence` | an `ALTER SEQUENCE` on a 16-bit or 32-bit Serial's sequence, which raises its maximum to the Int64 maximum, so the column wraps to negative values without an error | both | — |
| `transaction/scheme-and-data-in-one-query` | a block or an action call that runs a scheme statement and a statement reading or writing a table in one query, which YDB refuses whole | both | — |
| `ydb/add-column-form` | an `ADD COLUMN` YDB refuses: NOT NULL without a default on every line, or a default where `add_column_with_default` is false | both | — |

</div>

### What the rules for every dialect do on YDB

A YDB run reads migrations as YQL. The `YD` family above is YDB's own, and every rule with no dialect restriction runs there too. This table says what each of those does on YDB: it applies, the statement it reads does not exist in YQL, a `YD` rule replaces it, its subject is read by the provider rather than Ptah, or it needs a dev database, and a run without `--dev-url` names it as unmet.

| Rule | On YDB | What it rests on |
| --- | --- | --- |
| `correctness/immutable-calls-volatile` | no such statement in YQL | YQL stores no routine |
| `coverage/routine-body` | no such statement in YQL | YQL stores no routine |
| `coverage/unparsed-statement` | read by the provider | the rule is about Ptah's SQL parser, and YQL is read by its provider |
| `data-dependent/add-not-null-column` | replaced | `ydb/add-column-form`, since YDB refuses a NOT NULL column without a default even on an empty table |
| `data-dependent/add-unique-index` | applies | `ADD INDEX ... UNIQUE` where the target adds one to an existing table; `capability/unique-index-on-existing-table` replaces it where it does not |
| `data-dependent/alter-column-type` | no such statement in YQL | YQL changes no column type; `ALTER COLUMN ... TYPE` is a syntax error |
| `data-dependent/rebuild-index-as-unique` | applies | `DROP INDEX` then `ADD INDEX ... UNIQUE`, where the target adds one; `capability/unique-index-on-existing-table` replaces it where it does not |
| `dependency/drop-column-in-use` | needs a dev database | what reads a column comes from a dev database replay |
| `destructive/drop-column` | applies | `ALTER TABLE ... DROP COLUMN` |
| `destructive/drop-object` | applies | Drops YDB users, groups, topics, transfers, replication replica tables with CASCADE, coordination nodes and streaming queries. Topic positions, streaming checkpoints and coordination resources are lost with their objects |
| `destructive/drop-table` | applies | `DROP TABLE` |
| `destructive/remove-enum-value` | no such statement in YQL | YDB has no enum type |
| `destructive/truncate-table` | applies | `TRUNCATE TABLE`, on lines with `truncate_table` |
| `file-form/empty-migration` | applies | the migration directory's files |
| `file-form/file-name` | applies | the migration directory's files |
| `file-form/missing-down` | applies | the migration directory's files |
| `file-form/refused-file` | applies | the migration directory's files |
| `integrity/drop-check` | no such statement in YQL | YDB has no `CHECK` constraint |
| `integrity/drop-foreign-key` | no such statement in YQL | YDB has no foreign key |
| `integrity/drop-not-null` | applies | `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` |
| `integrity/drop-primary-key` | no such statement in YQL | YDB cannot drop a primary key |
| `integrity/drop-untyped-constraint` | no such statement in YQL | YDB has no named constraint to drop |
| `naming/check-name` | no such statement in YQL | YDB has no `CHECK` constraint |
| `naming/column-name` | applies | the columns of `CREATE TABLE` and `ADD COLUMN` |
| `naming/foreign-key-name` | no such statement in YQL | YDB has no foreign key |
| `naming/index-name` | applies | `INDEX` in `CREATE TABLE`, `ADD INDEX` and `RENAME INDEX` |
| `naming/schema-name` | no such statement in YQL | YDB has no schema to create |
| `naming/table-name` | applies | `CREATE TABLE` and `RENAME TO`, judged on the unquoted path |
| `rollout/drop-column` | applies | `ALTER TABLE ... DROP COLUMN` |
| `rollout/drop-table` | applies | `DROP TABLE` |
| `rollout/rename` | applies | `ALTER TABLE ... RENAME TO` |
| `security/disable-row-level-security` | no such statement in YQL | YDB has no row-level security |
| `security/dynamic-sql-from-value` | no such statement in YQL | YQL stores no routine |
| `security/no-force-row-level-security` | no such statement in YQL | YDB has no row-level security |

## SQL lint rules

9 rules, reported by `ptah sql lint` over standalone SQL files, on every dialect. The compatibility surface has no verb that reaches them. A file of an executable provider's engine, such as YDB, is read from the statement facts of the provider's split and can report `coverage/unsupported-statement`, `design/no-primary-key`, `capability/missing` and `coverage/unanalyzed-statements` only.

<div class="ptah-wide-table">

| Rule | Meaning | Surface | Atlas check |
| --- | --- | --- | --- |
| `capability/missing` | the statement needs a capability the target server version does not have | native only | — |
| `correctness/index-on-undeclared-column` | an index names a column the schema does not declare | native only | — |
| `correctness/syntax-error` | the statement is malformed or opens with words no engine Ptah reads has, so no rule could inspect it | native only | — |
| `coverage/dynamic-sql` | a routine body builds SQL at run time, so static analysis of that routine stops there | native only | — |
| `coverage/unanalyzed-statements` | the file carried statement kinds no rule examined, so a clean result would not mean it was checked | native only | — |
| `coverage/unsupported-statement` | nothing read the statement, which a server may still accept: the parser holds no grammar for it, or steps over it as it does a SELECT | native only | — |
| `design/no-primary-key` | the created table declares no primary key | native only | — |
| `naming/identifier-collision` | a name the server cuts to a name already in use where it refuses the second declaration | native only | — |
| `naming/identifier-too-long` | a name longer than the identifier limit of a server that cuts long names, which stores the object under its first bytes | native only | — |

</div>

## Default severities

39 rules report at error severity by default:

- `capability/missing`
- `capability/unique-index-on-existing-table`
- `capability/vector-index`
- `correctness/index-on-undeclared-column`
- `correctness/syntax-error`
- `coverage/unsupported-statement`
- `dependency/drop-column-in-use`
- `dependency/drop-indexed-column`
- `dependency/drop-table-read-by-view`
- `dependency/rename-table-with-changefeed`
- `destructive/drop-backup-collection`
- `destructive/drop-column`
- `destructive/drop-default-resource-pool`
- `destructive/drop-object`
- `destructive/drop-system-versioning`
- `destructive/drop-table`
- `destructive/remove-enum-value`
- `destructive/truncate-table`
- `file-form/refused-file`
- `integrity/drop-check`
- `integrity/drop-foreign-key`
- `integrity/drop-not-null`
- `integrity/drop-primary-key`
- `integrity/drop-untyped-constraint`
- `naming/identifier-collision`
- `naming/identifier-collision`
- `online/lock-held-across-scan`
- `online/unproven`
- `security/disable-row-level-security`
- `security/no-force-row-level-security`
- `security/secret-in-clear`
- `security/secret-value-in-migration`
- `side-effect/ignored-topic-setting`
- `side-effect/replay-sequence-restart`
- `side-effect/reset-topic-setting`
- `side-effect/revoke-grant-option`
- `side-effect/widen-serial-sequence`
- `transaction/scheme-and-data-in-one-query`
- `ydb/add-column-form`

The other 90 default to warning or info. A committed `.ptah-lint.yaml` replaces either, per rule or per category. `ptah sql lint` reads the `rules:` severities the same file sets for `capability/missing`, `correctness/index-on-undeclared-column`, `correctness/syntax-error`, `coverage/dynamic-sql`, `coverage/unanalyzed-statements`, `coverage/unsupported-statement`, `design/no-primary-key`, `naming/identifier-collision` and `naming/identifier-too-long`, so the severities above are the defaults. `--disable` refuses a selector covering `correctness/syntax-error` or `coverage/unsupported-statement`: those report that the file could not be analyzed, and a run that analyzed nothing must not report clean.

## Atlas check codes

Ptah rules have no numeric code. The compatibility layer maps each rule that reports an Atlas check to that check's code, so `ptah-compat` can print Atlas's own vocabulary. A rule not listed here has no Atlas check and keeps its name.

| Ptah rule | Atlas check | Dialects |
| --- | --- | --- |
| `destructive/drop-table` | `DS102` | all |
| `destructive/drop-column` | `DS103` | all |
| `integrity/drop-foreign-key` | `CD101` | all |
| `integrity/drop-check` | `CD102` | all |
| `integrity/drop-primary-key` | `CD103` | all |
| `data-dependent/add-not-null-column` | `MF103` | all |
| `rollout/rename` | `BC101` | all |
| `data-dependent/add-unique-index` | `MF101` | all |
| `data-dependent/rebuild-index-as-unique` | `MF102` | all |
| `naming/schema-name` | `NM101` | all |
| `naming/table-name` | `NM102` | all |
| `naming/column-name` | `NM103` | all |
| `naming/index-name` | `NM104` | all |
| `naming/foreign-key-name` | `NM105` | all |
| `naming/check-name` | `NM106` | all |
| `security/dynamic-sql-from-value` | `SA101` | all |
| `transaction/explicit-transaction` | `TX201` | postgres, mysql, mariadb, clickhouse, sqlite, sqlserver, cockroachdb, yugabytedb |
| `rollout/drop-table` | `BC103` | all |
| `rollout/drop-column` | `BC104` | all |
| `side-effect/inline-reference` | `MY102` | mysql, mariadb |
| `rewrite/remove-enum-value` | `MY110` | mysql, mariadb |
| `rewrite/reorder-enum-values` | `MY111` | mysql, mariadb |
| `rewrite/insert-enum-value` | `MY112` | mysql, mariadb |
| `rewrite/widen-enum` | `MY113` | mysql, mariadb |
| `rewrite/remove-set-member` | `MY120` | mysql, mariadb |
| `rewrite/reorder-set-members` | `MY121` | mysql, mariadb |
| `rewrite/insert-set-member` | `MY122` | mysql, mariadb |
| `rewrite/widen-set` | `MY123` | mysql, mariadb |
| `rewrite/alter-column-type` | `MY130` | mysql, mariadb |
| `locking/add-foreign-key` | `MY131` | mysql, mariadb |
| `rewrite/add-primary-key` | `MY132` | mysql, mariadb |
| `rewrite/drop-primary-key` | `MY133` | mysql, mariadb |
| `locking/add-fulltext-index` | `MY134` | mysql, mariadb |
| `locking/add-spatial-index` | `MY135` | mysql, mariadb |
| `rewrite/convert-charset` | `MY136` | mysql, mariadb |
| `rewrite/replace-primary-key` | `MY137` | mysql, mariadb |
| `rewrite/change-engine` | `MY138` | mysql, mariadb |
| `rewrite/change-partitioning` | `MY139` | mysql, mariadb |
| `rewrite/add-stored-generated-column` | `MY140` | mysql, mariadb |
| `locking/add-auto-increment-column` | `MY141` | mysql, mariadb |
| `rewrite/add-column-before-existing` | `MY142` | mysql, mariadb |
| `rewrite/modify-generated-column` | `MY143` | mysql, mariadb |
| `locking/add-check` | `MY144` | mysql, mariadb |
| `locking/enforce-check` | `MY145` | mysql |
| `destructive/drop-system-versioning` | `MY146` | mariadb |
| `rewrite/set-not-null` | `MY147` | mysql, mariadb |
| `rewrite/set-not-null` | `LT101` | sqlite |
| `locking/create-index` | `PG101` | postgres |
| `transaction/concurrent-index` | `PG103` | postgres |
| `locking/add-primary-key` | `PG104` | postgres |
| `locking/add-unique-constraint` | `PG105` | postgres |
| `locking/drop-index` | `PG102` | postgres |
| `locking/partitioned-index` | `PG108` | postgres |
| `locking/add-exclusion-constraint` | `PG109` | postgres |
| `design/column-alignment` | `PG110` | postgres |
| `rewrite/alter-column-type` | `PG301` | postgres |
| `rewrite/volatile-default` | `PG302` | postgres |
| `locking/set-not-null` | `PG303` | postgres, cockroachdb, yugabytedb |
| `locking/add-primary-key-on-nullable` | `PG304` | postgres |
| `locking/add-check` | `PG305` | postgres |
| `locking/add-foreign-key` | `PG306` | postgres |
| `rewrite/set-persistence` | `PG307` | postgres |
| `locking/create-trigger` | `PG308` | postgres |
| `rewrite/add-stored-generated-column` | `PG309` | postgres |
| `rewrite/add-identity-column` | `PG310` | postgres |
| `rewrite/set-access-method` | `PG311` | postgres |
| `locking/replace-primary-key` | `PG312` | postgres |
| `rollout/weaken-replica-identity` | `PG314` | postgres |
| `design/disable-autovacuum` | `PG320` | postgres |
| `transaction/mixed-statements` | `TX101` | postgres |

## Atlas analyzer checks

Every check code in the reviewed snapshot of the [Atlas analyzer documentation](https://atlasgo.io/lint/analyzers) (`internal/atlascodes/reference.txt`, reviewed 2026-09-06), and what Ptah does about it: 73 covered, 2 partial, 0 not implemented, 2 waived, of 77. That page presents its table as highlights rather than an inventory -- it says Atlas "runs and reports dozens of additional checks" beyond them -- so a complete row here is completeness against the snapshot, not full behavioral parity with the analyzer. A code Atlas marks as an Atlas Pro feature is marked here too, and the ones Ptah implements are reported through both surfaces except `BC101` and `BC102`, whose Ptah rule the compatibility surface does not report.

<div class="ptah-wide-table">

| Atlas check | Meaning | Ptah rule and status |
| --- | --- | --- |
| `DS101` | schema was dropped | `destructive/drop-object`<br>covered |
| `DS102` | table was dropped | `destructive/drop-table`<br>covered |
| `DS103` | non-virtual column was dropped | `destructive/drop-column`<br>covered |
| `MF101` | adding a unique index to an existing column | `data-dependent/add-unique-index`<br>covered — structural: the message names the query that settles the duplicates; a unique index the dev database already holds over the columns silences it |
| `MF102` | modifying a non-unique index to unique | `data-dependent/rebuild-index-as-unique`<br>covered — an index dropped earlier in the file and rebuilt as unique under the same name, or under a new name over the columns the dev database records for it; the message adds that the failure leaves the table without the index it had |
| `MF103` | adding a non-nullable column to an existing table | `data-dependent/add-not-null-column`<br>covered |
| `MF104` | modifying a nullable column to non-nullable might fail | `locking/set-not-null`, `rewrite/set-not-null`, `data-dependent/set-not-null`<br>covered — the first on PostgreSQL, CockroachDB and YugabyteDB, the second on SQLite, MySQL and MariaDB, the third on MySQL, MariaDB, SQL Server, ClickHouse and Spanner; every engine measured, each with its own failure named |
| `BC101` | renaming a table | `rollout/rename`<br>covered |
| `BC102` | renaming a column | `rollout/rename`<br>covered — one rule reports both object kinds |
| `BC103` | dropping a table | `rollout/drop-table`<br>covered — the rollout break, not the row loss destructive/drop-table reports on the same statement, and separately suppressible: accepting the data loss is not accepting deployed clients failing. Native `ptah migrations lint` only; no apply gate |
| `BC104` | dropping a column | `rollout/drop-column`<br>covered — as BC103, beside destructive/drop-column, and reported on the same surface; a column the migration created or added earlier is exempt from both, since no deployed client read it |
| `MY101` | adding a non-nullable column without a DEFAULT to an existing table | `data-dependent/add-not-null-column`<br>covered — the Ptah rule applies to every dialect |
| `MY102` | an inline REFERENCES clause in ADD COLUMN has no effect | `side-effect/inline-reference`<br>covered |
| `MY110` | removing enum values from a column requires a table copy | `rewrite/remove-enum-value`<br>covered |
| `MY111` | reordering enum values requires a table copy | `rewrite/reorder-enum-values`<br>covered |
| `MY112` | inserting enum values other than at the end requires a table copy | `rewrite/insert-enum-value`<br>covered |
| `MY113` | exceeding 256 enum values changes storage size and requires a table copy | `rewrite/widen-enum`<br>covered |
| `MY120` | removing set values from a column requires a table copy | `rewrite/remove-set-member`<br>covered |
| `MY121` | reordering set values requires a table copy | `rewrite/reorder-set-members`<br>covered |
| `MY122` | inserting set values other than at the end requires a table copy | `rewrite/insert-set-member`<br>covered |
| `MY123` | exceeding a set-size boundary changes storage size and requires a table copy | `rewrite/widen-set`<br>covered |
| `MY130` (Pro) | changing a column type requires a table copy | `rewrite/alter-column-type`<br>covered — fires only for a change InnoDB refuses to apply in place, with the old and new type and the boundary, character set, collation or key that decides it; rewrite/in-place-column-change is the info finding for the change applied in place |
| `MY131` (Pro) | adding a foreign key blocks DML | `locking/add-foreign-key`<br>covered |
| `MY132` (Pro) | adding a primary key requires a table rebuild | `rewrite/add-primary-key`<br>covered |
| `MY133` (Pro) | dropping a primary key without adding one requires a table copy | `rewrite/drop-primary-key`, `integrity/drop-primary-key`<br>covered — MY133 names the copy and CD103 the lost uniqueness guarantee; the message names the MariaDB case where another NOT NULL UNIQUE key keeps the change in place |
| `MY134` (Pro) | adding a FULLTEXT index blocks DML | `locking/add-fulltext-index`<br>covered |
| `MY135` (Pro) | adding a SPATIAL index blocks DML | `locking/add-spatial-index`<br>covered |
| `MY136` (Pro) | changing the table character set requires a table rebuild | `rewrite/convert-charset`<br>covered — names the columns whose re-encoding forces the copy; a conversion that touches no column, or only utf8mb3 to utf8mb4 on short VARCHAR and CHAR columns no key covers, is not reported |
| `MY137` (Pro) | modifying the primary key rebuilds the table and its secondary indexes | `rewrite/replace-primary-key`<br>covered — one statement that drops and adds; measured in place with writes allowed, so it subsumes MY132 rather than repeating it, and leaves CD103 to report the identity loss |
| `MY138` (Pro) | changing the storage engine requires a table copy and blocks DML | `rewrite/change-engine`<br>covered — any ENGINE= clause, including one naming the engine the table already uses, which costs the same because it is the documented way to force a rebuild |
| `MY139` (Pro) | partitioning or removing partitioning requires a table copy and blocks DML | `rewrite/change-partitioning`<br>covered — measured, this statement form accepts no ALGORITHM or LOCK clause at all, so the finding says there is no online-DDL negotiation rather than naming a copy |
| `MY140` (Pro) | adding a STORED generated column requires a table copy and blocks DML | `rewrite/add-stored-generated-column`<br>covered |
| `MY141` (Pro) | adding an AUTO_INCREMENT column rebuilds the table and blocks DML | `locking/add-auto-increment-column`<br>covered — the rebuild is in place and writes still stop: LOCK=NONE is refused, which is why the finding reports cost and locking separately |
| `MY142` (Pro) | adding a column before existing columns prevents an instant operation on older versions | `rewrite/add-column-before-existing`<br>covered — reports only where the server the run plans against lacks the instant placement, read from `--server-version` or the dev database: MySQL before 8.0.29 and MariaDB before 10.4 |
| `MY143` (Pro) | modifying a generated column requires a table copy and blocks DML | `rewrite/modify-generated-column`<br>covered — the MODIFY and CHANGE side; adding one is MY140, and data-dependent/alter-column-type still reports the type change where the declared type moves |
| `MY144` (Pro) | adding a CHECK constraint scans all existing rows | `locking/add-check`<br>covered |
| `MY145` (Pro) | modifying or enforcing a CHECK constraint re-validates all existing rows | `locking/enforce-check`<br>covered — MySQL only: MariaDB has no ENFORCED or NOT ENFORCED syntax, so a mariadb finding would describe a statement that server refuses to parse |
| `MY146` (Pro) | dropping system versioning permanently deletes all history rows | `destructive/drop-system-versioning`<br>covered — MariaDB only: MySQL 8.4 has no system versioning at all; reported as an error because the history is deleted rather than made expensive |
| `MY147` (Pro) | changing column nullability requires a table rebuild | `rewrite/set-not-null`<br>covered — the cost; measured in place with writes allowed. Whether an existing NULL fails the statement is the separate question data-dependent/set-not-null answers from the baseline |
| `MY148` (Pro) | changing a column character set or collation requires a table copy and blocks DML | `rewrite/alter-column-type`<br>partial — MY130 names this consequence where it can prove the copy from the dev database; without that baseline, or on a spelling it cannot resolve to a before-and-after, the statement is not reported |
| `LT101` | modifying a nullable column to non-nullable without a DEFAULT | `rewrite/set-not-null`<br>covered |
| `PG101` (Pro) | index created without CONCURRENTLY | `locking/create-index`<br>covered — on a TimescaleDB hypertable, which refuses CONCURRENTLY, the message names the per-chunk build instead when the dev database shows the table is one; that build is not reported |
| `PG102` (Pro) | index dropped without CONCURRENTLY | `locking/drop-index`<br>covered |
| `PG103` (Pro) | concurrent operation without the atlas:txmode none header | `transaction/concurrent-index`<br>covered — the atlas:txmode none header and Ptah's own directive both silence it, and so does a golang-migrate file that holds nothing else |
| `PG104` (Pro) | PRIMARY KEY creation acquires an ACCESS EXCLUSIVE lock | `locking/add-primary-key`<br>covered |
| `PG105` (Pro) | UNIQUE constraint creation acquires an ACCESS EXCLUSIVE lock | `locking/add-unique-constraint`<br>covered |
| `PG110` | creating a table with non-optimal data alignment | `design/column-alignment`<br>covered |
| `PG301` (Pro) | a column type change requires a table and index rewrite | `rewrite/alter-column-type`<br>covered — fires for a change PostgreSQL rewrites for, naming the abort a value can cause, and for a collation change on an indexed column, naming the indexes it rebuilds; rewrite/in-place-column-change is the info finding for a catalog edit |
| `PG302` (Pro) | a volatile DEFAULT on an added column rewrites the table | `rewrite/volatile-default`<br>covered |
| `PG303` (Pro) | SET NOT NULL scans existing rows | `locking/set-not-null`<br>covered |
| `PG304` (Pro) | PRIMARY KEY on nullable columns requires a full scan | `locking/add-primary-key-on-nullable`, `locking/add-primary-key`<br>covered — PG304 names the columns the key sets NOT NULL and the extra scan that costs, for a column list and for USING INDEX alike; PG104 names the lock every ADD PRIMARY KEY takes |
| `PG305` (Pro) | a CHECK constraint requires a full table scan | `locking/add-check`<br>covered |
| `PG306` (Pro) | a FOREIGN KEY requires a full scan and blocks writes | `locking/add-foreign-key`<br>covered |
| `PG307` (Pro) | a logging-mode change rewrites the table | `rewrite/set-persistence`<br>covered |
| `PG308` (Pro) | trigger creation acquires a SHARE ROW EXCLUSIVE lock | `locking/create-trigger`<br>covered |
| `PG309` (Pro) | a STORED generated column rewrites the table | `rewrite/add-stored-generated-column`<br>covered |
| `PG310` (Pro) | an identity column rewrites the table | `rewrite/add-identity-column`<br>covered |
| `PG311` (Pro) | an access-method change rewrites the table | `rewrite/set-access-method`<br>covered |
| `PG108` (Pro) | an index on a partitioned table blocks writes on all its partitions | `locking/partitioned-index`<br>partial — reported where the migration declares the parent PARTITION BY, or the dev database shows an earlier parent partitioned; without a dev database PG101 reports it instead |
| `PG109` (Pro) | an EXCLUDE constraint takes an ACCESS EXCLUSIVE lock and scans the table | `locking/add-exclusion-constraint`<br>covered |
| `PG312` (Pro) | redefining a primary key builds its unique index under an ACCESS EXCLUSIVE lock | `locking/replace-primary-key`<br>covered — distinct from security/definer-without-search-path, which is about a SECURITY DEFINER routine; the USING INDEX form builds nothing under the lock and is not reported |
| `PG314` (Pro) | changing REPLICA IDENTITY to FULL or NOTHING risks the logical replication setup | `rollout/weaken-replica-identity`<br>covered — FULL and NOTHING carry different consequences and are reported with different messages; DEFAULT and USING INDEX keep a usable row identity and are not reported |
| `PG320` (Pro) | disabling autovacuum lets dead rows accumulate | `design/disable-autovacuum`<br>covered — the one rule here whose hazard is not a lock or a rewrite: the statement takes only a SHARE UPDATE EXCLUSIVE lock and the cost is paid later |
| `CD101` (Pro) | a foreign-key constraint was dropped | `integrity/drop-foreign-key`<br>covered |
| `CD102` (Pro) | a check constraint was dropped | `integrity/drop-check`<br>covered |
| `CD103` (Pro) | a primary-key constraint was dropped | `integrity/drop-primary-key`<br>covered |
| `TX101` (Pro) | statements cannot run in a single transaction | `transaction/mixed-statements`<br>covered |
| `TX201` (Pro) | a nested transaction was detected | `transaction/explicit-transaction`<br>covered — reported on every engine the migrator applies, because the migrator refuses the file; a golang-migrate file on PostgreSQL, up or down, is read as the migrator runs its own transactions |
| `NM101` | a schema name violates the naming convention | `naming/schema-name`<br>covered — needs a configured naming convention |
| `NM102` | a table name violates the naming convention | `naming/table-name`<br>covered — needs a configured naming convention |
| `NM103` | a column name violates the naming convention | `naming/column-name`<br>covered — needs a configured naming convention |
| `NM104` | an index name violates the naming convention | `naming/index-name`<br>covered — needs a configured naming convention; a unique or primary key constraint counts as an index, as it does for Atlas |
| `NM105` | a foreign-key constraint name violates the naming convention | `naming/foreign-key-name`<br>covered — needs a configured naming convention |
| `NM106` | a check constraint name violates the naming convention | `naming/check-name`<br>covered — needs a configured naming convention |
| `SA101` | a possible SQL injection vulnerability was detected | `security/dynamic-sql-from-value`<br>covered — reports a routine body that builds its statement from an unquoted value; a literal text, quote_ident/quote_literal, format's %I and %L, QUOTENAME, and parameters are the safe forms it leaves alone |
| `OW101` (Pro) | a user is not authorized to modify a resource | —<br>waived — binds to a schema-ownership annotation set and an account model Ptah does not have |
| `OW102` (Pro) | a user is explicitly denied access to a resource | —<br>waived — same reason as OW101 |

</div>

<!-- END GENERATED LINT RULES -->
### Atlas codes in a config

`ptah-compat` reads Atlas's vocabulary in its own inputs: the Atlas check codes
and analyzer names an `atlas.hcl` `lint` block names, and an `atlas:nolint`
comment. It maps each to the Ptah rules the [Atlas check codes](#atlas-check-codes)
table lists. Native `ptah` and `ptah-compat` reading a `.ptah-lint.yaml` read
names only: a code in `disabled-rules`, in `--disable`, as a `rules:` key or in
a `ptah:nolint` comment is refused, and so is an `atlas:nolint` comment in a
migration file that native `ptah` reads.

## Changing what a rule does

This section is about migration lint. Severity and per-path exclusions are
configured per rule; see [Configuration](../configuration/) for
`.ptah-lint.yaml` and the `lint` block of an Atlas-compatible project file. A
single statement is silenced with a `ptah:nolint` comment, and the
compatibility surface accepts `atlas:nolint` with the analyzer names and codes
it prints.

A selector in each of those places is a Ptah rule name or a category:

```yaml
# .ptah-lint.yaml
disabled-rules:
  - rewrite                 # every rule named under the category
  - locking/set-not-null    # one rule, by its name
rules:
  destructive/drop-column:
    severity: warning
```

```sql
-- ptah:nolint locking/create-index
CREATE INDEX idx_users_email ON users (email);
```

When several `rules:` keys govern one rule, the rule's name wins, then its
category.

A `ptah:nolint` comment is a list of selectors followed by a reason. The
selectors come first. The reason starts at the first word that is not a
selector, or after a `--` separator, and nothing in it selects a rule:

```sql
-- ptah:nolint rollout/drop-table destructive on purpose, see https://example.com/ticket/ABC-1
-- ptah:nolint destructive/drop-table, locking/ -- reviewed with the DBA
```

A selector is a rule name, or a category written with a slash (`locking/`). A bare category (`locking`) is a selector only as the first word,
because a lower-case word later in the line is more likely prose. In the first
example above, `destructive` is part of the reason and hides nothing. A comment
with a reason and no selector silences no rule, and one with neither silences
every rule.

The comment is refused, with the file and the line, when a word is spelled as
a code (`DS102`, or codes joined by slashes), or is clearly meant as a name and
is wrong: its first segment is a declared category
(`locking/typo`), or it is one lower-case word ending in a slash (`lockin/`).
Any other word holding a slash, such as a URL, `and/or` or `w/o`, is part of the
reason.

`ptah sql lint` takes less. It reads only the severities the `rules:` section
of the same file sets for its own rules, keyed by name or category, and
honors no `nolint` comment. Its one control is `--disable`, repeatable, taking
a Ptah rule name or a category. A selector that names no rule is refused, as in `ptah migrations lint --disable`.
