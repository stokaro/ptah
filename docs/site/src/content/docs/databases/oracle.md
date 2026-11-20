---
title: Oracle
description: Oracle in Ptah - what renders, plans and reads back on the 23 and 21 release lines, bare identifiers, type mappings, object types, PL/SQL routines, and versioned migrations.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which Oracle objects and operations does Ptah support?"
goal: "Look up the Oracle objects and operations Ptah supports."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
sourceMode: live-database-only
overlaps: []
disposition: keep
---

Oracle renders, plans, connects to a live database and reads its catalog,
against two measured release lines, 23 and 21. A schema Ptah applies reads back
as itself on both:

```bash
ptah schema inspect --db-url oracle://user:pass@host:1521/FREEPDB1 > live.hcl
ptah schema diff --from oracle://user:pass@host:1521/FREEPDB1 --to live.hcl
```

```text
Schemas are synced, no changes to be made.
```

A **declared file** converges too. Names fold across case, so a `CHECK`
declared on a column is named `orders_total_check`, stored as
`ORDERS_TOTAL_CHECK` and matched on the next run. Oracle names an unnamed table
`CHECK` or `UNIQUE` itself, `SYS_C<n>`, so the comparison matches it to the
unnamed declaration by its condition, ignoring case and spacing, or by its
columns in order. A changed condition is a drop and an add. `schema inspect`
writes such a constraint with no name.

Oracle holds a constraint name once per schema, so two `CHECK` constraints of
one name are refused before anything runs (`ORA-02264`). `ck` and `"CK"` are
one name.

## Identifiers and type mappings

Identifiers are written **bare**, which is Oracle-only among the engines here
and is forced by the engine rather than chosen. Oracle folds an unquoted name to
upper case and preserves a quoted one, so a name has two spellings, and a
declaration has to agree with every expression that references it. A `CHECK` or
a generated expression is author text Ptah does not rewrite, so the declaration
is what moves:

```sql
CREATE TABLE q ("view_count" NUMBER(10) CHECK (view_count >= 0))   -- refused
CREATE TABLE b (view_count NUMBER(10) CHECK (view_count >= 0))     -- accepted
```

A name Oracle refuses bare — a reserved word such as `size`, `comment` or
`user`, or a name carrying a character outside a plain identifier — is quoted on
both sides instead, so the two still agree.

A plain name is written bare. It starts with a letter, and after that it may
hold digits, the underscore, `$` and `#`, as Oracle allows, so `a$b` is
written bare and stored as `A$B`. A name that starts with any other character,
such as `_a`, is written quoted, since Oracle refuses it bare with ORA-00911.

The case of a name declared bare carries no meaning. A declared `orders` means
the table Oracle stores as `ORDERS`, whether it comes from a Go annotation or
a SQL file. A reserved word is quoted, as above, and compared the same way. In
a SQL schema file a quoted name keeps the meaning Oracle gives it:
`CREATE TABLE "orders"` declares the table stored as `orders`. Go annotations
have no quoted form.

So a quoted name in lower or mixed case is not a spelling of the bare one. A
file that declares `CREATE TABLE "orders"` against an account holding `ORDERS`
plans a new table `"orders"` and drops `ORDERS`; write `orders` or `ORDERS`
bare to mean the existing table. The same file has to name the object the same
way everywhere: a table declared `"customers"` and referenced bare in a
foreign key or an index is refused, and the error names the quoted
declaration. A CHECK, a DEFAULT or a virtual column names a quoted column
quoted, as Oracle requires, such as `CHECK ("Status" IN ('a', 'b'))`; a bare
`status` there is refused before the server sees it.

A name read from the catalog is the stored spelling, and two spellings that
differ only in case are two objects. A table created as `"orders"` is stored
as `orders`, which is not what any bare name folds to, so Ptah keeps it apart
from `ORDERS` and writes it quoted wherever a statement names it. The rule
covers tables, columns, indexes, constraints, views, materialized views and
sequences. Triggers follow it too, but the Oracle read does not report them,
so a plan never names one. A catalog name that equals its upper-case fold is
written bare:

```sql
DROP TABLE APP."orders" PURGE;
DROP TABLE APP.ORDERS PURGE;
```

A database used as the desired side keeps the same spelling. `schema diff
--from` and `--to` on one account plan nothing, and `schema inspect --format
sql` and `db read` write `CREATE TABLE APP."orders"`, so the file creates the
same objects when it is replayed.

A declaration that would replace such an object is refused. If the declaration
names `orders`, the catalog has no `ORDERS`, and it holds the case-sensitive
`"orders"`, the plan would drop `"orders"` and create an empty `ORDERS`. Ptah
stops before planning and names both spellings instead. Only a name declared
bare and not in upper case is refused this way: `ORDERS` and `"orders"` each
name one object exactly. Rename the declaration or the database object so that
the two names agree.

An `--exclude` or `--include` selector matches the stored spelling exactly,
without quotes: `orders` selects the table created as `"orders"`, and `ORDERS`
selects the one created unquoted. It matches the same way on both sides of a
plan, including a desired side read from a database or declared in a SQL
file, where a bare `orders` is the table stored as `ORDERS` and is selected by
`ORDERS`. A selector written with quotes matches nothing. A report names such
a table the same way a statement does, `APP."orders"`. A selector that names
nothing as written, where its upper-case spelling names a table, is reported
with that spelling: `"orders" (the stored name is "ORDERS")`.

Two type mappings are worth knowing because Oracle has no direct equivalent:
`BOOLEAN` becomes `NUMBER(1)`, with `true` and `false` written as `1` and `0`,
and an enum becomes `VARCHAR2(255)` with a `CHECK` listing its values.

A column's size is compared by what it holds. `INTEGER`, `SMALLINT` and `DECIMAL` render
as `NUMBER(10)`, `NUMBER(5)` and `NUMBER` here, and the server builds `NUMBER(*,0)` from
each, as it builds `FLOAT(126)` from `DOUBLE PRECISION` and `FLOAT(63)` from `REAL`.
`BOOLEAN` and `BOOL` render as `NUMBER(1)`, and Oracle 23 builds a `BOOLEAN` from each.
Either column matches the word, so declaring it over a column built by hand plans nothing.

`NUMBER` holds any number, `NUMBER(*,0)` holds no fraction, `FLOAT` is `FLOAT(126)`, `CHAR` and
`NCHAR` hold one character, and `TIMESTAMP` holds six digits. `INTERVAL DAY TO SECOND`
is `INTERVAL DAY(2) TO SECOND(6)` and `INTERVAL YEAR TO MONTH` is `INTERVAL YEAR(2) TO
MONTH`. A lower size on a column with a row is refused by the server with `ORA-30082`
for a date or an interval and `ORA-01440` for a number, so the plan rated destructive
stops there and every value stays. A change of type needs an empty column too
(`ORA-01439`), so `BINARY_DOUBLE` declared `BINARY_FLOAT` is rated destructive, and it
stops the same way with a row in place. `BLOB` declared `RAW(50)` answers `ORA-22859`. `RAW`
is compared by its bytes, and a lower length runs if the row fits. See
[how a column change is rated](../../direct/plan-and-approve/#how-a-column-change-is-rated).

The column read uses `CHAR_USED` to keep `VARCHAR2(100)` and `VARCHAR2(100 CHAR)`
distinct. A change between byte and character length semantics is reported even
when the numeric length matches.

A SQL schema file read with `--dialect oracle` keeps `SYSDATE`,
`SYSTIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `USER`,
`UID`, `SESSIONTIMEZONE`, `DBTIMEZONE`, `ORA_INVOKING_USER` and
`ORA_INVOKING_USERID` as a default expression, written bare. A quoted
`'SYSDATE'` stays a string. Oracle calls each without parentheses and answers
`ORA-03077`, `ORA-30088` or `ORA-04044` to an empty argument list, and
`ORA-00920` or `ORA-00907` to one inside a parenthesized default. A default
such as `SYSDATE()`, `SYSDATE ()` or `(SYSDATE() + 1)` is refused before
anything is planned, and the refusal writes the default with the bare
spelling, `(SYSDATE + 1)`. A precision, as in `SYSTIMESTAMP(3)`, is
kept, and so is `SESSIONTIMEZONE()`, which Oracle calls.

A default that reads a sequence, `seq.NEXTVAL` or `seq.CURRVAL`, is kept as an
expression too, with or without a schema and with the sequence name bare or
quoted, as in `app.seq.NEXTVAL` or `"Seq".NEXTVAL`; `'seq.NEXTVAL'` stays a
string. Oracle stores an unqualified reference qualified and quoted, as
`"APP"."SEQ"."NEXTVAL"`, and a qualified one as written. The comparison reads
each by the names Oracle stores, leaving out a schema that is the table's own,
so a second apply plans nothing, and `schema inspect` writes the stored form
back as an expression.

An auto-increment column is an identity column. A column declared `GENERATED
ALWAYS AS IDENTITY` is written `ALWAYS`, and Oracle refuses an insert that
names it with `ORA-32795`. Every other one, `GENERATED BY DEFAULT AS IDENTITY`,
a serial type or an auto-increment flag, is written `BY DEFAULT`. The declared
options follow in parentheses: an option list as written, such as
`(START WITH 50 INCREMENT BY 5 CACHE 10)`, or a declared start and step. Oracle
builds every identity column `NOT NULL`, so one declared without `NOT NULL`
compares equal to the column Oracle built.

A plan compares the identity of a column that exists with the one the
declaration builds, and changes it in place:

- A changed generation is
  `ALTER TABLE t MODIFY (id NUMBER(10) GENERATED ALWAYS AS IDENTITY)`, or
  `BY DEFAULT`, with the declared options except the start, as in
  `(INCREMENT BY 5)`. The counter goes on from where it was.
- A declared `START WITH n` is left out of that `MODIFY`, because Oracle
  restarts the counter there: on a column holding 1 to 3, `START WITH 2` makes
  the next insert fail with `ORA-00001`. Ptah warns about each such column on
  standard error, and a generated migration file carries the warning as a
  comment. To set the start, declare `START WITH LIMIT VALUE`, which starts
  after the highest value held, or run the `MODIFY` with `START WITH n` by hand
  once the value is free.
- A column declared without an identity, over one that has it, takes
  `MODIFY (id NUMBER(10) DROP IDENTITY)`. Oracle leaves the column nullable,
  unless it is a key column, so a declared `NOT NULL` or default follows in a
  second `MODIFY`.
- An identity declared on a column that exists without one is refused before
  anything runs. Oracle has no statement for it and answers `ORA-30673`. Add
  the column as a new column, or create the table again. The refusal names the
  first such column and stops there.

The comparison leaves the identity options out: a changed start, step or
cache on a column that keeps its generation plans nothing.

A column Oracle reads as `BY DEFAULT ON NULL` compares equal to one declared
`BY DEFAULT`.

Oracle keeps no `START WITH` for a sequence. The read reports `LAST_NUMBER`,
the next value the sequence issues, rounded up to the end of its cached block,
and `schema inspect` writes it as the start. A declared start the counter has
reached compares equal, so a sequence that has issued values plans nothing. A
start changed in the declaration compares the same way: on a sequence whose
next value is 21, a start raised from 1 to 10 plans nothing and reports no
warning, since Oracle keeps no start to compare it with. The counter keeps its
place, and the new start takes effect only where the sequence is created. A
declared start it has not reached, above `LAST_NUMBER` for an ascending
sequence, is a change, and the plan refuses it rather than move the counter.

## What the 23 and 21 lines do differently

The `IF [NOT] EXISTS` guards are a 23-line feature. On 21 they are refused, and
the capability preset for that line reflects it, so a plan for a 21 server omits
them.

**Domains are a 23-line feature too**, and the only object where the two lines
differ rather than the spelling of a guard. Oracle 21 answers `ORA-00901` to
`CREATE DOMAIN` and has no `ALL_DOMAINS` view. So a declared domain is planned,
rendered, read back and compared on 23, and on 21 it is refused before any SQL,
since a column typed by it would name a type the server cannot create.

A Go annotation declares an Oracle domain or object type with
`dialects="oracle"`: a `domain` or `composite` declaration without a scope is
a PostgreSQL type, and an Oracle plan refuses it. An SQL file or an HCL
document read for an Oracle target keeps its `CREATE DOMAIN` statements and its
`domain` and `composite` blocks as Oracle's, and so does an HCL block whose
`dialect` attribute names `oracle`. The SQL reader takes the statements Ptah
writes, `CREATE DOMAIN IF NOT EXISTS` and `CREATE OR REPLACE TYPE ... AS
OBJECT`, so a rendered schema reads back. `schema inspect` writes the blocks of
an Oracle database without a `dialect` attribute, since the document is
written for Oracle.

A plan creates the domains, then the object types, each by name, with a type
another one names first, before the tables, and drops them after the tables.
A declared comment is written as a SQL comment line before the creation:
Oracle stores no comment on a domain or an object type.

A domain is compared by its base type and its `NOT NULL`. The base type is
compared in the spelling the catalog keeps, so `INTEGER` and `NUMBER(*,0)`,
`TIMESTAMP` and `TIMESTAMP(6)`, `FLOAT` and `FLOAT(126)`, or `VARCHAR(10)` and
`VARCHAR2(10)` are one type each. Its `CHECK` and default are written when the
domain is created and never compared, since the catalog stores its own
spelling of both. A plan that changes a domain's base
type or `NOT NULL` is refused, naming the domain. Oracle has no statement for
either change: `ALTER DOMAIN` changes a domain's `DISPLAY`, `ORDER` and
annotations only, `DROP DOMAIN` answers `ORA-11502` while a column uses the
domain, and `DROP DOMAIN ... FORCE` takes the domain off its columns. Declare a
domain under a new name and type the columns by it, or drop and create the
domain by hand.

Domain reads preserve numeric precision and scale, including `NUMBER(*,0)`.
National character types use their character count, so `NVARCHAR2(10)` remains
ten characters when the catalog reports twenty bytes. `ALL_DOMAIN_COLS` has no
`CHAR_USED`, so a `VARCHAR2` or `CHAR` length whose bytes exceed its characters
reads as a length in characters, `VARCHAR2(10 CHAR)`; where the two are equal
the unit cannot be told and the length reads bare. The view reports a
`BOOLEAN` domain as `UNDEFINED`, and Ptah reads it as `BOOLEAN`; a domain over
another type the view reports as `UNDEFINED` is recorded as one the read
cannot describe.

A domain with a `DISPLAY` or `ORDER` expression or with annotations is
recorded as found and not described, since the domain model has no place for
them: described without them, it would read back and replay as a domain that
has lost them. A declaration that names such a domain plans nothing for it,
and the comparison reports it as undecided.

Two of Oracle's catalog behaviors decide whether a domain converges. A domain declared `NOT NULL` grows a `CHECK` of its
own, named by the server and numbered per database, and Ptah reads the
nullability off the column instead so the plan does not carry that constraint
back and forth. And `DROP TABLE` is rendered with `PURGE`: a dropped table
keeps its dependencies in the recycle bin, and a plan that drops a table and
then the domain its column was typed by answers
`ORA-11538 ... has dependent objects in the recycle bin` halfway through. The
alternative, `DROP DOMAIN ... FORCE`, is worse — with a live dependent it
succeeds and silently untypes the column.

A column a domain types is read as the domain. The column catalog reports
such a column by its base type, `NOT NULL` where the domain is, with the
domain's default, and with a copy of each domain constraint on the table. Ptah
reads `DOMAIN_NAME` from `ALL_TAB_COLS` and `ALL_CONSTRAINTS` and leaves those
facts to the domain, so a table whose columns a domain types plans nothing once
it is applied, and `schema inspect` writes the column with the domain as its
type. A `NOT NULL` the column states itself stays: Oracle keeps it as a
constraint of the column. A default the column states itself cannot be told
apart from the domain's, since Oracle refuses a different one (`ORA-11501`),
so it reads as the domain's. A column typed by a domain of another schema, or
by one the read cannot describe, keeps its base type.

`ptah db drop-all` drops the account's domains after its tables and object
types. A domain made by `CREATE FLEXIBLE DOMAIN` names the domains it chooses
from, and `DROP DOMAIN` answers `ORA-11502` for one of those while it exists,
so each domain is dropped after the domains that reference it. The cleanup
writes `DROP DOMAIN` without `FORCE`, which would take the domain off any
column still using it.

The cleanup plan `ptah db drop-all` reports is the cleanup's own list: every
table, view, materialized view, sequence, synonym, trigger, type and domain
the account owns, Ptah's revision tables included, each with the statement the
cleanup runs, in the order it runs them. A materialized view keeps its rows
in a table of its own name, and that table goes with the view rather than as
a drop of its own.

## Column collations

A column declares its collation after its type, in every source: `COLLATE
BINARY_CI` in SQL, `collate: BINARY_CI` in YAML, `collate = "BINARY_CI"` in HCL,
and `platform.oracle.collate="BINARY_CI"` in a Go annotation.

```sql
CREATE TABLE customers (
  id NUMBER PRIMARY KEY,
  email VARCHAR2(200) COLLATE BINARY_CI
);
```

Oracle takes a column collation on `VARCHAR2`, `NVARCHAR2`, `CHAR` and `NCHAR`
columns only. A collation on `CLOB` answers `ORA-43912`, and one on a number,
`RAW`, date or `JSON` column answers `ORA-43911`; Ptah refuses both from the
declaration, before it writes a statement.

### The database has to run EXTENDED

Oracle accepts a column `COLLATE` only where the parameter `MAX_STRING_SIZE` is
`EXTENDED`. A database that runs `STANDARD` answers every `COLLATE` with
`ORA-43929`. Oracle Autonomous Database starts with `EXTENDED`; the
`gvenzl/oracle-free` image and a self-managed database start with `STANDARD`.

Ptah reads the setting from `DATABASE_PROPERTIES` when it connects. The
Oracle images grant that view to `PUBLIC`, and a database can revoke the grant.
When the account cannot read the view, Ptah still connects and every command
that declares no collation runs as before; the setting is then unknown, and a
declared collation reaches the server, which answers `ORA-43929` on a `STANDARD`
database after the statements before it have applied. On a `STANDARD` database
the account can read, a declared collation is refused by name before the first
statement runs:

```text
error: generate schema apply operations: render column c: unsupported feature: oracle: column "c" declares collation BINARY_CI, and this database runs with MAX_STRING_SIZE=STANDARD, which refuses a column collation (ORA-43929); convert the database to MAX_STRING_SIZE=EXTENDED, or remove the collation
```

Oracle commits each statement of a plan, so the refusal has to come first: the
server's own error would arrive after the statements before it had applied.
`ptah schema render` has no database to ask and writes the collation as
declared. For the same reason an offline render cannot check that a collation
name exists.

To convert a pluggable database that holds no data of value, run this as
`SYSDBA`. The change is one way, and Oracle documents no conversion back:

```sql
ALTER PLUGGABLE DATABASE FREEPDB1 CLOSE IMMEDIATE;
ALTER PLUGGABLE DATABASE FREEPDB1 OPEN UPGRADE;
ALTER SESSION SET CONTAINER=FREEPDB1;
ALTER SYSTEM SET MAX_STRING_SIZE=EXTENDED;
@?/rdbms/admin/utl32k.sql
ALTER SESSION SET CONTAINER=CDB$ROOT;
ALTER PLUGGABLE DATABASE FREEPDB1 CLOSE IMMEDIATE;
ALTER PLUGGABLE DATABASE FREEPDB1 OPEN;
```

### What a read reports

The read takes each column's `COLLATION` from `ALL_TAB_COLS` and the table's
`DEFAULT_COLLATION` from `ALL_TABLES`. A character column that names no
collation reads `USING_NLS_COMP`, and Ptah reports it as holding none. Names are
upper case, as the catalog stores them. A column in a table with a
`DEFAULT COLLATION` reads that collation as the one it compares with, and is not
reported as declaring it, because the catalog cannot say whether the column
named it. Ptah does not write `DEFAULT COLLATION`; it reads the table's value
only to tell the two cases apart. A declaration that restates the table's
default holds, and the plan is empty. Declaring `USING_NLS_COMP` on such a
column is a change: the column then names the collation itself.

An index over a column with a collation is a function-based index in the
catalog, and its key is listed under a hidden column, `SYS_NC00004$`, with the
expression `NLSSORT("C",'nls_sort=''BINARY_CI''')`. Ptah reads the key as the
column it names, so a declared index over the column matches and plans nothing.

### A collation name has to exist

A collation is a sort that `V$NLS_VALID_VALUES` lists for `PARAMETER='SORT'`
(`BINARY`, `FRENCH`, `GERMAN` and the rest), with `_CI` or `_AI` after it, or
`USING_NLS_COMP`. `BINARY_CI` and `BINARY_AI` follow from `BINARY`. Oracle
answers any other name with `ORA-12746`, including `USING_NLS_COMP_CI` and the
`_CS` forms. A YAML or HCL schema shared with MySQL that names `utf8mb4_bin`
therefore fails on Oracle, and without a check it fails after the tables
created before the column have committed.

A connected plan reads the list when it connects and refuses an unknown name
before the first statement, naming the column and the table. If the account
cannot read `V$NLS_VALID_VALUES`, the check is left to the server and Ptah
writes a warning that says so. `ptah schema render` has no database to ask and
cannot check the name.

### What a plan does

A declared collation is compared with the one the column holds, ignoring case.
A column that declares none is not compared, since it holds `USING_NLS_COMP` or
its table's default and a change would restate the column on every run. To
return a column to `USING_NLS_COMP`, declare `USING_NLS_COMP`.

A change is one statement, and the collation is the only thing it restates:

```sql
ALTER TABLE customers MODIFY (email VARCHAR2(200) COLLATE BINARY_AI);
```

The change is refused, naming the object, while the column belongs to an index,
a primary or unique key, a foreign key, a `CHECK`, or the expression of a
generated column. Oracle answers these with `ORA-43923` or `ORA-43922`, and a
plan that stopped there would leave the statements before it applied. A key
written on the column itself (`pk VARCHAR2(10) PRIMARY KEY`, `u VARCHAR2(10)
UNIQUE`) counts, although Oracle names it and Ptah reads it as a flag of the
column. Drop the object, apply the collation change, and declare the object
again in a later migration. A function-based index holds the column when an
expression of its key names it, such as `UPPER(email)`, quoted or not. The read
lists the key under a hidden column, `SYS_NC00006$`, and Ptah takes the
expression from `ALL_IND_EXPRESSIONS`.

A plan adds columns before it changes them, so a generated column or an inline
`CHECK` that the same plan adds over the column holds it as well, and the plan
is refused. Apply the collation change first and add the column in a later
migration. An index or a table-level `CHECK` the plan adds comes after the
change and holds nothing yet.

A key part with `DESC` over a collated column answers `ORA-54034` when an index
or key created before it holds the column in an ascending part: `(c)`, `(c, d)`,
`(d, c)`, a `UNIQUE` or a `PRIMARY KEY` on the column. It is accepted as the
first index over the column, and after indexes that hold the column only as
`DESC`, in any shape (`(c DESC)`, `(d, c DESC)`, `(c DESC, d)`). A function-based
index such as `UPPER(c)` holds nothing, and a `UNIQUE` constraint added after the
`DESC` index is accepted.

A plan creates the new indexes of a table in name order, byte order of the name
as declared, after the keys of a `CREATE TABLE` or an `ADD COLUMN` and before a
constraint it adds with `ALTER TABLE`. Ptah follows that order and refuses a new
index with a `DESC` part over a collated column when an ascending key holds the
column by then, naming the index that does. Indexes the database already holds
come first, and an index the database already holds is not created again. To get
the shape, name the `DESC` index so it sorts before the ascending one, drop
`DESC` from the part, or apply the ascending index in a later migration.

An index key the catalog lists as `NLSSORT("C",'nls_sort=''BINARY_CI''')` is the
column's own key only where `C` holds `BINARY_CI`. The same expression over a
column that holds another collation is an index you wrote, and Ptah reads it as
a function-based index.

## Object types

**A composite type is Oracle's object type**, and the spelling is the whole
difference. PostgreSQL's `CREATE TYPE t AS (a NUMBER)` is *accepted* here and
creates nothing usable — `USER_TYPES` reports `ATTRIBUTES 0` with
`INCOMPLETE YES`, `USER_OBJECTS` reports `INVALID`, and the driver returns no
error at all. Ptah writes `CREATE OR REPLACE TYPE t AS OBJECT (...)`, which is
the statement that creates one, and reads it back from `ALL_TYPES` and
`ALL_TYPE_ATTRS`.

An object type is compared by its attributes, in order, names without regard
to case and types in the spelling the catalog reports, so `NUMERIC(5,2)` and
`DECIMAL(5,2)`, `VARCHAR(10)` and `VARCHAR2(10)`, or `TIMESTAMP` and
`TIMESTAMP(6)` agree. One whose attributes changed is replaced with the same statement,
which Oracle refuses with `ORA-02303` while a column uses the type.

Object attribute reads preserve `DECIMAL` precision and scale, including a
negative scale. An unconstrained `FLOAT` remains `FLOAT`. A `TIMESTAMP` keeps
its fractional precision, an `INTERVAL` its two precisions, and a `VARCHAR2` or
`CHAR` length its unit, which `ALL_TYPE_ATTRS` reports in `CHAR_USED`. The
catalog names the time zone forms `TIMESTAMP WITH TZ` and
`TIMESTAMP WITH LOCAL TZ`, which no statement accepts, and the read writes
them out as `TIMESTAMP(6) WITH TIME ZONE` and
`TIMESTAMP(6) WITH LOCAL TIME ZONE`. SQL schema files accept the full local
time zone spelling with or without an explicit fractional precision.

The read describes the subset the model can carry, and records the rest as
found and not described rather than flattening it:

- an object type with a method, a subtype, a `NOT FINAL` or `NOT INSTANTIABLE`
  type, a collection type (`VARRAY`, `TABLE OF`) and an incomplete shell;
- on 23, a domain of several columns, an enumeration domain (`AS ENUM`) and a
  domain made by `CREATE FLEXIBLE DOMAIN`.

Describing one by its attribute list or its first column alone would say a
replay produces the same type when it produces a different one. A declaration
that names such a type plans nothing for it: the comparison reports it as
undecided, because `CREATE OR REPLACE TYPE` would replace the type the schema
holds. `schema inspect --format sql` writes a comment for each one.

Replacing a type a column uses answers `ORA-02303` and changes nothing — the
server declining to leave that column naming a shape it no longer has, which
is kept rather than forced.

## Synonyms

A synonym is declared with `//ptah:schema:synonym` or the HCL `synonym` block,
as on SQL Server, and the same declaration renders on both targets. Ptah
writes `CREATE SYNONYM`, reads the schema's own synonyms back from
`ALL_SYNONYMS`, and plans a changed target as a drop and a create. Oracle
records the target's owner even when the statement left it out, so a declared
`orders` and a stored `APP.ORDERS` are one target for a connection whose schema
is `APP`. A synonym through a database link is left as it is: a declaration has
no place for the link, so it is neither dropped nor declared again. Public
synonyms belong to `PUBLIC` and are not read.

Go export preserves synonym names, schemas, targets, and declaration comments,
binds the alias to Oracle and SQL Server, and refuses incomplete coverage and a
target scope the annotation cannot preserve.

## Views that read other views

Oracle checks a view's query when it creates the view, so Ptah creates each
view after the views it reads and refuses views that read one another. The
[MySQL page](../mysql/#views-that-read-other-views) says what counts as a read.

## Unique keys

A column gains its `UNIQUE` with `MODIFY (a ... UNIQUE)` and loses it with
`DROP UNIQUE (a)`, since Oracle names the key. A foreign key the plan drops
over that key goes first. A column of a key over several columns is not unique
by itself.

## Constraints that defer their check

A primary key, a `UNIQUE` and a table's `CHECK` can be `DEFERRABLE`, with
`INITIALLY DEFERRED` or `INITIALLY IMMEDIATE`, as a foreign key can. Ptah writes the clause and
reads it back from `ALL_CONSTRAINTS`, measured on Oracle Free 23. A primary
key Oracle named itself is compared through the table's column flags, which
cannot defer, so a deferrable one is compared as the table's key instead.
Name a deferrable `UNIQUE`: Oracle names an unnamed one `SYS_C<n>`, which
differs in every database. A change of deferral is a drop and an add. A key
that does not defer reads back with no deferral clause, so an inspected file
compares clean with its account.

A `CHECK` written on a column takes a deferral clause as a table's does, and
one that defers is compared as a table's `CHECK`. Oracle fixes a `CHECK`'s
deferrability when it creates it, so one whose deferral differs from the
declaration is replaced. The plan adds the new `CHECK` before it drops the old
one: a named one under a temporary name, renamed to the declared name after the
drop. Oracle commits each statement, so a row that refuses the new `CHECK`
stops the plan with the old one still in place. The temporary name is the same
on every run for one table and constraint, so the next plan drops a `CHECK` a
stopped run left under it before it adds the new one again.

No declaration disables a `CHECK` or a foreign key, or leaves its rows
unchecked, so the parser refuses `DISABLE` and `NOVALIDATE`. One the account
holds `DISABLE`d, or `ENABLE NOVALIDATE`d, whose definition is the one declared
is enabled in place with `ALTER TABLE ... ENABLE VALIDATE CONSTRAINT`. A row
the constraint refuses fails the statement, with `ORA-02293` for a `CHECK` and
`ORA-02298` for a foreign key, and the constraint stays as it was:

```text
error: apply schema changes: failed to execute SQL statement: oracle: SQL execution failed: ORA-02293: cannot validate (APP.CK_QTY) - check constraint violated
SQL: ALTER TABLE APP.ORDERS ENABLE VALIDATE CONSTRAINT CK_QTY
```

Every later apply plans the same statement and fails the same way, because the
declaration says the rows are checked and the database says they are not.
There are two remedies. Fix the rows the constraint refuses, and the next apply
validates it. Or leave the constraint out of the comparison by its stored name,
`--exclude 'ORDERS.CK_QTY[type=check]'` for a `CHECK` or `[type=foreign_key]`
for a foreign key, and the apply leaves it unvalidated.

## Functions and procedures

**Functions and procedures** are rendered, read back and planned on both lines.
Their body is PL/SQL, which is what the declaration says:

```go
//ptah:schema:function name="fn_double" params="p IN NUMBER" returns="NUMBER" language="plsql" body="BEGIN RETURN p * 2; END;"
```

A declaration that omits `language=` is in PL/SQL, the target's own language,
and one that omits `security=` runs as `AUTHID DEFINER`, the server's own
default.
One that declares another language, such as `plpgsql`, is named by the renderer,
which creates nothing rather than writing a body this server cannot run.
Two other shapes are named the same way: a parameter default, because
`ALL_ARGUMENTS` reports that one exists and never what it is, so a routine
created with one would be replanned on every run. And `volatility="STABLE"` is
refused, because Oracle reports determinism as `YES` or `NO` only — `IMMUTABLE`
is the `DETERMINISTIC` clause and `VOLATILE` is its absence, and there is no
third cell that does not either lie to a function-based index or diff forever.

A standalone routine has no overloads. A schema that declares two functions, or
two procedures, of one name with different parameters is refused before any
statement is planned. Ptah writes `CREATE OR REPLACE`, and on Oracle 23.26 the
second statement replaces the first without an error: one routine is left,
holding the second body, and a call with the first one's arguments fails with
`PLS-00306`. A function and a procedure of one name are left to the server,
which refuses the second with `ORA-00955` and keeps the first.

The semicolon that closes a PL/SQL block belongs to the block, not the client,
so Oracle's own tooling ends one with a `/` on the next line.
A `CREATE` handed to the server without it returns **no error at all** and
leaves the object `INVALID`: `USER_TRIGGERS` still reports such a trigger
`ENABLED`, and `USER_PROCEDURES` omits the routine. Ptah keeps that
semicolon on every statement it sends.

## Versioned migrations

`ptah migrations up`, `down` and `status` run against Oracle, with either
revision table format. The revision table has an Oracle spelling of its own:
`NUMBER` and `CLOB` columns, `DEFAULT` written before `NOT NULL`, and a PL/SQL
block that creates the table only when it is absent, because the 21 line has no
`IF NOT EXISTS`. Oracle stores an empty string as `NULL`, so the text columns of
that table are nullable. The spelling is measured on the 23 line; on the 21 line
it rests on the capability preset.

Each statement of a migration commits as it runs, under `--tx-mode file` as
under `none`. Oracle commits before every schema statement, and Ptah opens no
transaction around the body, so when a statement fails, the statements before
it stay applied and the revision row counts them. See
[what a failed body records](../../versioned/apply/). For the same reason
`--tx-mode all` is refused.

`--migrations-engine` is refused on Oracle, because the revision table has no
engine clause to carry it.

### The migration lock

Two `ptah migrations up` runs against one schema take the migration lock in
turn: the second waits for the first, then finds nothing pending.
`--migration-lock-timeout` bounds the wait, and a lock
still held when it ends refuses the run with `timed out acquiring migration lock
"ptah_migrate" for oracle after 10s`. `ptah schema apply` takes its own lock the
same way, a dry run included. A dry run creates neither the lock table nor the
lock's row, so where no run has taken that lock yet it holds none, and says so
on standard error.

The lock is a row lock. Ptah keeps one row per lock name in the table
`ptah_locks`, which it creates beside the revision table the first time it
takes a lock there: in the session's schema, or in the schema
`--migrations-schema` names (`--revisions-schema` on `ptah-compat`). So two
accounts that record in one revision table take one lock. The row's key is the
lock name exactly as given, `ptah_migrate` unless `--migration-lock-name` names
another, and Ptah locks that row with `SELECT ... FOR UPDATE NOWAIT`. It asks
again every 25 milliseconds until the wait ends, so waiting runs are not served
in the order they arrived. On other engines a table of that name is an ordinary
one.

Creating the table needs `CREATE TABLE` in the account's own schema, and
`CREATE ANY TABLE` in a schema another account owns. Without that privilege, a
DBA creates the table once and grants the account `SELECT`, which
`FOR UPDATE` needs, and `INSERT`, for the row of each new lock name. `READ`
in place of `SELECT` is refused by `FOR UPDATE`.

```sql
CREATE TABLE "APP".ptah_locks (name VARCHAR2(1000) PRIMARY KEY);
GRANT SELECT, INSERT ON "APP".ptah_locks TO deployer;
```

A lock table in another account's schema belongs to that account, so the run
needs `PTAH_ALLOW_FOREIGN_METADATA_TABLE=1`, as the revision table there does.
A table created in a schema is the schema's account's, whoever creates it, so
without the variable the run refuses before it creates or writes anything
there: a lock table, revision table or log that does not exist yet is refused
as one that does, even where `CREATE ANY TABLE` would create it.

Where neither the table nor the privilege is there, the lock refuses before the
run starts and names both remedies. It does not fall back to the account's own
schema, where a table would not exclude another account recording in the same
revision table.

The revision table and its log follow the same rule. Ptah sends no `CREATE` for
a metadata table that `ALL_TABLES` shows the account, so tables a DBA created
in another account's schema need only `SELECT`, `INSERT`, `UPDATE` and
`DELETE`. Oracle checks the privilege before the name, and its guarded
`CREATE` of an existing table answers ORA-01031 to an account without
`CREATE ANY TABLE`. Such tables belong to another account, so they also need
`PTAH_ALLOW_FOREIGN_METADATA_TABLE=1`, as described in
[Maintain migration history](../../versioned/maintain-history/).

Do not name a table of your own `ptah_locks` on Oracle. Schema reads leave out
the table as the lock creates it, unquoted, which Oracle stores as
`PTAH_LOCKS`, as they leave out the revision table. So `schema diff` and
`schema apply` never see a table of that name, the lock fails when its columns
are not the ones Ptah creates, and `db drop-all` drops it with the rest.

The rows are never deleted, and a row holds no state of its own: only the lock
on it does, and the lock ends with the session's transaction. A run that
crashes leaves its row behind, unlocked once the server ends the session, and
the next run takes the lock. The lock session runs nothing but pings and the
lock statement, so it is idle when its client dies. When the client's host
closes the connection, the server notices at once: one second after the client
of an idle holder was killed with `SIGKILL` on its own host, another session
locked the row.

A host that fails, or a network partition, closes nothing, and the session
keeps the row until the server's dead connection detection notices, which
`SQLNET.EXPIRE_TIME` in the server's `sqlnet.ora` configures. A waiting run
waits without end unless `--migration-lock-timeout` bounds it, so set one where
a run must not wait that long. A held row cannot outlive the session that holds
it.

The first time a lock name is taken, Ptah inserts its row, and Oracle makes
that INSERT wait for another session's uncommitted INSERT of the same name.
Neither `--migration-lock-timeout` nor a run that does not wait ends that wait:
Oracle bounds no INSERT, and a canceled call does not stop the server's wait.
Ptah's own runs commit the row in the call that inserts it, so only a session
outside Ptah that inserts into `ptah_locks` without committing can hold a run
there.

Oracle commits every schema statement, and a commit gives a row lock back. So
the lock lives on a database session of its own that runs nothing else, and
the migration runs on other sessions. Each run holds that one extra session for
as long as it holds the lock. Oracle's named lock, `DBMS_LOCK`, would survive a
commit, but only `SYS` can grant `EXECUTE` on it, so an ordinary account cannot
take it.

The migration lock is what serializes runs, retries included. A run that holds
no lock, such as `ptah-compat migrate apply --skip-lock`, is protected only
where its read of the revision table is stale by the time it writes. When
another run recorded the version after this run found no row for it, the
INSERT of the revision row fails on its key, and the run stops with `another
run recorded this migration after this run read the revision table` before any
statement of the body. An `--allow-dirty` retry rewrites the failed row only
while it still has the state, applied count and checksum the retry read, so a
retry that read the row before another retry rewrote it stops with the same
error. Two retries that both read the row before either writes it both pass
that check; see
[Locking](../../versioned/apply/#locking-and---migration-lock-timeout).

## Open work

What remains for Oracle is tracked by
[#1875](https://github.com/stokaro/ptah/issues/1875) and
[#1920](https://github.com/stokaro/ptah/issues/1920).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- Declaring functions, types, and constraints in Go sources: [Go annotation reference](../../reference/go-annotations/).
