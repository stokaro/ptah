---
title: Trace view column lineage
description: Run ptah schema lineage to find which base columns each view column and each routine reads, and which tables and columns each routine writes, before dropping or renaming one of them.
type: how-to
audience:
  - "schema-author"
readerQuestion: "How do I run ptah schema lineage to find which base columns each view column and each routine reads, and which tables and columns each routine writes, before dropping or renaming one of them?"
goal: "Identify dependencies before dropping or renaming a database object."
sourceOfTruth:
  - "cmd/schema"
  - "internal/schemalineage"
generated: false
overlaps: []
disposition: keep
owns:
  - cli-ptah-schema-lineage
---

`ptah schema lineage` derives column-to-column dependencies from the view and
materialized-view bodies a schema declares, the columns each routine body
reads, and the tables and columns each routine writes. It answers "what breaks
if I drop this column" before the drop rather than after, and "what changes this
column" alongside it.

The analysis reads a schema source and contacts no database, so it answers
before a change is applied. What it cannot attribute it reports as undecided
instead of dropping, which is the part of the output a reader has to look at.

Prerequisites: an installed `ptah` binary ([Install Ptah](../../start/install/))
and a desired schema. Nothing else — no database, no server.

## Starting state

The examples run against one SQL file holding two tables and two views. Save
this as `schema.sql`:

```sql
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  email TEXT NOT NULL,
  country TEXT NOT NULL
);

CREATE TABLE orders (
  id INTEGER NOT NULL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers (id),
  total_cents INTEGER NOT NULL DEFAULT 0
);

CREATE VIEW order_totals AS
SELECT customer_id AS buyer, total_cents AS cents FROM orders;

CREATE VIEW customer_orders AS
SELECT c.email, o.total_cents FROM customers c JOIN orders o ON o.customer_id = c.id;
```

Substitute your own schema source throughout. `--root-dir` reads
[Go annotations](../go-annotations/) instead of a file, and naming both merges
them into one [composite desired schema](../composite/).

## Trace the columns

```bash
ptah schema lineage --schema-file schema.sql --dialect sqlite
```

Expected output includes:

```text
SOURCE              FEEDS               KIND
orders.customer_id  order_totals.buyer  view
orders.total_cents  order_totals.cents  view

1 view(s) not fully resolved:
  customer_orders: the FROM clause names more than one source, so an unqualified column cannot be attributed
```

`SOURCE` is the base column, `FEEDS` is the view column that reads it, and
`KIND` is `view` or `materialized`. `--dialect` selects how the source is
parsed, not a render target, because the same file parses differently per
dialect. The default is `postgres`.

## Read the undecided list

The second block is what tells you whether the first one is complete. A view
body that resolves only partially is reported under `undecided` with its
reason, and the whole view goes there rather than the one column that could not
be attributed.

`customer_orders` above reads two tables, so `email` and `total_cents` cannot be
attributed to one of them. A reader who saw only the edge list would conclude
that nothing reads `customers.email`, and dropping it on that basis would break
the view.

Four shapes land there, each with its own reason line:

| Shape in the view body | Reason reported |
| --- | --- |
| more than one `FROM` source, such as a join | `the FROM clause names more than one source, so an unqualified column cannot be attributed` |
| a subquery as the `FROM` source | `the FROM clause is a subquery, which this analysis does not resolve` |
| `UNION`, `INTERSECT` or `EXCEPT` | `the body combines queries, which this analysis does not resolve` |
| a function call in the select list with no alias | `a computed select entry has no alias, so its output column has no name here` |

An undecided view does not change the exit code. The run above exits `0`.

## Trace what a routine reads

A routine reads base columns the same way a view does, and is traced on the
same terms. Save this as `routines.sql`:

```sql
CREATE TABLE customers (
  id INTEGER NOT NULL PRIMARY KEY,
  email TEXT NOT NULL,
  country TEXT NOT NULL
);

CREATE FUNCTION customer_emails() RETURNS SETOF TEXT
LANGUAGE sql AS $$ SELECT email FROM customers $$;

CREATE FUNCTION touch_customer(target INTEGER) RETURNS void
LANGUAGE plpgsql AS $$ BEGIN UPDATE customers SET country = 'CZ' WHERE id = target; END; $$;
```

```bash
ptah schema lineage --schema-file routines.sql --dialect postgres
```

Expected output:

```text
SOURCE           READ BY          KIND
customers.email  customer_emails  function

SOURCE        READ BY         STATEMENT
customers.id  touch_customer  update

TARGET             WRITTEN BY      STATEMENT
customers.country  touch_customer  update

1 routine(s) not fully resolved:
  touch_customer: the body is plpgsql: every statement was classified, so the writes are complete; the reads are those of its statements that name one table
```

Two of those tables say `READ BY` and are not the same list. The first is a
routine whose whole body is one `SELECT`, resolved column to column like a view;
the second is a column a statement inside a procedural body reads, and its third
column names the statement. A routine appears in one or the other, never both.

The boundary is the same one the view half draws, in the same place. A body
that is a single `SELECT` -- which is the shape a `LANGUAGE sql` routine has --
resolves to the columns it reads, through the same reader and with the same four
undecided shapes in the table above.

A procedural body is resolved for what it **writes**: the tables and columns its
`INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` statements name, including the ones
inside an `IF` or a loop. Its reads are not derived, so it is always reported in
`undecided` as well, and the reason says which of two things happened:

| Reason ends | Meaning |
| --- | --- |
| `every statement was classified, so the writes are complete; ...` | the write list above is the whole set |
| `..., so the writes are not complete; ...` | something in the body could write and was not resolved |

The clause after the semicolon says what was established about the reads, and it
depends on the engine rather than on the body:

| Reads clause | Meaning |
| --- | --- |
| `the reads are those of its statements that name one table` | reads were derived; a statement with two tables in scope contributed none |
| `the columns it reads are not resolved, because a declared variable takes precedence over a column of the same name on this engine ...` | MySQL, where the two cannot be told apart |

Four things land in the second row: an `EXECUTE`, which composes its statement
at run time; a statement whose leading word is not one this analysis knows,
because `CALL`, `MERGE` and `COPY` all write; a `TRUNCATE` naming more than one
table, refused rather than half-read; and a control-flow statement whose
contents the parser could not split.

### Which dialect can answer about reads

A read is a column named in a statement, and the danger is a local variable or
parameter carrying a column's name: reporting it as a read of that column would
be a wrong answer about the one question this verb exists for. Each engine
resolves that ambiguity differently, and the difference is what decides whether
reads can be answered at all.

| Dialect | Variable against column | Reads |
| --- | --- | --- |
| the PostgreSQL family | `plpgsql.variable_conflict` defaults to `error`, so a body that compiles has no collision | derived |
| `sqlserver` | variables carry an `@` prefix and cannot collide with a column name | derived |
| `mysql`, `mariadb` | a declared variable takes precedence over a column of the same name | not derived, and the reason says so |

A column a statement assigns to is a write and not a read. In `SET country =
'CZ' WHERE id = 1` only `id` is read; in `SET email = lower(email)` the column
`email` is both, and appears in both lists.

### Which dialect reads the body

Each engine writes routine bodies in its own procedural language, and each has
its own parser here. `--dialect` is what selects one; there is no shared parser
to fall back on, because a body read by the wrong one is a wrong answer rather
than a cautious one. A dialect with no routine-body parser reports that, by
name, instead of being analyzed by whichever parser was reachable.

| Dialect | What resolves |
| --- | --- |
| the PostgreSQL family | every writing statement, including the ones inside an `IF` or a loop |
| `mysql`, `mariadb` | every writing statement at the top level of the body |
| `sqlserver` | every writing statement at the top level of the body |
| any other | nothing, reported as having no routine-body analysis |

One limit in that table is the body model rather than this analysis. The MySQL
and T-SQL models carry no statements inside a branch, so an `IF` is reported as
a statement whose contents could not be read -- not as a branch that writes
nothing.

That distinction is the whole point of reading the list. A routine reported with
writes and nothing else would say its reads are none rather than unknown.

## Take the answer as JSON

```bash
ptah schema lineage --schema-file schema.sql --dialect sqlite --format json
```

Expected output includes:

```json
{
  "edges": [
    {
      "from_table": "orders",
      "from_column": "customer_id",
      "to_view": "order_totals",
      "to_column": "buyer"
    },
    {
      "from_table": "orders",
      "from_column": "total_cents",
      "to_view": "order_totals",
      "to_column": "cents"
    }
  ],
  "undecided": [
    {
      "view": "customer_orders",
      "reason": "the FROM clause names more than one source, so an unqualified column cannot be attributed"
    }
  ]
}
```

`edges` is always present and is `[]` when nothing resolved. `undecided` appears
only when at least one view landed there. A check that wants to fail on an
unresolved view reads that key, since the exit code stays `0` either way.

Routines carry their reads, their writes and their undecided list under
`routines`, so a reader parsing `edges` keeps parsing exactly the view edges it
always parsed:

```json
{
  "edges": [],
  "routines": {
    "edges": [
      {
        "from_table": "customers",
        "from_column": "email",
        "to_routine": "customer_emails",
        "kind": "function"
      }
    ],
    "reads": [
      {
        "table": "customers",
        "column": "id",
        "by_routine": "touch_customer",
        "kind": "function",
        "statement": "update"
      }
    ],
    "writes": [
      {
        "table": "customers",
        "column": "country",
        "by_routine": "touch_customer",
        "kind": "function",
        "statement": "update"
      }
    ],
    "undecided": [
      {
        "routine": "touch_customer",
        "reason": "the body is plpgsql: every statement was classified, so the writes are complete; the reads are those of its statements that name one table",
        "kind": "function"
      }
    ]
  }
}
```

## Failure modes

A completed trace exits `0`, whether or not any view landed in `undecided`. A
usage error exits `2` with one line on stderr. See
[Exit codes](../../reference/exit-codes/) for the contract.

## Limitations

- The resolvable shape is a select list over a single `FROM` source, with
  columns named plainly or aliased. The four shapes in the table above land in
  `undecided` instead.
- A routine's body-wide **read** resolves only when the body is one such
  `SELECT`. Inside a procedural body, a read resolves per statement, and only
  where that statement names one table -- which is why every procedural routine
  appears in `undecided` even when its writes are complete.
- A **write** is resolved from the statement that performs it, so it is exact
  about the table and about the columns an `UPDATE` assigns or an `INSERT`
  lists. Which statements are reached depends on the dialect; see the table
  above. A statement naming the table alone -- a `DELETE`, a `TRUNCATE`, an
  `INSERT` with no column list -- is reported with no column, which means the
  whole table rather than an unknown column.
- `SET (a, b) = (...)` is not read as a column list. The statement goes
  unresolved instead, because reading the left-hand names as columns would work
  and reading the right-hand tuple as more of them would not.
- An arithmetic expression in the select list with no alias produces an edge
  that is wrong rather than an `undecided` entry. For a view declared
  `SELECT a + b FROM t`, the command reports one edge, `t.a` feeding a view
  column named `b`: the source is the first identifier in the expression, and
  the target names a column the view does not have. Nothing reports `t.b`,
  which the view reads. Alias the expression — `SELECT a + b AS total` — and
  both edges resolve correctly.
- There is no way to ask about one column or one view. The whole graph is
  written and the caller filters it.
- With neither `--root-dir` nor `--schema-file`, the working directory is
  scanned for Go annotations. In a directory that holds none, the run reports
  `No view or routine columns to trace.` and exits `0`, which reads the same as
  a schema with no views. Name the source.

## Trace a live database

`--db-url` traces the schema a server reports instead of one a file declares,
which is how the same question is asked about a database nobody has a
declaration for:

```bash
ptah schema lineage --db-url "postgres://user:pass@localhost/app" --schemas public
```

The dialect comes from the server rather than from `--dialect`: a lineage traced
against a live database is about that database, and a routine body is read by
its own engine's parser.

`--schemas` narrows what is read, the same way it does for
[`ptah db read`](../../reference/native-commands/). Everything below applies
unchanged -- the resolvable shapes, the undecided list and the JSON document are
the same whether the schema was declared or read back.

## Exact reference

Run `ptah schema lineage --help` for the flag set with its environment
variables. [Column lineage](../../reference/native-commands/#column-lineage)
places the verb in the native tree, and
[Exit codes](../../reference/exit-codes/) carries its row.

## Next steps

- Ready to make the change the trace cleared?
  [Apply directly](../../direct/apply/).
- Want the whole schema drawn rather than one dependency?
  [Visualize the schema](../visualize/).
- Counting what the database holds instead?
  [Count schema objects](../stats/).
