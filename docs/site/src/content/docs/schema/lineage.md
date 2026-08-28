---
title: Trace view column lineage
description: Run ptah schema lineage to find which base columns each view column and each routine reads, before dropping or renaming one of them.
---

`ptah schema lineage` derives column-to-column dependencies from the view and
materialized-view bodies a schema declares, and the columns each routine body
reads. It answers "what breaks if I drop this column" before the drop rather
than after.

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

1 routine(s) not fully resolved:
  touch_customer: the body is plpgsql rather than plain SQL
```

The boundary is the same one the view half draws, in the same place. A body
that is a single `SELECT` -- which is the shape a `LANGUAGE sql` routine has --
resolves through the same reader, with the same four undecided shapes in the
table above. A procedural body written in PL/pgSQL or any other language lands
in `undecided` naming its language, because resolving control flow needs more
than this analysis does today.

That distinction is the whole point of reading the list. `touch_customer`
writes `customers.country`, and nothing above says so: an undecided routine is
a routine whose dependencies were not established, not one with none.

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

Routines carry the same two halves under `routines`, so a reader parsing
`edges` keeps parsing exactly the view edges it always parsed:

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
    "undecided": [
      {
        "routine": "touch_customer",
        "reason": "the body is plpgsql rather than plain SQL",
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
- A routine body resolves only when it is one such `SELECT`. A procedural body
  is reported as undecided naming its language, and the columns it writes are
  never reported at all -- lineage answers what is read, not what is written.
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
