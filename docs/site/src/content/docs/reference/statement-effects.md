---
title: Statement effects
description: What a saved plan records about each statement it can describe, and how the record is versioned.
type: reference
audience:
  - "ci-operator"
  - "database-engineer"
readerQuestion: "How do I read what a planned statement changes without parsing its SQL?"
goal: "Read the effect a plan records for a statement, and know which statements carry one."
sourceOfTruth:
  - "internal/planeffect"
  - "internal/atlasschema"
generated: false
searchAliases:
  - "plan effects"
  - "security_invoker in a plan"
  - "sql_security in a plan"
overlaps: []
disposition: keep
---

A reviewer of a plan often needs to know what a statement changes rather than
what it says. The SQL cannot always tell. `CREATE OR REPLACE VIEW` has to state
every option a view keeps, because the view loses each option the statement
leaves out, so a body change that keeps `security_invoker` reads like one that
turns it on. Ptah computed the plan from the database, so it knows the
difference, and it records it beside the statement as an effect.

## Where effects appear

| Document | Field |
| --- | --- |
| A saved plan: `ptah schema plan --save`, `--output` or `--dry-run` | `effects` on each statement |
| `ptah schema plan --json` | `effects` on each statement of `plan` |
| `ptah schema apply --json` | `effects` on each entry of `units` |

`ptah schema apply --plan` lists the effects the plan file records. Without
`--plan`, the apply document lists the effects of the plan the run computed.

A statement without `effects` is one Ptah does not describe. That says nothing
about what the statement changes: read its SQL. Some plans carry no effects at
all:

- a plan whose statements `--edit` changed; an edit that leaves them as they
  were, comments and whitespace aside, keeps every effect;
- a plan read back from the Atlas `.plan.hcl` format, which has no place for
  them;
- a plan that an [executable provider](../../extend/executable-providers/)
  renders.

## The record

The body change from the first paragraph, on a view that holds
`security_invoker=true`, is recorded like this:

```json
"effects": [
  {
    "version": 1,
    "object": {
      "kind": "view",
      "schema": "public",
      "name": "order_totals"
    },
    "action": "replace",
    "properties": [
      {
        "name": "security_barrier",
        "before": false,
        "after": false,
        "changed": false
      },
      {
        "name": "security_invoker",
        "before": true,
        "after": true,
        "changed": false
      }
    ],
    "before_plan": {
      "exists": true,
      "properties": [
        {
          "name": "security_barrier",
          "value": false
        },
        {
          "name": "security_invoker",
          "value": true
        }
      ]
    }
  }
]
```

| Field | Meaning |
| --- | --- |
| `version` | The version of this record; see [Versioning](#versioning) |
| `object` | The object the statement changes: its `kind`, and its `schema` and `name` as the database spells them |
| `action` | The kind of statement; see the next table |
| `properties` | Each property the effect reports, sorted by `name` |
| `before`, `after` | The value before the statement runs and after it, of the type the property's `name` gives it. Each is absent where the object does not exist at that point |
| `changed` | Whether the two values differ. An absent value differs from every value, so `create` and `drop` change every property |
| `before_plan` | The object as the database held it when the plan started; see [Before the plan](#before-the-plan) |

| Action | Statement |
| --- | --- |
| `create` | `CREATE VIEW`. The view does not exist before it |
| `replace` | `CREATE OR REPLACE VIEW`. The view takes the options the statement states and loses the others |
| `alter` | `ALTER VIEW ... SET` or `ALTER VIEW ... RESET`, one statement each |
| `drop` | `DROP VIEW`. The view does not exist after it |

An effect describes one statement. Its `before` is the state the statements
ahead of it leave, so a plan that drops a view and creates it again records a
`drop` with `before` values and a `create` with `after` values. An
`ALTER VIEW ... RESET` that follows an `ALTER VIEW ... SET` starts from the
options the `SET` left.

`changed` alone does not say that a view gains an option. A `create` of a view
that sets no option records `security_invoker` with `after: false` and
`changed: true`, because the view did not exist before. A reviewer that holds
a plan when a view starts reading with its readers' rights tests for `after`
being `true` where `before` is absent or `false`.

## Before the plan

`before_plan` is the object as the database held it when the plan started, as
the plan read it. Every effect records it, whatever the statements ahead of the
effect did to the object.

| Field | Meaning |
| --- | --- |
| `exists` | Whether the object existed when the plan started. It is always written |
| `properties` | The value each property held then, as `name` and `value`, sorted by `name`. It is absent when the object did not exist |

A `create` has no `before`, because no view of its name exists when it runs.
`before_plan` says whether the plan makes the view again or makes a new one.
This matters where the statement that took the view records no effect for it:
a `DROP ... CASCADE` takes what depends on what it drops, and a `DROP VIEW IF
EXISTS` after it may find nothing, so Ptah does not describe that drop. A
`create` with `before_plan.exists` set to `true` makes again a view the plan
took. Compare its `after` with the `before_plan` value to see whether the view's
rights change. A `create` with `exists` set to `false` makes a new view.

A property that `before_plan` does not list is not described, as in
`properties`. `before_plan` is absent from an effect that a Ptah without the
field wrote, which is not the same as `exists: false`.

## What Ptah describes

Ptah describes views on a target that keeps view options: PostgreSQL, and the
YugabyteDB and CockroachDB lines that have them, as
[capabilities](../capabilities/) lists. It reports two properties for each,
`security_barrier` and `security_invoker`. Each value is a boolean, the one the
server applies, so a view that holds no value for an option reads `false`.

On MySQL and MariaDB, Ptah describes views by their `SQL SECURITY` mode, as
the property `sql_security`. The value is a string, `DEFINER` or `INVOKER`, the
mode the server applies, so a statement that names no mode reads `DEFINER`.
A body change to a view held as `INVOKER`, declared without a mode, is recorded
like this:

```json
"effects": [
  {
    "version": 1,
    "object": {
      "kind": "view",
      "schema": "shop",
      "name": "order_totals"
    },
    "action": "replace",
    "properties": [
      {
        "name": "sql_security",
        "before": "INVOKER",
        "after": "INVOKER",
        "changed": false
      }
    ]
  }
]
```

`sql_security` does not describe the view's definer. A `CREATE OR REPLACE
VIEW` that states no `DEFINER` makes the account that runs it the definer, so a
`DEFINER` view replaced by another account runs with that account's
privileges, though `sql_security` reads unchanged.

The `before` values come from the database the plan read. The plan's source
fingerprint covers the options and the mode, so a view whose options or mode
change after the plan was computed makes the plan stale, and `ptah schema apply
--plan` refuses it.
Ptah does not check the effects again when it reads a plan file. The plan's
digest and an approval signature cover them with the rest of the file.

A statement whose starting state Ptah cannot establish carries no effect. The
usual case is a `CREATE OR REPLACE VIEW` after a statement that drops what
depends on something it drops, with `CASCADE`: the view may or may not exist
when the replace runs. Materialized views and their storage parameters are not
described.

## Versioning

`version` belongs to the effect record. It is separate from the plan file's
`format_version` and from the documents' `contract_version`, which the record
does not change.

- `version` rises when a field of the record changes meaning or leaves.
- A new kind of object, action or property does not raise it, and neither does
  a new field. A consumer treats an entry it does not know as a statement Ptah
  does not describe, and reads the SQL for it.
- A consumer reads an effect only at a `version` it knows.

A consumer that reads no effects is unaffected, because `effects` is optional on
every statement and unit. A reader that refuses fields it does not declare, as a
strict decoder of the plan file does, has to declare `effects` before it reads
a plan that carries it.
