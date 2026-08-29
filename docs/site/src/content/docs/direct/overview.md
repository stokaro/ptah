---
title: Direct schema changes
description: How a change reaches a database without a migration file - the read, drift, plan, and apply loop, and the history it does not keep.
type: landing
audience:
  - "database-engineer"
  - "ci-operator"
readerQuestion: "When should I use direct schema changes, and which command comes next?"
goal: "Choose the direct workflow when appropriate and open the correct next task."
sourceOfTruth:
  - "cmd/schema"
  - "migration/schemadiff"
  - "migration/planner"
generated: false
overlaps:
  - "/start/choose-a-workflow/"
  - "/direct/apply/"
disposition: keep
---

You have a description of the schema you want and a database that does not
match it, and you would rather move the database now than commit a migration
file first. In a direct change, Ptah reads the live schema, computes the
difference against the desired schema, and executes it. Nothing is written to a
migration directory, and nothing is recorded in a revision table.

## Direct names how the change lands, not how it was written

This group is named for how a change reaches the database.
`ptah migrations generate` reads the same desired schema and turns the same
difference into a reviewed pair of SQL files, so "declarative" does not separate
the two groups. What separates them is whether the difference runs now or is
committed as a file first. [Choose a workflow](../../start/choose-a-workflow/)
covers that decision alongside the independent question of where the change
comes from.

## The loop

Four verbs, in the order a change passes through them: read the database,
compare it with the desired schema, save the difference for review, run the
saved plan.

```bash
ptah db read      --db-url "$DATABASE_URL"
ptah schema drift --db-url "$DATABASE_URL" --schema-file schema.sql
ptah schema plan  --db-url "$DATABASE_URL" --schema-file schema.sql --output change.plan.json
ptah schema apply --db-url "$DATABASE_URL" --plan change.plan.json
```

Only the last step changes the database. `ptah schema apply` also runs without a
saved plan: it computes the change, prints the SQL, and prompts for approval
before executing it.

| Task | Page |
| --- | --- |
| Read a live schema as SQL, Go models, HCL, or JSON | [Inspect a database](../inspect/) |
| See how a database differs, and fail a pipeline when it does | [Compare and drift](../compare-and-drift/) |
| Separate review from execution with a signed plan file | [Plan and approve changes](../plan-and-approve/) |
| Run the change against the database | [Apply directly](../apply/) |

## What it costs

The live database is the only record of what happened. Run `ptah migrations
status` against a database changed this way and it reports `Current Version: 0`
and `Applied Migrations: 0`, whatever the schema now holds. No migration wrote a
revision row. Rolling back means computing a new difference toward the schema
you want back, rather than replaying a committed down file.

Three surfaces stand in for that history, and they are the whole of the audit
trail:

- `--dry-run` prints the SQL and changes nothing.
- A saved plan file records a fingerprint of the state it was computed against,
  and can require a reviewer's signature.
- `ptah schema drift` fails a pipeline once a database has diverged.

## What this group leaves to other pages

Writing the desired schema is a separate subject. The formats Ptah reads, and
how several of them merge into one, are under
[Work with a desired schema](../../schema/work-with-a-source/). Recording each
change as ordered files that every environment replays in the same order is
[Versioned migrations](../../versioned/overview/).

## Next steps

- Starting from a database you did not create?
  [Inspect a database](../inspect/).
- Changing a database somebody else also uses?
  [Plan and approve changes](../plan-and-approve/).
- Still weighing this against migration files?
  [Choose a workflow](../../start/choose-a-workflow/).
