---
title: Drift findings
description: Every category a drift report counts a change under, with the severity it carries.
type: reference
audience:
  - "ci-operator"
  - "database-engineer"
readerQuestion: "Which finding categories can a drift report name, and how severe is each?"
goal: "Look up every category a drift report can name and the severity it carries."
sourceOfTruth:
  - "migration/safety"
  - "internal/cli/drift"
generated: false
searchAliases:
  - "drift categories"
overlaps: []
disposition: keep
---

`ptah schema drift` counts every difference between the desired schema and the
database under a finding category, and `ptah schema serve` counts the same
categories in its drift panel. A finding is named for the kind of object it
counts and for what applying the desired schema would do to it: `<kind>_added`
counts the objects the database lacks, `<kind>_removed` the objects the desired
schema does not declare, and `<kind>_modified` the objects both hold that
differ. Every difference the comparison reports is counted under a category, so
a report whose `drift` is true has at least one finding.

The categories are not disjoint: a change that has a category of its own, such
as a removal that takes a UNIQUE constraint's enforcement with it, is counted
under its kind as well. `migration/safety/categories_doc_test.go` fails when the
classifier reports a category this page does not list, when a row here names
one it does not report, or when the two give a category different severities.

## Kinds of object

| Kind | `_added` | `_removed` | `_modified` |
| --- | --- | --- | --- |
| `columns` | warning | destructive | warning |
| `constraints` | warning | destructive | |
| `enum_values` | warning | destructive | |
| `enums` | safe | destructive | |
| `functions` | safe | destructive | warning |
| `grants` | safe | warning | |
| `indexes` | warning | warning | |
| `materialized_views` | safe | destructive | destructive |
| `rls_enabled_tables` | safe | destructive | |
| `rls_policies` | safe | destructive | warning |
| `role_memberships` | safe | warning | |
| `roles` | safe | destructive | warning |
| `schemas` | safe | destructive | warning |
| `sequences` | safe | destructive | warning |
| `tables` | safe | destructive | |
| `triggers` | safe | destructive | warning |
| `views` | safe | destructive | warning |

A blank cell is a direction the kind has no category for. A table or an enum
type that differs is counted by what differs in it: its columns, its values or
its comment.

Removing an object is destructive when something is lost that the report cannot
give back: a table's or a materialized view's rows, a sequence's position, the
definition of a view, a function or a trigger that readers and writers rely on.
Removing an index, a privilege or a membership is a warning: nothing stored goes
with it, and a reader or a writer that relied on it slows down or is refused
until it is restored. A modified materialized
view is destructive because the plan drops it and creates it again; one whose
only change is a setting its owner changes in place keeps its rows and is a
warning.

The plan check rates the statements a plan writes for a change as this page
rates the change, and tests plan each removal and each replacement on the
engines that have it and fail when the two ratings differ:

- `ptah migrations plan --check-destructive` refuses a plan that drops a
  sequence, a view, a materialized view or a trigger, and rates a `REVOKE` a
  warning.
- A drop of a view or a trigger that the same plan creates again, under the
  same name and for a trigger on the same table, replaces the object rather
  than removing it. It is a warning, as `views_modified` and
  `triggers_modified` are, and so is a `CREATE OR REPLACE` of either.
- A drop with `CASCADE` stays destructive even when the object is created
  again: `CASCADE` also drops every object that depends on it, and creating it
  again does not bring them back. A PostgreSQL view whose column list changes
  is dropped that way, so the plan check refuses a plan that changes one while
  the drift report counts a modified view.
- A sequence and a materialized view lose their position or their rows when
  they are dropped, so a drop of either is destructive whatever follows it.

## Settings and parts of an object

These categories count a setting or a part of an object rather than the object:

| Category | Severity | What it counts |
| --- | --- | --- |
| `comments_changed` | safe | Comments the plan writes in place on tables, indexes, constraints and the other objects that take a comment through a statement of their own. A column's comment is counted under `columns_modified`. |
| `constraints_validated` | warning | Constraints the database holds `NOT VALID` and the plan validates. Validation reads every row and fails on one the constraint refuses. |
| `grant_options_added` | safe | Privileges the database holds without the `WITH GRANT OPTION` the desired schema gives them. |
| `grant_options_removed` | warning | Privileges the database holds `WITH GRANT OPTION` and the desired schema grants without it. |
| `index_visibility_changed` | warning | Indexes whose visibility to the optimizer changes: MySQL's `VISIBLE` and `INVISIBLE`, MariaDB's `IGNORED`. |
| `indexes_renamed` | warning | Indexes the plan renames in place, on YDB. A query that names the index stops finding it. |
| `rls_force_added` | safe | Tables whose `FORCE ROW LEVEL SECURITY` the plan turns on. |
| `rls_force_removed` | destructive | Tables whose `FORCE ROW LEVEL SECURITY` the plan turns off, which exempts the owner from the table's policies. |
| `unique_protections_removed` | destructive | Index removals that take a UNIQUE constraint's enforcement with them. Each is one of the `indexes_removed` as well. |
| `vector_dimension_changed` | destructive | Column modifications that change a pgvector dimension, which the server refuses while a row holds a vector. Each is one of the `columns_modified` as well. |

## Feature owners, undecided objects and rows

A change a feature owner compares, such as a PostgreSQL extension, policy or
domain, is named `feature_changes:<change kind>` with the severity the owner
gives it. An owner that assesses the change's effect on access adds
`feature_access_widened:<change kind>`, `feature_access_narrowed:<change kind>`,
`feature_access_unchanged:<change kind>` or `feature_access_unknown:<change kind>`.
A declared object the read could not check is counted under one `undecided`
finding at `warning` severity; see
[Drift: the difference as a check](../../direct/compare-and-drift/#drift-the-difference-as-a-check).
Reference rows have
[findings of their own](../../direct/compare-and-drift/#reference-rows-are-part-of-the-check).

## The categories as data

`docs/drift-findings.json` in the Ptah repository lists every category a drift
report can carry: the categories above, `undecided`, and the reference rows'
`data_rows_inserted`, `data_rows_updated` and `data_rows_deleted`. The file is
generated, and a test fails when it falls behind the code, so a tool that keeps
its own list of categories can read it at the commit it pins rather than read
Ptah's source:

```bash illustration
git show <commit>:docs/drift-findings.json
```

Each entry names a category, the severities a finding of it can carry from the
lowest to the highest, and the forms it is written in: `bare` for the category
alone, and `qualified` for the category followed by a colon and a feature
owner's change kind.

```json
{
  "category": "feature_access_unknown",
  "severities": ["destructive"],
  "bare": true,
  "qualified": true
}
```

In Go, `migration/safety.FindingCategories` returns the schema comparison's
share of the list in the same shape.
