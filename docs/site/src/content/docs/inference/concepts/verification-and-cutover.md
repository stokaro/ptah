---
title: Verification, cutover, and rollback
description: What Ptah checks before a switch, what a cutover binds to, and the conditions under which going back is actually possible.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How does Ptah model verification, cutover, and rollback?"
goal: "Explain Ptah's model for verification, cutover, and rollback."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

Three operations are easy to confuse, and confusing them is expensive. This page
separates them.

- **Verification** measures the new generation. It changes nothing.
- **Cutover** makes the new generation the one queries read. It changes the
  pointer, not the data.
- **Rollback** puts the previous generation back. It is possible only while the
  previous generation is still current.

## What verification checks

`ptah inference verify` runs five layers. Each reports findings, and a finding is
either **blocking** or **advisory**.

| Layer | Asks |
| --- | --- |
| `structural` | Does the vector column exist, with the right type and dimension? Does the index exist and is it valid? |
| `coverage` | Does every in-scope source row have a vector, a deliberate skip, or a tombstone? |
| `freshness` | Was each vector computed from the source as it is now? |
| `vector_validity` | Are the stored vectors the shape the generation declares? |
| `consistency` | Has the backfill finished, has catch-up reached the barrier, and is anything still holding a lease? |

A blocking finding refuses the cutover. An advisory one is reported and does not.

## What verification does not check

It cannot tell you the vectors are *good*. Every layer above is a deterministic
question about shape, coverage, and freshness — none of them asks whether the
search results improved.

`verify` also reports what it did not measure. It checks a stored vector's
dimension without reading every value back, and says so rather than letting a
report that names only what it checked read as though it checked everything.

For quality, `ptah inference evaluate` measures retrieval against a corpus of
questions and expected answers that you write. It is a separate command because
it needs input Ptah cannot derive.

Neither of them proves your application is correct. What a passing run
establishes is narrower and worth stating in full: Ptah verified the configured
structural, freshness, consistency and retrieval-evaluation conditions for the
exact recorded generation. Whether the answers that corpus returns are the ones
your users should get is a question about your application, and no check here
asks it.

## A cutover binds to a plan

Run `cutover` with no approval and it refuses, printing the digest of the plan it
built:

```console
$ ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id my-run
plan 1df24fc375d7
cutover refused:
  - this policy requires an approval and none was given
```

Approve that exact plan:

```bash
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id my-run \
  --approve 1df24fc375d7 --approver "your name"
```

A name given this way is a name the operator wrote down. A signature over the
plan file is a different claim — whose key covered these exact bytes — and the
published record says which it was, in `approval_signed`. A cutover and a
retirement both carry it, and a record with the field absent was authorized by a
name rather than by a key.

The approval binds to the digest. If anything the plan rests on changed between
you reading it and approving it, the digest changes and the approval is refused
rather than applied to a different plan.

The digest covers the plan, not the clock. What is true *now* — the pointer, the
freshness, the findings — is checked again at the moment of the cutover.

### What the plan cites

The plan file an approver signs names every fact the plan digest binds:

```text
ptah inference cutover plan, format 2
generation: 8ddaf10bf421…
replaces:
target: public.articles.embedding_v1
prepared at: 2026-09-02T09:14:22.104Z
verification report: 4c17a2e9b330…
verification passed: false
consistency mode: outbox
consistency watermark: 49731
index ready: true
source mutable: true
consistency blockers: none
accepts blocking finding: 412 rows are stale and this policy allows 0
UNACCEPTED blocking findings: none
plan: 2f7f81ece160…
```

An approval binds to the digest, so anything the digest covers and the file
omits is something the approver signed for and could not have read. The
acceptance of blocking findings is the line that carries that weight: it is what
separates a cutover going ahead over a failed verification from one going ahead
because verification passed, and the two are otherwise the same operation.

Empty lists still write their line. A file silent about accepted findings cannot
be told from one whose author had nothing to say, and the reader deciding is the
one who cannot tell.

`verification report` is a digest of what the report *measured* — the verdict, the row
counts, the findings, the layers that did not run — and not of when it ran. Two
consequences follow, and both are why the line is there:

- A finding appearing, a count moving, or a layer going unmeasured changes the
  line, so it changes the plan digest, so an approval already given stops
  applying. The approval binds to the measurement rather than to a verdict.
- The value is reproducible. Anybody holding the verification record can
  recompute it, which is what makes it a citation rather than a number.

The cutover record written afterwards carries the same value under
`verification_digest`, so the plan and the record cannot disagree about which
report authorized the move.

## What your application has to do

Ptah moves a pointer it keeps in its own tables. Your queries name a column.

Moving the pointer does not rewrite your application's SQL, and Ptah will not do
that for you. Two shapes work:

1. **Read the pointer.** Query Ptah's pointer table for the active generation and
   the column it names, and build your search query from that.
2. **Deploy the column name.** Cut over, then deploy the application change that
   reads the new column. The pointer is then your record of which is live rather
   than the mechanism.

The second is what most teams do, and it means the cutover and the deploy are two
steps you order yourself.

## Rollback is a property, not a button

A stabilization window is what makes going back possible:

```bash
ptah inference cutover ... --stabilize-for 24h
```

That window is not enough on its own, and this is the part worth reading twice.

The previous generation stops receiving changes the moment your queries stop
reading it. Within an hour it is behind the source; within a day it may be far
behind. Going back to it would answer queries from a corpus that no longer
matches your data.

So keeping it a way back means **continuing to catch it up** during the window:

```bash
ptah inference catchup --spec previous-spec.yaml --db-url "$DB" \
  --run-id previous-run --maintain-for 1h
```

`rollback` measures the previous generation before it moves anything: is it
present, is it still being maintained, how many rows are stale or missing, is its
index valid, and was the cutover recent enough for the window. A generation that
drifted is refused — which is the honest answer, not a gap.

A cutover run without `--stabilize-for` leaves no rollback at all, and says so at
the time.

## Retirement is separate and permanent

`retire` drops a generation's index and, by default, its column. The vectors
live in that column, so `--drop-column=false` keeps them: the run becomes an
index drop. It takes the same digest-bound approval a cutover does, the digest
binds which of the two answers was approved, and it is refused while queries
still read the generation.

There is no undo. The vectors are gone, and rebuilding them means paying the
provider for the whole corpus again.
