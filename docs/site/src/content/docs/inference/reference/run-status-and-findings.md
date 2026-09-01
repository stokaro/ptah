---
title: Run status and findings
description: Reading what status reports, what each verification layer measures, and what a blocking finding means.
type: reference
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I interpret inference run status and verification findings?"
goal: "Interpret inference run status and verification findings."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

## Reading `status`

```console
$ ptah inference status --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles
run 2026-08-articles: caught_up, running
  - generation: 31122cc8322d44317514ca2b54f29853f1c43d19ecd3b2a1b183320ef8f5bb37
  - scanned 48231, embedded 48200, skipped 31, deleted 0
  - 965 batches committed, 0 retries since the last one
  - 6104382 prompt tokens, 6104382 total, as the provider reported them
  - snapshot boundary: 8842
  - catch-up watermark: 9017
  - lease: worker-1, fencing token 1
```

| Line | Means |
| --- | --- |
| `caught_up, running` | The phase reached, and the run's state: `running`, `paused`, `failed`, or `complete` once the run reached `retired` |
| `generation` | The identity this run is building |
| `scanned / embedded / skipped / deleted` | Rows read, rows given a vector, rows deliberately skipped, rows tombstoned |
| `batches committed` | How many provider round trips landed |
| `prompt tokens` | What the provider said it charged for, which is the number to compare against an invoice. Ptah counts none of its own |
| `the provider reported no token usage` | Stands in place of the line above when no answer carried a usage object. A provider that charged zero and one that said nothing both leave the counts at zero, and this is which |
| `snapshot boundary` | The point the backfill embeds the source as of |
| `catch-up watermark` | How far catch-up has read past it |
| `snapshot_done` | Whether the backfill's walk ran off the end of the source. In the JSON only; the text form says it through the consistency finding |
| `lease` | Who holds the run, and which token may still commit |

A catch-up watermark is usually a transaction identity, as above: every
transaction below it is processed in full. A run stopped partway through a
transaction — a page that filled before that transaction ended — records the
sequence it reached as well, and reads `9017:412`. Both are ordinary; the second
says the next `catchup` resumes inside transaction 9017 rather than after it.

### The phase

The phase is a high-water mark: it records the furthest point the run reached,
not where it is now. Running `catchup` again after a verification leaves it at
`verified`.

The order is `boundary_captured`, `backfilling`, `backfilled`, `caught_up`,
`indexed`, `verified`, `cut_over`, and then either `rolled_back` or `retired`.

Those last two are not the same kind of end. `retired` is where a run stops:
the corpus is destroyed, nothing further is possible, and the run's status
becomes `complete` and its lease is released. `rolled_back` is reversible,
because the rollback it records is — cutting the generation over again returns
the run to `cut_over`, and a generation rolled off the pointer can still be
retired later. A generation merely *replaced* by a newer cutover is neither: it
keeps `cut_over`, because that is the furthest point its run reached, and which
generation queries read now is what the pointer says.

`backfilling` and `backfilled` are two facts, not one worded twice. A run is at
`backfilling` while it walks the snapshot and at `backfilled` once the walk
reached the end.

Neither of them decides whether the snapshot is complete, and the phase cannot:
a high-water mark records the furthest point reached, so a run whose backfill
finished and was then given more to do — a resumed pass that failed partway —
is still at `backfilled` with work left. `status` reports the answer separately,
as `snapshot_done`, and the backfill is what writes it: the flag follows the
last page its walk saw, and only the page that ran off the end of the source
sets it.

Rows written after a walk has finished do not reopen it. Under `outbox` those
belong to catch-up, which is what the barrier finding is about; under
`immutable` they are a source that changed when the specification said it would
not, and coverage reports them as rows with no vector.

### `skipped` is not `missing`

A skipped row is one the specification declined to embed — an empty input under
`empty_policy: skip`. Coverage counts it as accounted for.

A row nothing ever embedded is a gap, and it is reported as one. The two are one
finding apart in the same layer, and Ptah keeps them distinct because a rollback
to a corpus that is exactly what was asked for should not be refused.

### The lease and the fencing token

A **lease** says who should be working. A **fencing token** says who may still
commit. They are different questions, and the second is the one that matters
after a worker was paused long enough for its lease to lapse and then resumed: it
still believes it holds the run, and the token is what stops it.

A worker whose token is behind the run's is refused before it touches your
table. The token moves when a worker **takes** the run: `prepare` takes it, and
so does every verb that does work. So a second `backfill` started against a run
another process is already embedding fences the first at its next commit, rather
than both writing.

## Reading `verify`

```console
$ ptah inference verify --spec spec.yaml --db-url "$DB" --run-id 2026-08-articles
generation 31122cc8...: 48231 source rows, 48200 target rows
  - [freshness/blocking] 12 target rows were computed from a source state that has since changed
      keys: 4471, 4472, 4480, ...
  - [structural/advisory] the index uses ivfflat and the generation expects hnsw
  - not measured: the stored vectors were not read back, so their dimension was
    checked and their values were not
error: verification found 1 blocking findings
```

Each finding names its **layer** and its **severity**.

### The five layers

| Layer | Asks | Typical finding |
| --- | --- | --- |
| `structural` | Does the column exist with the right type and dimension? Is the index there and valid? | `the generation's vector column does not exist` |
| `coverage` | Does every in-scope row have a vector, a skip, or a tombstone, and does anything else carry one? | rows with no vector and not marked skipped or deleted; `N target rows are outside the generation's source scope` |
| `freshness` | Was each vector computed from the source as it is now? | rows computed from a source state that has since changed |
| `vector_validity` | Are the stored vectors the shape the generation declares? | `the column holds N dimensions and the generation expects M` |
| `consistency` | Has the backfill finished, has catch-up reached the barrier, is a lease still held? | `catch-up has not reached the barrier, so changes after the snapshot are unprocessed` |

### Severity

**Blocking** refuses the cutover and exits non-zero. **Advisory** is reported and
does not.

Accepting a blocking finding takes both halves. The specification says whether
accepting is permitted at all:

```yaml
policy:
  allow_accepted_findings: true
```

and the cutover names which findings, by their exact summary:

```bash
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id r1 \
  --accept-finding "3 in-scope source rows have no vector in this generation"
```

The specification is where the permission lives because it is reviewed; the
summary cannot be, because it carries counts and keys that only exist once a
report has run.

Three refusals follow from that, and each is deliberate. **Every** blocking
finding has to be named — accepting one does not carry the others. A summary
matching no blocking finding is refused rather than ignored, because an
acceptance copied into a runbook outlives the finding it was written for. And
the consistency decision is not reachable this way at all: accepting "changes
after the snapshot are unprocessed" would be accepting a cutover onto a
generation nobody claims covers the source.

What was accepted, and what was left blocking, are both in the plan digest. So
an approval given for a plan that accepted one finding does not authorize a plan
that accepted another.

### What was not measured

`verify` reports what it did not check, not only what it did. A report saying
only what it checked reads as though it checked everything.

An unmeasured check is not a blocker. It is a sentence, and the difference
matters: the question six months later is exactly which of these numbers was
measured.

## Reading `evaluate`

```console
generation b115a08fbd46e92857378a5eea6855091e76d428e522c6c180134541a1e1af4c against corpus 479e9ad227ce
  - recall 1.000, MRR 1.000, NDCG 1.000 over 3 cases
  - the index agrees with an exhaustive search on 1.000 of results, over 3 cases
  - measured under hnsw.ef_search=40,ivfflat.probes=8
  - not measured: retrieval quality was not compared against the generation being replaced, because no baseline was measured for it
```

That corpus holds three cases and every score is a 1.000, which is what a
small corpus over a small table looks like. The shape is what to read.

| In the report | Means |
| --- | --- |
| `recall` | The share of expected answers found, at the depth this case looked to |
| `MRR` | Mean reciprocal rank: how high the first correct answer came |
| `NDCG` | How well the whole ordering matches the expected one |
| `the index agrees with an exhaustive search` | How often the index returned what a scan of every row would have |

The depth is the case's own `k` where it names one, otherwise the corpus's
`default_k`, otherwise `--k`. The corpus wins over the flag on purpose: a run at
a different depth is a different measurement, and the file is where that is
written down.

The agreement figure measures the **index**, not the model. A low number means
the index or its query parameters need attention, not that the embeddings are
wrong.

`measured under` names the query-time settings every number above it was taken
at, because a recall figure without them is not comparable to any other. A
setting the session does not carry is reported as `(absent)` rather than filled
in with a default Ptah did not read.

## Where the records go

`verify --publish-evidence` and `cutover --publish-evidence` write these to an
OCI registry. The verification record carries the findings whole rather than a
verdict, and what was not measured; the cutover record carries the plan digest
the approval bound to, the approver, and the verification it cited.

A verification is attached to its release as a referrer rather than tagged,
because evidence accumulates: a generation gets one release and several
verifications, and finding them by remembering a tag each is how a record goes
missing.
