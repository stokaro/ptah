---
title: Production rollout
description: Ordering the steps, the evidence to keep, and the decisions to take before rather than during.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I order and verify a production inference migration?"
goal: "Order and verify a production inference migration."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

A production generation change is the same commands in the same order. What
differs is what you decide beforehand and what you keep afterwards.

## Decide before you start

**The window.** How long do you want a rollback to be possible? That number goes
in `--stabilize-for`, and it commits you to catching up the previous generation
for that long.

**Who approves.** `policy.require_exact_approval: true` means a cutover needs a
digest-bound approval. `--approver` is recorded on the run and in the published
evidence.

**The evaluation gate.** What retrieval quality would make you stop? Put it in
the command rather than in a judgment call at 2am:

```bash
ptah inference evaluate --spec spec-v2.yaml --db-url "$DB" \
  --corpus corpus.yaml --baseline <previous-generation> \
  --max-ndcg-regression 0.02 --min-recall 0.9 --require-every-case
```

**When the application changes.** Cutover and deploy are two steps and you order
them. See
[Migrate to another model](../../guides/migrate-to-another-model/#then-change-your-application).

## A rollout that has worked

```bash
# 0. Put the proposal on the record, before anything is built.
ptah inference plan     --spec spec-v2.yaml --db-url "$DB" \
  --publish-evidence oci://registry.example.com/search-evidence:release

# 1. Build, on a schedule that fits the provider budget.
ptah inference prepare  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference backfill --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN" \
  --batch-inputs 64 --batch-rows 500
ptah inference catchup  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"
ptah inference index    --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"

# 2. Measure. Both the deterministic checks and the retrieval quality.
ptah inference verify   --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN" \
  --attach-to oci://registry.example.com/search-evidence:release
ptah inference evaluate --spec spec-v2.yaml --db-url "$DB" \
  --corpus corpus.yaml --baseline <previous> --max-ndcg-regression 0.02

# 3. Drain the last changes immediately before cutting over.
ptah inference catchup  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN"

# 4. Cut over, with a window.
ptah inference cutover  --spec spec-v2.yaml --db-url "$DB" --run-id "$RUN" \
  --approve <digest> --approver "your name" --stabilize-for 24h \
  --attach-to oci://registry.example.com/search-evidence:release
```

Step 3 is the one that gets skipped. A verification from four hours ago passed
against a source that has moved since; running catch-up immediately before the
cutover is what keeps the gap small.

## Keep the previous generation alive

For the length of the window, on a schedule:

```bash
ptah inference catchup --spec spec-v1.yaml --db-url "$DB" \
  --run-id "$PREVIOUS_RUN" --maintain-for 1h
```

Every hour, for 24 hours. Stop, and the window elapses over a generation that
drifted — `rollback` will refuse it, correctly.

## Keep the evidence

`--publish-evidence` writes a record to an OCI registry, where the rest of Ptah's
evidence already lives:

- the **release** record, from `plan`, carries what the change proposes: the
  generation, the digest of the document that proposed it, what it replaces, and
  whether it can be rebuilt;
- the **verification** record carries the findings whole, not a verdict, plus
  what the run did not measure;
- the **cutover** record carries the plan digest the approval bound to, the
  approver, the verification it cited, and the watermark the source had been
  accounted for up to — the generation identity says how a vector was computed,
  and that says which source state was.

`--attach-to` names the release a verification or a cutover is about, and
publishes the record into that release's repository as a referrer of it. Step 0
above is what the two later steps attach to. Attached rather than tagged, because
evidence accumulates: a generation gets one release and several verifications,
and finding them by remembering a tag for each is how a record goes missing.

There is no step 0 in some pipelines, and that is allowed. A verification with no
release to attach to is published on its own, addressed by its own digest — it is
the record somebody wants most, and requiring a subject would have taken it away
from every operator without a registry at plan time.

Six months later the question is not whether it passed — the pointer answers
that — but what it said. A record holding one boolean cannot be re-read into an
answer.

A failure to publish is reported and does not fail the verb. The measurement or
the pointer move already happened, and a registry nobody can reach is not a fact
about the generation.

Without a registry, `--evidence-file <path>` writes the same record as JSON. The
bytes are identical, so what you keep locally is what you would have fetched —
which is the destination for a first migration, for a CI job that runs before
anything is published, and for a team with no registry at all.

## Promote one release through environments

`plan --publish-evidence` writes the release, and the release carries the
specification it was built from. Every verb takes `--release` in place of
`--spec`, so development, staging and production run one document rather than
three copies of it:

```bash
# Once, from the machine that holds the file.
ptah inference plan --spec spec-v2.yaml --db-url "$DEV" \
  --publish-evidence oci://registry.example.com/search-embeddings:release

# In each environment afterwards. No file, and no ConfigMap to keep in step.
ptah inference plan     --release "$RELEASE" --db-url "$STAGING"
ptah inference prepare  --release "$RELEASE" --db-url "$STAGING" --run-id "$RUN"
```

Address it by digest where it matters. A tag works, and what it resolved to is
printed on standard error — a promotion whose record kept only the tag says two
environments agreed without establishing that they did.

Each environment produces its own run, verification and cutover records against
the same release identity, and `ptah inference describe --release "$RELEASE"`
reads the whole specification back without a database.

An air-gapped environment takes the same flag over a directory:
`ptah oci copy oci://... oci-layout://./release` on one side of the gap, carry
the directory across, and `--release oci-layout://./release` on the other.

## Gate a deployment on the state

`ptah inference status --require-ready` exits 1 until the generation is verified
and ready to cut over, and 0 when it is. That is the whole of a rollout gate: a
CI step or an init container that keeps failing until the corpus is there.

It does not wait for the approval, which is reported separately with the digest
to approve — a gate that waited for a signature given in the same breath as the
cutover would never open. See
[Run in Kubernetes](../../guides/run-in-kubernetes/).

## What to watch after the cutover

- **Search latency.** A new index with different parameters behaves differently
  under load than it did in your evaluation.
- **Result quality complaints.** The evaluation measured the questions you wrote;
  your users ask different ones.
- **The maintenance job.** If it stops, your rollback stops being possible, and
  nothing will tell you until you try.

## Retiring

Not on the day. See
[Rollback and retire](../../guides/rollback-and-retire/#when-to-retire) — the
common shape is to stop maintaining after the window and retire a week later,
once nobody has asked to go back.
