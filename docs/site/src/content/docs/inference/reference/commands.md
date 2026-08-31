---
title: Command reference
description: Every ptah inference verb, what it does, and the flags it takes.
type: reference
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "Which inference commands and flags does Ptah expose?"
goal: "Look up the available inference commands and flags."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
owns:
  - cli-ptah-inference-describe
  - cli-ptah-inference-plan
  - cli-ptah-inference-prepare
  - cli-ptah-inference-backfill
  - cli-ptah-inference-catchup
  - cli-ptah-inference-index
  - cli-ptah-inference-verify
  - cli-ptah-inference-evaluate
  - cli-ptah-inference-pause
  - cli-ptah-inference-resume
  - cli-ptah-inference-status
  - cli-ptah-inference-cutover
  - cli-ptah-inference-rollback
  - cli-ptah-inference-retire
---

Fifteen verbs. Each is a decision taken separately: none of them is implied by
another.

Every verb takes `--spec` or `--release`, and most take `--db-url` and
`--run-id` — an identifier you choose, and how a resumed run finds its
checkpoint.

`--release` names a published release instead of a file. The release carries the
specification it was built from, so an environment that has never seen the file
runs the same document; what a mutable reference resolved to is printed on
standard error. An `oci-layout://` directory is accepted on the same flag, which
is what an air-gapped environment has instead of a registry.

## `describe`

Reads a specification and reports what it says on its own. **The only verb that
opens no connection**, which is what makes it usable where every other one
cannot be: writing a specification, and asking in CI whether an edit changed the
corpus.

| Flag | Meaning |
| --- | --- |
| `--format` | `text` or `json` |

It reports the generation identity, whether that generation can be rebuilt and
why not, what running it would send out of the database, what the consistency
mode can establish, and the objects a generation would write.

Nothing here is measured. The row count is **absent rather than zero**, because
counting needs the database and an uncounted source rendered as zero says the
disclosure is empty.

```console
$ ptah inference describe --spec spec.yaml
articles: generation 31122cc8322d44317514ca2b54f29853f1c43d19ecd3b2a1b183320ef8f5bb37
  - reproducibility: partial (provider "openai-compatible" exposes no immutable
    revision for model "text-embedding-3-small", so asking it again may answer
    with different vectors)
  - target: articles.embedding_v2 vector(1536)
  - index: articles_embedding_v2_31122cc8322d_idx using hnsw
```

The JSON form is what a CI job diffs:

```bash
ptah inference describe --spec spec.yaml --format json |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["generation"])'
```

Two digests that differ mean the edit changed the corpus, and every vector will
have to be computed again.

## `probe`

Asks the embedding provider what it answers, and sends nothing from your
database. **The second verb that opens no connection**, so it runs in CI beside
`describe`.

| Flag | Meaning |
| --- | --- |
| `--format` | `text` or `json` |
| `--provider-timeout` | How long one provider request may take |

Every fact a plan states about a provider is configured rather than measured:
the model identifier and the output dimension are what somebody typed. Until
this verb, the first thing that checked them was the backfill — which had
already sent source rows to the endpoint by the time it found the width was
wrong.

What it establishes: the endpoint answers, it accepts the credential the
specification points at, the model answers an embedding request, one input comes
back as one usable vector of finite values, the width is the one the
specification declares, a batch is answered for every input, a canceled request
stops, and a refusal arrives as an error the engine can act on.

```console
$ ptah inference probe --spec spec.yaml
text-embedding-3-small at api.openai.com, declared hosted
  - ok   reachable: the endpoint at api.openai.com answered
  - ok   authorized: the credential from env:OPENAI_API_KEY was accepted
  - ok   embeds: model text-embedding-3-small answered an embedding request
  - ok   shape: one vector of 1536 finite values for one input
  - ok   dimension: 1536 dimensions, as declared
  - ok   batch: 2 inputs answered with 2 vectors
  - ok   cancellation: a canceled request stopped rather than answering
  - ok   error shape: a refused request arrived as a classified error the engine can act on
```

It returns 1 when a check fails, so a pipeline can gate on it. Two fixed strings
go out and no vector comes back into the report, which is what lets it be run —
and its output pasted into an issue — before anybody has decided to send a
corpus anywhere.

What it cannot establish is whether the provider retains what you send it. That
is outside Ptah's knowledge and nothing here claims otherwise.

## `plan`

Reports what a generation change would do, and where each answer came from.
Reads the database; creates nothing in it.

| Flag | Meaning |
| --- | --- |
| `--current` | Identity of the generation queries read now, when there is one |
| `--publish-evidence` | OCI reference to publish the release record to |
| `--evidence-file` | Path to write the release record to as JSON |
| `--plain-http` | Allow an unencrypted connection to a trusted local registry |

This is where a generation change is put on the record. Naming a destination
leaves a **release**: what the change proposes — the generation, the document
that proposed it, what it replaces, and whether it can be rebuilt — addressed by
its own digest. A verification published later attaches to it with `--attach-to`,
which is how several verifications of one generation are found without
remembering a tag for each.

Naming no destination leaves nothing behind, which is what an operator asking a
question of a specification wants.

Every fact is labeled with its provenance — `measured`, `configured`,
`inferred`, `unknown`, or `unsupported`. A fact labeled `unknown` is telling you
a decision rests on something nobody checked.

The output ends with what would leave the database: the endpoint, the model, the
columns whose text is sent, and the row count.

## `prepare`

Creates the vector column and the four bookkeeping columns beside it, installs
the change-capture mechanism, records the snapshot boundary, and creates the run.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier for this run (required) |
| `--worker` | Name recorded as the lease holder |

Idempotent. It does not install pgvector — `CREATE EXTENSION` is a database-wide
privileged act, and the refusal names the statement to run.

It refuses when the column you named already holds another generation.

## `backfill`

Embeds the source as it was at the boundary. Resumable: run it again after an
interruption and it continues from its checkpoint.

| Flag | Meaning |
| --- | --- |
| `--batch-rows` | Source rows read in one query, which bounds how long a cancellation waits |
| `--batch-inputs` | Inputs sent to the provider in one request |
| `--provider-timeout` | How long one provider request may take |

## `catchup`

Processes the source changes made since the boundary. Run it until it reports
nothing left.

Takes the flags `backfill` takes, and `--maintain-for` besides. Refused against
a consistency mode that records nothing, rather than reported as success.

| Flag | Meaning |
| --- | --- |
| `--maintain-for` | After catching up, extend this generation's stabilization window by this much |

`--maintain-for` is what keeps a previous generation a way back during its
stabilization window: it catches the generation up and extends the promise that
it is current, in one command. That pairing is why `backfill` does not take it:
a window is kept true by the catch-up that moves it, so a backfill carrying the
flag would extend a promise with nothing behind it.

## `index`

Builds the vector index the specification declares, concurrently, and leaves it
valid. An invalid index left by a failed build is dropped and built again.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier of the run (required) |

A specification declaring no `index_method` has nothing to build, and this says
so rather than failing. The run still reaches its `indexed` phase: the phase
names the step, and a run left short of it could never be verified.

## `verify`

Runs the deterministic checks a cutover rests on, across five layers, and reports
what it did not measure.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier of the run (required) |
| `--publish-evidence` | OCI reference to publish this run's record to |
| `--attach-to` | OCI reference of the release this record is about |
| `--evidence-file` | Path to write this run's record to as JSON |
| `--plain-http` | Allow an unencrypted connection to a trusted local registry |

`--attach-to` publishes the record into the release's own repository, as a
referrer of it. That is where a referrer lands, so a run naming
`--publish-evidence` as well is refused: it would have said where the record went
twice. A verification with no release to attach to is still publishable, and is
addressed by its own digest.

Exits non-zero when any finding is blocking. The record is kept either way — a
verification that found something is the evidence somebody will want.

The two destinations are independent: name both and the record is written and
pushed, name neither and it is neither. A failure to keep it is reported and does
not fail the verb, because the measurement already happened.

## `evaluate`

Measures what the generation actually retrieves, against a corpus of questions
and expected answers that you write.

| Flag | Meaning |
| --- | --- |
| `--corpus` | Path to the evaluation corpus (required) |
| `--baseline` | Identity of the generation to compare against; needs `--baseline-spec` |
| `--baseline-spec` | That generation's own specification, which is what measures it |
| `--k` | How deep to look when neither the case nor the corpus says |
| `--min-recall` | Refuse below this recall; zero gates nothing and reports the number |
| `--max-mrr-regression` | Refuse when MRR falls further than this below the baseline |
| `--max-ndcg-regression` | Refuse when NDCG falls further than this below the baseline |
| `--min-exact-agreement` | Refuse when the index agrees with an exhaustive search less than this |
| `--require-every-case` | Refuse when any case produced no result |
| `--provider-timeout` | How long one provider request may take |

The numbers carry the query parameters they were taken under, because recall at
one `ivfflat.probes` setting is not comparable to recall at another.

## `pause`

Stops a run at the boundary its last checkpoint reached.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier of the run (required) |
| `--reason` | Why the run is being stopped (required) |
| `--worker` | Name recorded as the lease holder |

Nothing is lost and nothing is undone. The work that is committed stays
committed and the position the run reached stays recorded; `resume` picks it up
from there.

A pause **takes the run**, which moves the fencing token past the worker that
held it. That is what makes it take effect rather than take note: without the
claim, the pause lands in a row the running worker overwrites at its next
checkpoint, and the run reads paused for a few seconds while the provider bill
goes on.

So a backfill that was running will fail at its next commit, saying the run has a
newer fencing token. That is the pause working.

The reason is required, and `status` prints it. A paused run whose reason is
empty is one nobody can act on.

## `resume`

Returns a paused run to running and clears the reason it stopped for.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier of the run (required) |
| `--worker` | Name recorded as the lease holder |

Nothing starts working here. This changes what the run is, not what is happening
to it: `backfill` or `catchup` takes the run in turn and continues from the
checkpoint.

It claims for the same reason `pause` does, and for one more — the worker the
pause fenced is not necessarily gone, and a resume that left the token where the
pause put it would return the run to running with that worker still able to
commit into it.

Only a paused run resumes. Anything else is refused by name.

## `status`

Reports what a run has done, how far it got, and what it is waiting for: the
phase, the progress counts, the watermarks, the lease and its fencing token, and
why it stopped if it did — the reason for a pause, the class and detail for a
failure.

| Flag | Meaning |
| --- | --- |
| `--format` | `text` or `json` |
| `--require-ready` | Return 1 unless the generation is verified and ready to cut over |

Two of its answers are measured rather than read off the run. `verified` runs
the deterministic layers now, and `cutover ready` decides with the same code the
cutover verb decides with. Both cost what `verify` costs, which is a read of the
target.

Cutover readiness excludes the approval, which is reported separately with the
plan digest to approve. An approval nobody has given yet is not a defect in the
state, and a rollout gate waiting for one would wait forever under the policy
most production environments run.

`--require-ready` is the gate: exit 1 until both conditions hold, exit 0 when
they do. See [Run in Kubernetes](../../guides/run-in-kubernetes/).

## `cutover`

Makes the new generation the one queries read.

| Flag | Meaning |
| --- | --- |
| `--approve` | Plan digest this cutover is approved for; run without it to see the digest |
| `--approver` | Who approved it |
| `--plan-file` | Path to write the refused plan to, so it can be signed |
| `--approval` | Path to a plan file signed with `ptah schema approve` |
| `--allowed-signers` | OpenSSH allowed_signers file listing approvers |
| `--signer` | Require the approval to belong to this principal |
| `--stabilize-for` | How long the previous generation stays a way back; zero leaves no rollback |
| `--publish-evidence` | OCI reference to publish this run's record to |
| `--attach-to` | OCI reference of the release this record is about |
| `--evidence-file` | Path to write this run's record to as JSON |
| `--plain-http` | Allow an unencrypted connection to a trusted local registry |

The approval binds to the plan digest. What is true now — the pointer, the
freshness, the findings — is checked again at the moment of the cutover.

`--approve` records the digest and `--approver` the name to put beside it. Where
who approved something has to be evidence rather than a claim:

```bash
# Refused, and the plan is written where somebody can read it.
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --plan-file cutover.plan

# Signed with the mechanism the rest of Ptah already uses.
ptah schema approve --plan cutover.plan --key ~/.ssh/id_ed25519

# The approver is the principal the signature verifies as.
ptah inference cutover --spec spec.yaml --db-url "$DB" --run-id "$RUN" \
  --approval cutover.plan
```

The file names the operation, the generation, what it replaces and the target,
so the signature covers something an approver could read — a signature over
sixty-four hex characters attests to a number nobody could have checked. Both
halves are required: the signature says an allowed key covered these bytes, and
the digest inside them says the bytes are about this plan.

`policy.require_signed_approval: true` refuses the typed form.

## `rollback`

Puts the previous generation back, while it is still a place to go back to.

| Flag | Meaning |
| --- | --- |
| `--to` | Identity of the generation to return to (required) |
| `--publish-evidence` | OCI reference to publish the rollback record to |
| `--attach-to` | OCI reference of the release this record is about |
| `--evidence-file` | Path to write the rollback record to as JSON |
| `--window` | How long after a cutover the previous generation stays eligible; zero for no limit |

Measures the generation before moving anything: present, maintained, complete,
fresh, indexed. A generation that drifted is refused.

## `retire`

Destroys a generation. This cannot be undone.

| Flag | Meaning |
| --- | --- |
| `--generation` | Identity of the generation to destroy (required) |
| `--drop-column` | Drop the vector column as well as the index |
| `--approve` | Plan digest this retirement is approved for |
| `--approver` | Who approved it |
| `--plan-file` | Path to write the refused plan to, so it can be signed |
| `--approval` | Path to a plan file signed with `ptah schema approve` |
| `--allowed-signers` | OpenSSH allowed_signers file listing approvers |
| `--signer` | Require the approval to belong to this principal |
| `--publish-evidence` | OCI reference to publish the retirement record to |
| `--attach-to` | OCI reference of the release this record is about |
| `--evidence-file` | Path to write the retirement record to as JSON |

Refused while queries still read the generation.

The approval binds to what is **destroyed** rather than to what is named:
approving the removal of an index does not authorize the removal of the column,
and the plan file says which, along with how many rows go with it. For an
operation nothing can undo, `--approval` is worth the extra step.

## Environment variables

Most flags also read a `PTAH_`-prefixed environment variable, printed as
`[env: PTAH_...]` on the flag's `--help` line. A flag without that marker has no
environment binding. Check `--help` rather than assuming.
