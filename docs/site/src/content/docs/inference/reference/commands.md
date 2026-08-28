---
title: Command reference
description: Every ptah inference verb, what it does, and the flags it takes.
---

Eleven verbs. Each is a decision taken separately: none of them is implied by
another.

Every verb takes `--spec` and `--db-url`. Most take `--run-id`, which is an
identifier you choose and which is how a resumed run finds its checkpoint.

## `plan`

Reports what a generation change would do, and where each answer came from.
Reads the database; creates nothing and writes nothing.

| Flag | Meaning |
| --- | --- |
| `--current` | Identity of the generation queries read now, when there is one |

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
| `--maintain-for` | Extend this generation's stabilization window by this much |

## `catchup`

Processes the source changes made since the boundary. Run it until it reports
nothing left.

Takes the same flags as `backfill`. Refused against a consistency mode that
records nothing, rather than reported as success.

`--maintain-for` is what keeps a previous generation a way back during its
stabilization window: it catches the generation up and extends the promise that
it is current, in one command.

## `index`

Builds the vector index the specification declares, concurrently, and leaves it
valid. An invalid index left by a failed build is dropped and built again.

A specification declaring no `index_method` has nothing to build, and this says
so rather than failing.

## `verify`

Runs the deterministic checks a cutover rests on, across five layers, and reports
what it did not measure.

| Flag | Meaning |
| --- | --- |
| `--run-id` | Identifier of the run (required) |
| `--publish-evidence` | OCI reference to publish this run's record to |
| `--plain-http` | Allow an unencrypted connection to a trusted local registry |

Exits non-zero when any finding is blocking. The record is published either way —
a verification that found something is the evidence somebody will want.

## `evaluate`

Measures what the generation actually retrieves, against a corpus of questions
and expected answers that you write.

| Flag | Meaning |
| --- | --- |
| `--corpus` | Path to the evaluation corpus (required) |
| `--baseline` | Identity of the generation to compare against |
| `--k` | How deep to look when neither the case nor the corpus says |
| `--min-recall` | Refuse below this recall; zero gates nothing and reports the number |
| `--max-mrr-regression` | Refuse when MRR falls further than this below the baseline |
| `--max-ndcg-regression` | Refuse when NDCG falls further than this below the baseline |
| `--min-exact-agreement` | Refuse when the index agrees with an exhaustive search less than this |
| `--require-every-case` | Refuse when any case produced no result |
| `--provider-timeout` | How long one provider request may take |

The numbers carry the query parameters they were taken under, because recall at
one `ivfflat.probes` setting is not comparable to recall at another.

## `status`

Reports what a run has done, how far it got, and what it is waiting for: the
phase, the progress counts, the watermarks, the lease and its fencing token, and
the failure if there was one.

## `cutover`

Makes the new generation the one queries read.

| Flag | Meaning |
| --- | --- |
| `--approve` | Plan digest this cutover is approved for; run without it to see the digest |
| `--approver` | Who approved it |
| `--stabilize-for` | How long the previous generation stays a way back; zero leaves no rollback |
| `--publish-evidence` | OCI reference to publish this run's record to |
| `--plain-http` | Allow an unencrypted connection to a trusted local registry |

The approval binds to the plan digest. What is true now — the pointer, the
freshness, the findings — is checked again at the moment of the cutover.

## `rollback`

Puts the previous generation back, while it is still a place to go back to.

| Flag | Meaning |
| --- | --- |
| `--to` | Identity of the generation to return to (required) |
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

Refused while queries still read the generation.

## Environment variables

Most flags also read a `PTAH_`-prefixed environment variable, printed as
`[env: PTAH_...]` on the flag's `--help` line. A flag without that marker has no
environment binding. Check `--help` rather than assuming.
