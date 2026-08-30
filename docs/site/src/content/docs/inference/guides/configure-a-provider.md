---
title: Configure an embedding provider
description: Naming an endpoint, pointing at a credential without holding one, and measuring the whole of it before a source row leaves.
type: how-to
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How do I configure and check the embedding endpoint a generation uses?"
goal: "Configure an embedding endpoint and check it before running a migration."
sourceOfTruth:
  - "internal/embedprovider"
  - "cmd/inference/probe.go"
generated: false
overlaps: []
disposition: keep
owns:
  - cli-ptah-inference-probe
---

Four lines of a specification decide where your corpus goes. This page is what
each of them means, and how to find out whether they are right before anything
is sent.

## The four lines

```yaml
model:
  provider: openai-compatible
  endpoint_class: local           # local | hosted | gateway
  endpoint: http://localhost:11434/v1
  identifier: bge-small-en
  revision: "sha256:..."          # where the provider exposes one
  credential: env:PTAH_EMBED_TOKEN
  reported_dimension: 384
  normalization: none
```

`provider` names the adapter, and there is one: `openai-compatible`, meaning a
`POST /v1/embeddings` that answers in the OpenAI shape. An endpoint that speaks
something else needs a gateway in front of it.

`endpoint_class` is **your statement**, not a measurement. Ptah cannot tell from
an address who operates it. The class is carried into the plan and into the
generation identity, so moving a corpus from a local endpoint to a hosted one is
a different generation rather than a quiet change of who has your text.

`identifier` and `revision` are the model. Where the provider exposes an
immutable revision, name it: without one the generation is reported as
**partially reproducible**, with the reason, because asking the same provider
again may answer with different vectors and Ptah cannot see that it did.

## A local endpoint is a first-class case

Nothing here needs a hosted provider, a Ptah account, or an outbound connection.
An endpoint on `localhost` with no `credential` is a complete configuration, and
it is the one to reach for when the text must not leave the machine.

Leave `credential` out entirely in that case. An empty reference means no
`Authorization` header rather than an empty one.

## The credential is a reference

```yaml
credential: env:PTAH_EMBED_TOKEN     # an environment variable
credential: file:/run/secrets/token  # a file, read at the moment of use
```

Two schemes: `env` and `file`. Either names *where* the value is, and the value
is read at the moment of each request and not kept. What Ptah records — in the
plan, in the run's own tables, in published evidence, in what an agent can see —
is the reference.

That is what makes a specification a file you can commit.

A `file:` reference is refused when the filesystem lets anyone but the owner read
it, where the platform can answer that question. A token in a world-readable
file is a token every process on the host has.

## Measure it before you send anything

```bash
ptah inference probe --spec spec.yaml
```

```text
bge-small-en at localhost:11434, declared local
  - ok   reachable: the endpoint at localhost:11434 answered
  - ok   authorized: no credential was sent, and the endpoint asked for none
  - ok   embeds: model bge-small-en answered an embedding request
  - ok   shape: one vector of 384 finite values for one input
  - ok   dimension: 384 dimensions, as declared
  - ok   batch: 2 inputs answered with 2 vectors
  - ok   cancellation: a canceled request stopped rather than answering
  - ok   error shape: a refused request arrived as a classified error the engine can act on
```

Two fixed strings go out and nothing from your database does, so this is
runnable — and its output shareable — before anybody has decided to send a
corpus anywhere. It opens no database either, which is what lets a CI job run it
against a specification somebody is still writing.

It returns 1 when a check fails.

### What each answer is for

**`dimension`** is the one that used to be found late. Every fact a plan states
about a provider is configured rather than measured, so before this verb the
first thing that compared `reported_dimension` with what the endpoint answers was
the backfill — which had already sent rows by then.

**`cancellation`** decides what an interrupted backfill costs. A provider that
answers a canceled request turns your Ctrl-C into a wait, and the bill keeps
running.

**`error shape`** is what the engine acts on. Retry, stop, fail the batch: every
one of those decisions reads a classified error, and a refusal that arrives as
whatever the endpoint wrote is one the engine cannot act on correctly.

A check that could not run is named rather than reported as passing. An
unreachable endpoint reports the reachability check and says everything after it
was not measured.

## There is no fallback

If the endpoint you named fails, Ptah does not try another one. It could not do
so safely: a different provider is a different generation identity and a
different privacy boundary, and choosing one for you would send your text
somewhere you did not authorize while producing vectors that are not comparable
with the ones already stored.

A failed backfill stops and resumes from its checkpoint when the endpoint is back.

## What Ptah cannot tell you

Whether the provider retains what you send it. That is a contract question with
them, and nothing here claims an answer.

See [Security and data boundaries](../../concepts/security-and-data-boundaries/)
for what leaves the database, and
[Plan provider capacity](../../strategies/plan-provider-capacity/) for sizing the
run against rate limits and cost.
