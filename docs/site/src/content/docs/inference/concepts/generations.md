---
title: Generations
description: What a generation is, why its identity is computed rather than named, and why a new one needs its own column.
type: concept
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "How does Ptah model generations?"
goal: "Explain Ptah's model for generations."
sourceOfTruth:
  - "cmd/inference"
  - "integration/inference_cli_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

A **generation** is one complete set of vectors, produced one way, over one set
of source rows.

When you change the model, you are not editing a generation. You are building a
second one beside the first.

## The identity is computed, not chosen

You do not give a generation a name or a version number. Ptah computes its
identity from the specification: every field that could change what a vector
comes out as is hashed into one digest.

```console
$ ptah inference plan --spec spec.yaml --db-url "$DB"
generation 31122cc8322d44317514ca2b54f29853f1c43d19ecd3b2a1b183320ef8f5bb37
```

Two runs of the same specification address the same generation. Change the model
identifier, add a column to `input_fields`, switch the separator — any of those
produces a different digest, and Ptah treats it as a different corpus.

This is deliberate. A name is something a person keeps in step by hand, and it
goes wrong quietly: a specification edited without renaming produces vectors
that are not comparable to the ones already stored, under a name saying they
are. A computed identity cannot drift from what it identifies.

### What is deliberately excluded

Index build options are not part of the identity. `index_options` such as `m`
and `ef_construction` trade build cost against recall over *the same vectors*,
so changing them does not make a different corpus — it makes a different index
over one corpus.

## Reproducibility

`plan` reports whether the generation can be rebuilt, and says why not when it
cannot:

```console
  generation.reproducibility = partial (inferred: provider "openai-compatible" exposes no immutable revision for model "text-embedding-3-small", so asking it again may answer with different vectors)
```

It is reported either way. A specification pinning `model.revision` reads
`full`, and the answer sits among the facts the plan inferred rather than the
ones it measured, because Ptah reads the revision out of your specification
without asking the provider whether it honors it.

A provider that serves a model under a name and changes what it returns has
changed your corpus without changing the identity, and Ptah cannot see that. If
your provider exposes an immutable revision, put it in `model.revision` and the
answer becomes exact. If it does not, `plan` says so rather than implying a
reproducibility it cannot offer.

## A generation writes its own column

Two generations over one table need two columns. This is the rule that surprises
people most, so it is worth being direct about why.

If the new vectors overwrote the old ones row by row, then halfway through a
backfill your table would hold a mixture that is not searchable, and by the end
the previous corpus would be gone — with nothing to go back to. A cutover would
not be a switch; it would be the moment you noticed the data had already been
replaced.

So a specification whose model changed also needs its `target.column` changed:

```yaml
target:
  table: articles
  column: embedding_v2      # was: embedding
```

`prepare` refuses when the column you named already holds another generation, and
says which one. That refusal is early on purpose: the write path refuses the same
thing one row at a time, in the middle of a backfill, after the provider has
already been called and paid for.

## The bookkeeping beside the vector

`prepare` creates four columns next to the vector column:

| Column | Holds |
| --- | --- |
| `<column>_generation` | the generation identity this row's vector belongs to |
| `<column>_input_hash` | a digest of the exact text that was sent |
| `<column>_source_version` | the source version the vector was computed at |
| `<column>_state` | whether the row is a vector, a deliberate skip, or a tombstone |

They are written in the same statement as the vector. A row whose vector landed
and whose input hash did not would be a row that verification calls fresh
forever.

You do not have to read these columns. Verification does, and so does the
freshness check a rollback rests on.

## The pointer

Ptah records which generation queries should read, per target table. `cutover`
moves that pointer; `rollback` moves it back.

Moving the pointer changes nothing about your application on its own. Your
queries name a column, and it is up to you to read the active one — see
[Verification and cutover](../verification-and-cutover/#what-your-application-has-to-do).
