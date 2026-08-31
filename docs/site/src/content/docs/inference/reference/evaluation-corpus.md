---
title: Evaluation corpus reference
description: Every field of an evaluation corpus file, what each one measures, and which numbers it decides.
type: reference
audience:
  - "application-developer"
  - "database-engineer"
readerQuestion: "What goes in the file `ptah inference evaluate --corpus` reads?"
goal: "Look up the accepted evaluation corpus fields and what each one measures."
sourceOfTruth:
  - "internal/embedcorpus"
  - "internal/embedeval"
  - "integration/inference_evaluate_e2e_test.go"
generated: false
overlaps: []
disposition: keep
---

An evaluation corpus is a YAML file of questions and the answers a good
generation returns for them. `ptah inference evaluate --corpus` reads it,
embeds each query with the generation under test, searches, and reports how
well the results matched.

It is the only thing that answers "is this generation actually working".
Verification measures coverage and freshness — every row has a vector, and that
vector was computed from the text the row holds now — and neither of those is a
statement about whether search results are good.

## Top level

| Field | Meaning |
| --- | --- |
| `version` | The format version. Must be `1`; anything else is refused. |
| `name` | A display name, for a report a person reads. Outside the digest. |
| `description` | Optional prose, for the same reader. Outside the digest. |
| `default_k` | How deep a case looks when it does not say. |
| `cases` | The queries. At least one. |

## `cases[]`

| Field | Meaning |
| --- | --- |
| `id` | Names the case in the report. Required. |
| `query` | The text embedded and searched with. Required. |
| `k` | How deep this case looks. Zero or absent uses `default_k`. |
| `required` | Source keys that **must** appear in the top `k`. |
| `relevant` | A map from source key to graded relevance, for the ranked measures. |

A key is the source row's key as a string. Where the specification declares a
composite key, the components are joined in the order `key_fields` names them.

A case must declare `required`, `relevant`, or both. One that declares neither
is refused: any answer satisfies it, including none, and it lifts the mean of
every measure it is averaged into.

A grade must be greater than zero. Zero is what an unlisted key already means,
so a listed zero reads as "this one matters" and scores as "this one does not".

## What each field decides

`required` is a hard expectation and `relevant` is a score, and they answer
different questions. A corpus that only scored would pass a generation that
ranks the one document the question is about eleventh; a corpus that only
required would say nothing about the order of the ten above it.

Three numbers come out, all averaged over the cases:

| Number | What it measures |
| --- | --- |
| `recall` | The share of a case's relevant keys that appear in its top `k`. |
| `MRR` | One over the position of the first relevant key. |
| `NDCG` | The graded ranking, discounted by position. |

All three read `relevant`. A case that declares `required` and no `relevant` is
read as grading each required key `1`, because a case naming a right answer is
naming a right answer. Declaring `relevant` explicitly overrides that, which is
how an author says one right answer is better than another.

That derivation matters more than it looks. A case contributing no grades
contributes to no ranked measure at all — which is right for a case that
expects nothing, and wrong for one that named its answer under `required`. Were
such a case ungraded, a corpus made entirely of them would score nothing, and
the mean of nothing is zero: `recall 0.000` for a generation answering every
query perfectly, and a `--max-recall-drop` gate comparing against it.

## A complete example

```yaml
version: 1
name: docs questions
description: what support actually gets asked
default_k: 5
cases:
  - id: pricing
    query: "How much does it cost per month?"
    required: ["42"]
    relevant:
      "42": 1.0
      "43": 0.5
  - id: cancellation
    query: "How do I cancel?"
    k: 3
    required: ["17"]
```

## The digest

A corpus has a content address, and `evaluate` reports it beside the numbers.
It covers the queries, their depths and their expectations — everything a
number depends on — and not `name` or `description`, for the same reason a
generation's display name is outside its identity: renaming a corpus does not
make its measurements incomparable.

Two evaluations are comparable when their corpus digests match. That is what
`--baseline` rests on.

## Writing one

The keys have to be keys the generation actually holds, so write the corpus
against the source table rather than from memory. A query whose answer is a row
your filter excludes measures the filter, not the model.

Ten to thirty cases is usually enough to see a regression. What matters more
than the count is that they are questions somebody asked: a corpus of questions
you invented measures how well the model answers questions you invented.
