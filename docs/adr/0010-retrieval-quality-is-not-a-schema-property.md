# ADR 0010: Retrieval quality is not a property of the schema, and what follows for verification

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2068](https://github.com/stokaro/ptah/issues/2068)
- Records the decisions the research phase measured its way up to and declined to make

## 1. Context

The research phase of #2068 is complete: four measurements, taken against
pgvector 0.8.6 on PostgreSQL 17, and its last output states that what remains is
decision-making rather than measurement. This record is that decision's
paperwork — the options and what each costs — written so the choice is made
once and in the open.

It decides nothing. Section 5 states four questions as questions.

## 2. The measurement that decides the shape

On a single unchanged index, recall@10 spans **26.5% to 100%** purely as a
function of `ivfflat.probes`, a session-level setting Ptah can never own.

That is the whole argument. Retrieval quality is a property of five things —
the corpus, the embedding model, the index parameters, the query parameters, and
the query itself — of which a schema tool owns two. A number Ptah measures is a
reproduction of one caller's conditions, not a property of the database it
described.

Two supporting measurements bound it further:

- Recall falls monotonically as `lists` rises at a fixed `probes`, so the pair
  is not separable: an index tuned for one `probes` value is mistuned for
  another.
- An `ivfflat` build fails outright rather than degrading when memory is short,
  with the requirement scaling as `lists × dimension` — the two quantities an
  embedding migration changes.

And one methodological control that any future corpus must reproduce: an
uncorrelated generator produced 50 000 identical vectors, and every recall
number on that corpus read 100.0% at every setting. A corpus that cannot
distinguish settings reports success for a broken index.

## 3. What Ptah owns today

Corrections first, because two things the research said are no longer true:

- **Index tuning is carried now.** #2183 closed via #2251: `m`,
  `ef_construction` and `lists` round-trip, owned by `internal/pgindexstorage`.
  It is gated behind an environment switch and defaults off, and the gating is
  not cosmetic — reading storage parameters without it plans a DROP/CREATE on
  every apply, measured with pgvector as three indexes and three DROP/CREATE
  pairs.
- **The Assist constraint is Decision 11, not Decision 12.** Decision 12 is the
  no-arbitrary-access rule. Decision 11 is the one that keeps source content out
  of the Assist conversational model, and it carries the carve-out this epic
  depends on: source rows may reach a selected embedding endpoint, but not
  Assist prompts or MCP resources. An ADR that cites the wrong one gets the
  wrong exception.

What exists in the tree for vectors is **generic, not vector-specific**. There is
no vector code in the shipped packages: `vector(384)` survives because the
PostgreSQL reader keys on `atttypmod` and keeps any extension type's modifier as
an opaque string. Nothing parses a dimension and nothing knows 2000 is a ceiling.
The access method is a free string, the distance metric is the per-key operator
class, and `DBIndex.RequiresExtensions` already records that an HNSW index
depends on pgvector.

Nothing exists for the inference half: no package for embeddings, generations,
backfill, run state or corpora.

## 4. What this means for verification

The research's own split, and this record adopts it as the frame rather than as
a decision:

**Completeness is Ptah-shaped.** Every row that should have an embedding has
one, of the right generation. It is countable, it is a property of the database,
and it is what actually gates a cutover.

**Retrieval quality is not.** It is a reproduction of one caller's conditions,
and §2 is why.

So the question is not "how good is the retrieval" but "what does Ptah do with a
number it cannot own".

## 5. The decisions

### 5.1 Is quality evaluation a gate or evidence?

The measurement argues against any gate whose threshold omits a `probes` value:
the same index passes and fails depending on a setting the caller chooses at
query time. A gate stated without it is a coin flip with a number on it.

Options: no quality gate at all and completeness gates the cutover; a gate that
requires the query parameters to be declared alongside the threshold; or
evidence recorded and never gating.

### 5.2 If evidence, what is recorded beside the number?

The epic already answers this in draft — corpus identity, and the verification
report's contents. §2 adds one requirement that draft did not have: **the query
parameters**, without which two numbers are not comparable.

### 5.3 What does an evaluation compare against?

The epic already names both, so this is a choice between two things rather than
a design:

- **Exact search on the same corpus** answers "how much did the index lose",
  which is a property of the index.
- **The previous generation's answers** answers "did the migration change what
  users see", which is a property of the change.

They diverge when the old generation was itself poor: overlap can be low because
the new one is better. Choosing one is choosing which question the report
answers; choosing both means saying which one gates.

### 5.4 Whose corpus, and whose queries?

A trust-boundary question rather than a detail, and Decisions 11 and 12 bound it.
Together they rule out three concrete designs: Ptah discovering a corpus by
querying arbitrary source tables; returning corpus text or expected documents
through MCP or Assist; and any LLM-judged relevance.

There is a tension to resolve rather than inherit. The research's last comment
says "an evaluation corpus is source content", while the epic's own terminology
defines the corpus as user-supplied queries and relevance expectations, with
expectations as document identifiers. Those are different objects with different
boundaries: identifiers are not content, and a corpus supplied by the operator is
not discovered by Ptah. The ADR that answers this should say which definition it
means, because the constraints follow from it.

## 6. Decision

None. The four above are stated so they can be answered together, since 5.1
decides whether 5.2 matters and 5.4 decides what 5.3 can be run against.

What this record does settle is the frame: completeness and retrieval quality
are two different claims with two different owners, and no threshold on the
second is meaningful without the query parameters beside it.

## 7. Consequences

#2068 stays open on these four questions rather than on anything being measured;
its research phase is complete and this record is what it produced.

Nothing here blocks the schema half, which already works: two generations with
different dimensions and different operator classes describe, plan and retire
correctly, and halfvec round-trips as well as vector does.

One warning worth carrying: that baseline was true only after a defect was
fixed the same day. The dimension had been dropped on read, and `--dry-run`
reported success because **both sides of the comparison read through the same
defective projection**. Any verification this epic builds has to avoid that
shape — a check that reads both sides the same way agrees with itself.
