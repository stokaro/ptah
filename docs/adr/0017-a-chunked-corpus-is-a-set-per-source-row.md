# ADR 0017: A chunked corpus is a set per source row, and the set is the unit of identity, correctness and replacement

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2625](https://github.com/stokaro/ptah/issues/2625)
- Builds on [ADR 0013](0013-inference-state-transitions-answer-with-their-provenance.md), whose row rules this record restates over a set

## 1. Context

[#2068](https://github.com/stokaro/ptah/issues/2068) proved one source row to
one vector and said, in its own words, that one-to-many chunking "requires a new
identity and cardinality model because `one source key → many target chunk keys`
is not the same migration problem". #2625 promotes that work and asks for the
model to be written down before any of it is built, because the model is the
whole of the problem: written afterwards, it is decided by whichever branch of
an `if` ran.

This record decides the model. It does not decide the splitter.

### What the shipping code assumes, measured

Four assumptions, each read out of the tree at `10aeff8d3` rather than
remembered.

**The target write cannot create a row.** The only write to the target relation
is an UPDATE addressed by the source key:

```go
query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", …)
for index, key := range t.spec.Source.KeyFields {
    conditions = append(conditions, fmt.Sprintf("%s::text = $%d", quoteIdentifier(key), len(arguments)))
}
```

Every `INSERT INTO` in `internal/embedpg` names one of Ptah's own tables — the
run, generation, pointer and outbox-event tables — and none names the target.
So chunking is not a cardinality parameter added to an existing write path.
There is no path that produces a second row for one source key.

**A second row for one key is a blocking finding.** `corpusWalk.takeDuplicate`
reports adjacent rows sharing a key, and the report calls it blocking:

```go
report.addf(LayerCoverage, Blocking, w.duplicates.total, w.duplicates.kept,
    "%d target keys appear more than once", w.duplicates.total)
```

Under chunking that shape is not a defect, it is the corpus.

**The coverage verdicts are per row.** `takeCoverage` answers one of four:
`rowMissing`, `rowWrongGeneration`, `rowStale`, `rowCovered`, and a source row
with no stored row is `missing`. A source row holding four chunks whose current
text implies three is none of those four. It is not missing, nothing about it is
stale in the version sense, and it is not covered.

**The identity already covers the transformation, up to chunking.** Eight
preprocessing parameters are in the generation digest — separator, prefix, null
policy, empty policy, unicode normalization, collapse whitespace, max input
bytes, truncate — beside the source, model and target facts. Chunking is the
same kind of fact and is absent because it does not exist.

### What the prerequisites actually stand at

#2625 orders the work ADR, then [#2624](https://github.com/stokaro/ptah/issues/2624)
(a separate target table), then [#2621](https://github.com/stokaro/ptah/issues/2621)
(streaming verification). Measured:

- #2621 is **closed**. `VerificationCorpus` hands back an `iter.Seq2` and the
  two rows a position points at are written through on every row, so a
  verification's memory is proportional to findings rather than to rows. A
  chunked corpus multiplies by the chunks-per-row factor, which is exactly the
  cost that change removed.
- #2624 is **partly in**. A generation whose vectors live in a table of their
  own prepares, backfills, catches up, indexes and verifies — the joined
  verification query and the target-only position both exist. What is missing is
  that Ptah does not create that table: `EnsureTarget` is
  `ALTER TABLE … ADD COLUMN IF NOT EXISTS` and the operator creates the relation
  and its key column first.

So the layout this model needs exists as a shape and not yet as an object Ptah
creates.

## 2. Definitions

**Chunk.** One piece of a source row's text, produced by a deterministic
splitter from the input the preprocessing rules already assemble.

**Chunk set.** Every chunk one source row's current text produces, under one
generation's parameters. It is ordered, finite, and may be empty.

**Ordinal.** A chunk's position within its set. It orders; it does not identify.

**Set write.** One operation that makes a source key's stored rows equal to its
chunk set — creating, updating and removing rows as that requires.

## 3. Decision

### 3.1 The unit of correctness is the chunk set, not the chunk

Every coverage verdict is restated over a source key's set. A source key is
covered when its stored rows are exactly the set its current text and the
generation's parameters produce; it is stale when the version or the input hash
says the text moved; it is missing when it has no rows and its text produces
some.

This is the decision the "four chunks where the text implies three" case forces.
Per chunk there is no verdict for it. Per set there is one, and it is the
ordinary `stale`: the set the source implies is not the set the target holds.

### 3.2 A chunk is addressed by (source key, ordinal), and the ordinal is not an identity

The ordinal is how a chunk is written down and read back in order. It is not
stable across a re-chunk: text that gains a sentence can move every boundary
after it, so chunk 3 before and chunk 3 after are not the same chunk in any
sense a rule may rely on.

Therefore **no rule may compare a chunk to the chunk that held its ordinal
before**. ADR 0013's row rules — a write never crosses generations, a stale
answer does not win, a tombstone survives a late update — are restated with the
source key as their subject and the set as what they carry. They are not applied
per ordinal.

The alternative, a content-derived chunk id, is rejected in §4.

### 3.3 The chunking parameters join the generation identity

They go in the `pre.` group of `identityComponents`, beside the eight already
there, because they are the same kind of fact: a parameter that changes what
text the provider is asked about. A corpus chunked at 512 and one chunked at 256
are different corpora, and the digest is what says so.

That includes the splitter's own version where the splitter is not fixed by its
parameters alone. A splitter whose output can change under an upgrade without a
parameter changing is a reproducibility hazard of the kind
`ReproducibilityPartial` already names for a provider with no immutable
revision, and it is recorded the same way rather than silently.

### 3.4 A set write is one operation, and the empty set carries a tombstone

Going from four chunks to three and going from four chunks to none are the same
operation with different arguments. One mechanism, not two: the set write
removes the rows the new set does not have.

The empty set is the exception that proves it needs care. A tombstone exists so
that a late, out-of-order update cannot resurrect a row that left scope, and a
set of zero rows has nowhere to carry that record. So **an empty set is stored
as exactly one tombstone row at ordinal 0**, carrying the version and no vector,
and the general rule that a stored row's ordinal must be within its set's length
takes that row as its base case.

Reporting follows: `takeOutOfScope`'s existing exemption — a tombstone with no
vector is Ptah's own record and not a finding, while a tombstone that still
holds a vector is — applies unchanged to the ordinal-0 row.

### 3.5 The duplicate finding is restated, not removed

"Target keys appear more than once" stops being a defect and becomes the shape
of the data. What replaces it is the finding that actually matters: **a source
key holds rows its chunk set does not declare** — a surplus ordinal, a gap in
the sequence, or a row at an ordinal beyond the set's length.

Removing the finding rather than restating it is refused. It is the check that
catches a half-completed set write, which is the failure this cardinality
introduces.

### 3.6 The write path gains the ability to create and remove rows

This is the largest consequence and it is stated as a decision so it is not
discovered later: the target write is an UPDATE keyed on the source key, and it
must become an operation that can insert and delete. It cannot be reached by
parameterising what is there.

It also means the separate target table is a hard prerequisite rather than a
companion: rows a source row does not have cannot live in the source row.

## 4. Alternatives

**A content-derived chunk id instead of an ordinal.** Stable across re-chunking,
and it makes an unchanged chunk identifiable as unchanged, which would let a
re-chunk re-embed only what moved. Rejected for this record because two
identical chunks in one document collapse to one id, so the id is not a key
without an occurrence counter — at which point it is an ordinal wearing a hash.
The saving it offers belongs to re-chunking as an operation, which #2625 lists
as an explicit non-goal, and a model that assumed it would have to be revisited
to deliver the non-goal it was chosen for.

**Chunks as their own rows in the application's schema, with Ptah pointed at
them.** This is what a reader is told to do today, and it works. Rejected as the
answer because it moves chunk identity, re-chunking and source consistency into
a hand-written pipeline — which is the whole of the problem, handed back.

**A per-chunk verdict, with a set-level check layered on top.** Rejected because
the per-chunk verdicts are the ones that have no answer for the case that
motivates the feature. Two verdict systems where the lower one cannot answer is
the shape that makes a report look complete and say nothing.

**Keeping the source-row key and storing the set as an array or JSON column.**
One row per source row, so every rule above survives untouched. Rejected because
a vector index is per row: pgvector indexes a column, and a set stored in one
row is one indexable value or none. The retrieval this feature exists for would
be gone.

## 5. Consequences

- `internal/embedverify` gains a set-shaped walk. The corpus is already an
  ordered stream with equal keys adjacent, which is what a set-per-key fold
  needs, so this is a fold over a run of rows rather than a new traversal.
- `embedrun.ResolveWrite` keeps its rules and changes its subject. Its three
  guarantees are about a source key's set from here on, and the per-ordinal
  comparison it must not grow is written down in §3.2.
- The generation identity changes shape, so every existing generation's digest
  is unaffected only while the chunking parameters are absent. Adding them with
  a zero value that means "not chunked" keeps existing digests stable; adding
  them any other way rewrites the identity of every corpus already published.
  This record chooses the former and marks it as the reason the parameters are
  optional in the specification.
- `#2624` becomes blocking rather than adjacent, and its remaining half — Ptah
  creating the target table — is on this feature's path.
- The four state columns (`_generation`, `_input_hash`, `_source_version`,
  `_state`) stay per stored row, which is per chunk. The input hash of a chunk
  is the hash of that chunk's text, not of the source row's, so a set write can
  leave a chunk untouched when its text did not move.

### What this record does not decide

- The splitter: its algorithm, its boundary rules, and whether it is
  configurable beyond size and overlap.
- Re-chunking an existing corpus as a distinct operation, which #2625 lists as a
  non-goal.
- Whether the ordinal sequence must be dense, which is a property of the set
  write's implementation rather than of the model.
- The chunk column layout in the target table, which follows #2624's remaining
  half rather than preceding it.
