# ADR 0013: An inference-state transition answers with its provenance, and its approval binds to exact content

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2068](https://github.com/stokaro/ptah/issues/2068)
- Answers the four questions [ADR 0010](0010-retrieval-quality-is-not-a-schema-property.md) stated and declined to decide

## 1. Context

[ADR 0010](0010-retrieval-quality-is-not-a-schema-property.md) closed the
research phase of #2068 with four questions written as questions. The domain
that answers them now exists, and this record is the paperwork for the choices
it embodies — written after the implementation rather than before it, because
three of the four were decided by what the code turned out to need.

The epic's subject is a class of migration a schema tool has not had to handle:
one whose correctness is not a property of the DDL. Changing an embedding model
rewrites every vector in a corpus, and the schema change — a column of a
different dimension — is the smallest part of it. The rest is execution over
data, with a provider outside the database in the middle of the loop.

## 2. What makes this different from a schema migration

Three properties, each of which breaks an assumption the rest of Ptah is built
on.

**The result cannot be derived from the input.** Ptah can render the DDL for a
desired schema and compare it to what a database reports. It cannot render a
vector. What a vector means is decided by a provider Ptah does not run, and two
providers given identical text answer differently — as does one provider on two
days, when the model behind an alias moves.

**The operation is long, resumable, and partially applied by construction.** A
DDL migration either ran or did not. A backfill over a hundred thousand rows is
interrupted, resumed, retried, and concurrent with writes to its own source. At
every instant its target is a mixture of rows that are current and rows that are
not, and the difference is invisible in the data.

**Completion is not correctness.** A backfill that reached the end of its source
proves a loop terminated. It does not prove the vectors are right, or that they
are all there, or that they were computed from the text the source has now.

## 3. Decision 1 — every generation is a content address, and nothing mixes two

A generation is identified by the digest of every property that decides what a
vector MEANS: the source relation and its key and input fields in order, the
whole preprocessing pipeline, the model and its revision, the reported
dimension, the normalization, the representation, and the metric.

Alternatives considered:

- **A version number the operator increments.** Rejected: it makes two
  incomparable corpora look like one whenever somebody forgets, and forgetting
  is the normal case — the properties that decide comparability are spread
  across a specification, not gathered under a version field.
- **The model identifier alone.** Rejected for the same reason, one level down:
  identical models over differently preprocessed text produce vectors that
  cannot be compared, and the preprocessing is where the interesting changes
  happen.

Two halves of the rule are enforced rather than documented. A reflection ratchet
enumerates the specification's fields and requires each to appear in the digest
or in an excluded list carrying the reason changing it leaves existing vectors
comparable. The encoding is length-prefixed, because any separator a component
could contain lets two different specifications produce one digest.

Reproducibility is reported, never fabricated. A provider exposing no immutable
model revision yields `partial` and a sentence naming what is missing, because
an identity claiming `full` over a mutable alias promises a rebuild it cannot
deliver.

## 4. Decision 2 — the plan states where each of its answers came from

Facts are `measured`, `configured`, `inferred`, `unknown` or `unsupported`, and
the classification travels with the value rather than beside it.

The alternative — one plan of equally-weighted statements — was rejected on a
single case. A source nobody counted, rendered as `0`, tells the operator the
backfill is free. The number is the same shape either way, so the type that
reports an unknown takes no value at all: a signature that let a caller supply
one would make it a label rather than a rule, and "unknown, approximately
40 000" is a number people plan around.

Everything except a measurement owes an explanation. An inference without its
premise, an unknown without its reason and an unsupported without what is
missing are each a sentence that sounds like an answer and is not one.

`unknown` and `unsupported` are separate values because a gap in knowledge and
a gap in the product have different fixes — and, at the capability layer, "not
asked" and "answered no" have opposite consequences: one refuses a database that
would have worked, the other promises one that will not.

## 5. Decision 3 — verification is five deterministic layers plus one that is not

The five ask whether the generation is well-formed (including whether its index
is `VALID` and whether its operator class matches the metric), whether every
in-scope source row is covered, whether each stored vector was computed from the
source as it is NOW, whether the vectors are usable numbers of the right
dimension, and whether the run finished what it started.

Coverage is answered key by key rather than by count. The alternative was
rejected by stating it: a source count matching a target count is satisfied by a
corpus that missed a thousand rows and invented a thousand others.

Freshness is the layer with no cheaper substitute. Every key is present, every
count agrees, and one vector answers for text the source no longer has.

Each layer runs even when an earlier one failed, because an operator deciding
what to do wants the whole picture rather than the first thing that went wrong —
with one exception: a missing column stops the layer that would otherwise report
its type, its dimension and its index as three more failures of one fact.

## 6. Decision 4 — quality is a gate the operator declares, and a comparison that refuses to cross conditions

This answers ADR 0010 §5.1 and §5.2 together.

ADR 0010's measurement is that one unchanged index spans 26.5% to 100% recall as
`ivfflat.probes` alone moves, and it concluded that a threshold stated without a
query-parameter value is a coin flip with a number on it.

The decision is **a gate whose thresholds the operator sets, defaulting to
none**, with the query parameters bound to the numbers:

- A policy with no thresholds applies no SCORE floor, and the report is
  evidence. This is the default, and it is ADR 0010's third option. Two
  refusals stand without a threshold and are not floors: a `required` key the
  corpus declared and the search did not return, which no score expresses, and
  a case that produced no result at all unless `--require-every-case=false`
  says otherwise.
- A policy with thresholds gates, and the operator who set them is the one who
  knows the query parameters their application uses.
- The scores carry the parameters they were taken under, and a regression
  comparison across two different sets **refuses and says so** rather than
  producing a number. So does a comparison where either side's parameters went
  unrecorded — an absent setting is not "the defaults".

The rejected alternative is a gate with thresholds Ptah picks. It fails on ADR
0010's own measurement: the threshold would be met or missed by a setting Ptah
does not own, and the operator's fix for the resulting alarm is a rollback of
the wrong thing.

## 7. Decision 5 — an evaluation compares against both, and both gate

This answers ADR 0010 §5.3, which observed that choosing both means saying which
one gates.

Both gate, because they fail differently and the failures need different fixes:

- **Against an exhaustive search on the same corpus**, which asks how much the
  index lost. A generation failing only this has good vectors and a bad recall
  setting.
- **Against the previous generation's numbers**, which asks whether the
  migration changed what users see. A generation failing only this has a worse
  model.

ADR 0010 noted they diverge when the old generation was itself poor. That is
handled by them being separate thresholds rather than one score: an operator
replacing a known-bad generation sets the regression tolerance wide and leaves
the exact-search floor alone.

Neither is scored when it did not run. An exhaustive search that was never
performed and one that agreed completely are the same zero unless the difference
is carried, and only one of them is evidence — so the report names what it could
not measure.

## 8. Decision 6 — the corpus is operator-supplied identifiers, and Ptah does not discover it

This answers ADR 0010 §5.4, which found a tension between the research phase's
"an evaluation corpus is source content" and the epic's definition of a corpus
as user-supplied queries with relevance expectations.

The epic's definition is the one implemented. A case is a query string the
operator wrote and a set of **document identifiers** — not documents. Ptah does
not query arbitrary source tables to assemble one, no relevance judgment is
made by a model, and nothing here returns corpus text.

The research phase's sentence is true of a different object: the source rows an
evaluation's identifiers point AT are source content, and they are governed by
the same boundary as everything else the backfill reads. What the corpus itself
holds is a question and a list of keys.

## 9. Decision 7 — cutover binds to exact evidence, and rollback is measured

A cutover plan carries the evidence it was built from — the verification report
by digest, the consistency watermark, whether the index was ready, which
blocking findings an operator accepted — and an approval binds to the plan's
digest. A reflection ratchet requires every field of the plan to be in that
digest, because whatever the digest does not cover is whatever the approval does
not cover, and that failure is invisible: the approval still matches and the
cutover still runs.

The alternative was an approval naming a generation. It authorizes a plan built
from evidence that has since changed, which is the case the mechanism exists to
prevent.

Evidence is re-read at execution rather than trusted. A plan's evidence is what
WAS true; what IS true is read back, and the gap is what makes a plan able to go
stale rather than able to be re-run forever. Somebody else cutting over first is
the sharpest case: executing the plan then moves the pointer off whatever they
put there, and the plan's own success says nothing about it.

Rollback eligibility is measured, never assumed. The epic's rule is that it must
not be reported as available merely because the old tables still exist, and the
refusals are all generations whose tables are perfectly present: never verified,
no longer maintained, too stale for the policy, or with the index dropped —
which makes going back the same queries against a sequential scan.

Retirement is a third decision with its own permission and its own digest, and
that digest binds what is destroyed rather than what is named. Approving the
removal of an index does not authorize the removal of the column.

## 10. Consequences

- Two generations can be compared only when their identities match, and the
  system can say so mechanically rather than by convention.
- A plan is longer and less confident than one that classified nothing. That is
  the intended trade: the places it hedges are the places it does not know.
- A quality gate does nothing until an operator sets thresholds. A deployment
  that wants retrieval gated has to state what it wants, in terms that include
  the query parameters.
- Approvals expire in practice rather than in principle. Any change to bound
  evidence invalidates one, which means an operator who takes a day to approve a
  plan will be asked to approve a rebuilt one.
- Nothing here can retire a generation as a side effect. Deleting a manifest, a
  session, an image tag or a Kubernetes object reaches none of these decisions.

## 11. What this record does not decide

- The exact PostgreSQL transaction sequence for the transactional-outbox
  consistency mode, which the epic itself flags as needing its own measurement.
- Where run state is durably stored, and under what schema.
- Whether any of this is reachable through the agent surfaces. Decisions 11 and
  14 of the epic bound that, and nothing in these packages exposes vectors or
  source content.
