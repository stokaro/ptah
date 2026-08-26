# ADR 0008: The agent surfaces are experimental, and these are the criteria that end that

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1490](https://github.com/stokaro/ptah/issues/1490), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Answers the Phase 7 checklist item *"Define experimental-to-stable promotion criteria."*

## 1. Context

`ptah mcp serve` and `ptah assist` ship today. Nothing in this repository says
what they are: `experimental` appears in no document, ADRs 0001-0007 record no
maturity, and neither operator page carries a status. So there is no label for
criteria to lift, and a reader deciding whether to build on the contract has
only the code to go by.

That is the smaller half. The larger half is that "stable" for this surface is
not one property. It is a published contract other people's clients bind to, a
policy boundary that decides what a model may reach, and a loop that runs a
model against a repository. Each fails differently, and a promotion decision
that named only the first would promote the other two by omission.

### 1.1 What the surfaces are today

**Experimental**, by this record. Concretely, that means: the contract may
change with the version date, the capability names are not frozen, and a client
should read `agentapi.Version` rather than assume.

It does **not** mean unmeasured. Most of what promotion needs is already built
and gated, which is why this record is a list of criteria with states rather
than a plan.

## 2. Criteria

Each criterion says what must be true and what measures it. A criterion with no
measurement is not a criterion; it is a hope, and it is marked as one.

### 2.1 The contract is pinned and its changes are deliberate

**State: met.** `internal/mcpserver/contract_snapshot_test.go` holds every
served tool, its description and its full input and output schema against
`testdata/agent-contract.json`, so a change to the published surface has to be
made on purpose. `agentapi.Version` carries the rule for when the date moves,
and its comment records each change and why.

### 2.2 Every refusal has a code, and the code is stable

**State: met.** ADR 0007 and `internal/agentdiag`; the taxonomy is closed and
carried in `_meta`, so a client branches on a code rather than on prose.

### 2.3 Authority is decided in one place

**State: met.** ADR 0006: every operation runs on a session and asks the broker
first. `internal/agentapi/authorization_test.go` and the cross-surface
conformance run assert a denied capability is refused identically on every
surface.

### 2.4 The surfaces answer the same thing

**State: partial, and this is the criterion that needs a decision.** Direct-call
versus MCP equality is asserted for five of the nine served tools, and MCP
versus Assist for two. The four that are not compared are not an oversight:
`read_database` needs a live target, `search_docs` is exercised on one surface,
and `preview_patch`/`apply_patch` mutate and mint a single-use token, so
equality cannot be shown by calling both surfaces with the same arguments.

*Open question 1.* What "the surfaces agree" means for the stateful triple. The
options are to compare the two answers structurally with the token and digest
elided, to drive one surface and assert the other observes the same resulting
artifact, or to accept read-only equivalence as the promise and say so.

### 2.5 The repository is treated as data, on every channel it arrives through

**State: met.** `internal/mcpserver/adversarial_content_test.go` covers the
artifact channel and `adversarial_schema_source_test.go` the schema-source one,
each asserting the injected text reaches the caller *and* arrives labeled, with
the capability table unmoved. The notice is not sold as a control; the controls
are the broker, the path containment and the gates.

### 2.6 No surface carries the operator's credentials

**State: met.** `internal/agentapi/credential_sweep_test.go` sweeps whole
rendered answers rather than named fields, with a control that keeps the
"does not contain" assertions from passing vacuously.

### 2.7 A write cannot be redirected or replayed

**State: met.** Digest-bound patches, single-use preview tokens, path
containment bound to an operating-system handle, and the refusals for a
concurrent edit and for a project that moved under the session.

### 2.8 The loop is bounded and says why it stopped

**State: met.** Turn, tool-call and repeat limits, each asserted at the boundary
and end to end through the CLI, with the published numbers bound to the document
that quotes them.

### 2.9 The surface is exercised against real databases

**State: not met.** Apart from `integration/agentapi/read_database_live_test.go`
the whole agent contour runs on a fake provider and an in-memory transport.

*Open question 2.* Whether promotion requires a live-database run of the agent
contour, or whether the read path being covered by the ordinary integration
suite is enough given that the agent surface adds no SQL of its own.

## 3. Decision

The agent surfaces are experimental until 2.4 and 2.9 are answered and their
answers implemented. The other seven criteria are met and are regression
surfaces: a change that breaks one un-promotes the surface rather than merely
failing a test.

## 4. Consequences

A reader of the operator pages learns the status from the page rather than by
asking. A criterion that regresses is visible as a failing gate, because every
met criterion above names one. And the two open questions are recorded as
questions, so the next person does not re-derive that they are open — which is
what this record exists to prevent.
