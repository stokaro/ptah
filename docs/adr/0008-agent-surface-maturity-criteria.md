# ADR 0008: What the agent surfaces must satisfy before they stop being experimental

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1490](https://github.com/stokaro/ptah/issues/1490), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Answers the Phase 7 checklist item *"Define experimental-to-stable criteria."*

## 1. Context

Three surfaces expose the same semantic capabilities to three different callers:
the agent API, the MCP server, and Ptah Assist. #1483's non-negotiable invariant
is that they are one contract rather than three implementations, and Phase 7
exists to measure that they are.

Phase 7 asks for the criteria that would let those surfaces be called stable.
Before this record there were none, and the gap was not a missing paragraph:
**nothing in the repository declares the surfaces experimental in the first
place.** Measured on `master` — `experimental` appears in the tree only in
ClickHouse's native-JSON notice and in a TimescaleDB schema name, and nowhere in
`internal/mcpserver`, `internal/agentapi`, `internal/assistloop` or `docs/adr`.

That is the shape this record fixes. A surface with no declared maturity has no
label for criteria to lift, so "when is it stable" cannot be answered even in
principle, and the answer drifts into whatever the next reader assumes.

## 2. Decision

**The agent API, the MCP server and Ptah Assist are experimental until every
criterion below is met, and each criterion is met by a test rather than by
judgment.**

A criterion is satisfied when something in the repository fails if the property
stops holding. A criterion satisfied by a document, a review, or a maintainer's
recollection is not satisfied.

### 2.1 The criteria, and what measures each

| # | Criterion | State | Measured by |
| --- | --- | --- | --- |
| 1 | The agent API and MCP answer equivalently for every shared capability | met | `internal/mcpserver/cross_surface_test.go`, `cross_surface_workspace_test.go`, `conformance_test.go`, `contract_snapshot_test.go` |
| 2 | MCP and Assist produce deterministic, equal results for one input | met | `internal/assistloop/cross_surface_test.go`, `assistloop_test.go` |
| 3 | An invalid or hallucinated tool call is refused rather than acted on | met | `internal/assistloop/invalid_tool_calls_test.go` — five bad-call shapes plus a non-interference control |
| 4 | A repeated loop terminates, and the iteration limit holds | met | `internal/assistloop/assistloop_test.go`, end to end in `cmd/assist/{explain,context,jsonl}_test.go` |
| 5 | No credential reaches a response, a log, or a session record | met | `internal/agentapi/credential_sweep_test.go`, `credential_redaction_test.go`, `internal/assistsession/redact_test.go`, `internal/aiprovider/transport_test.go` |
| 6 | An artifact edited concurrently is refused rather than silently overwritten | met | `internal/mcpserver/artifact_test.go`, `concurrent_previews_test.go` with its retry control, `internal/agentpatch/agentpatch_test.go` |
| 7 | Every dialect the surface accepts is answered, and answered differently | **partial** | `internal/mcpserver/multi_dialect_corpus_test.go` — six of ten dialects |
| 8 | Untrusted repository content does not steer the surface | **partial** | `internal/mcpserver/adversarial_content_test.go` — one fixture, one channel, no model in the loop |
| 9 | A workspace that moves underneath a session is refused, not retargeted | **partial** | measured one layer down in `internal/pathguard`; never through the agent surface |
| 10 | These criteria exist and are current | met | this record, and `check-adr-index.sh` over `docs/adr/README.md` |

Criterion 10 is not a formality. Criteria 1 to 6 were all satisfied before
anybody wrote them down, and #1490's checklist read as unstarted work for
months while six of its boxes were already delivered. A criterion nobody can
find is one that gets re-derived.

### 2.2 Two questions this record does not answer

Both are decisions rather than tests, and recording them unanswered is
deliberate: answering them in passing would settle a contract in a document
whose job is to say what is unsettled.

**How is surface equivalence asserted for the stateful artifact triple?**
`preview_patch` mints a single-use token — `internal/mcpserver/mcpserver.go`
says so in the tool description and again in the comment beside it — so
equivalence cannot be shown the way criterion 1 shows it everywhere else, by
calling the same thing on both surfaces and comparing. Either the comparison
becomes a state-machine equivalence rather than a value equality, or the triple
is excluded from criterion 1 with that exclusion written down. Excluding it
silently is the option this record rules out.

**Does promotion require a live-database run of the agent contour?** Today it
does not, and that is measurable rather than a guess: 18 tests under `internal/`
drive the surface through `aiprovider.NewFake` and an in-memory transport, none
under `integration/` uses the fake at all, and `integration/agentapi` holds a
single live file, `read_database_live_test.go`. So the surface is proved against
a fake almost everywhere. Whether that is sufficient for a stable label is a
product decision about what "stable" promises.

## 3. Consequences

The three partial criteria are the remaining work, and each has a branch of its
own rather than a plan: the dialect corpus, the untrusted-content channels, and
the moved-workspace refusal.

Until they close, the honest statement about these surfaces is that they are
experimental — and this record is what makes that statement checkable instead of
a matter of opinion. When they close, the two open questions in §2.2 are what
stands between the criteria being met and the label being changed.

Two smaller gaps were found while auditing and are recorded here so they are not
lost, neither of them named by a criterion above:

- The loop's repeat fingerprint is `name + arguments`. Nothing calls one tool
  more than `MaxRepeats` times with *different* arguments, so a mutant reducing
  the fingerprint to the name alone stays green — and a model legitimately
  reading four artifacts in a row would be cut off with nothing red.
- `internal/assistloop` answers a tool call whose arguments are not a JSON
  object, which is exactly what a model emits when it is cut off mid-call.
  Nothing reaches that path.
