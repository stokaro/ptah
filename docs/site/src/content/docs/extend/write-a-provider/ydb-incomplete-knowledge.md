---
title: "What a comparison may conclude: YDB transfer scenarios"
description: How the YDB transfer comparison decides from what a read established and what it did not, one changed fact at a time, with the test that measures each result.
type: concept
audience:
  - "go-developer"
  - "integration-developer"
  - "database-engineer"
readerQuestion: "When a read did not establish every fact, what may Ptah conclude about a YDB transfer's source and target, and who refuses a plan that depends on a fact nobody established?"
goal: "Predict, for a change to the walkthrough example, which component decides, what the run reports and what to do next, and find the test that measures it."
sourceOfTruth:
  - "provider-sdk/protocol/experimental/provider.proto"
  - "provider-sdk/provider/current_context.go"
  - "docs/adr/0122-a-comparison-reads-the-current-state-of-another-comparisons-models.md"
  - "stokaro/ptah#5807"
generated: false
searchAliases:
  - "incomplete coverage"
  - "transfer refused"
  - "current context"
  - "YDB transfer scenarios"
overlaps:
  - /extend/write-a-provider/ydb-walkthrough/
  - /extend/write-a-provider/models-and-planning/
disposition: keep
---

The [walkthrough](../ydb-walkthrough/) declared every object the transfer
`ingest_orders` depends on, and the read could describe each of them. Here
one fact of that example changes at a time: a source the schema leaves out,
a changefeed the read cannot describe, a directory the run does not select.
Each change shows what the read established, which component decides, what
the user sees and what to do next.

[What a read promises](../models-and-planning/#what-a-read-promises) defines
coverage. This page shows what coverage permits a comparison to conclude.

## What a read can establish

Coverage is evidence of what a read established. It is not a confidence
score, a permission, or a second list of objects. Four states matter for the
objects of the walkthrough:

| State | What the read established | Example | What a plan may conclude |
| --- | --- | --- | --- |
| Described | The object and its definition, sent as a value | `orders/feed`, with its mode, format and consumer | Compare it with the declaration, then keep, change or drop it |
| Absent | A complete region holds no such object | No topic `events` in `shop`, the directory the run selects | It is not there, and nothing may rely on it |
| Recorded, not described | The object is there, but its definition does not fit the model: a coverage record names it as uninspected or unrepresentable, and no value is sent | A changefeed whose consumer names codec 10042 | Leave it as it is; conclude nothing about its definition |
| Not inspected | Nothing: the region is outside the read | Directory `ext`, when the run selects `shop` | Nothing, in either direction |

Two readings are wrong. A region the read did not inspect is not evidence
that an object there exists. An empty record list, or a missing coverage
record, is not evidence that nothing exists.

## The case current context exists for

ADR 0122, a comparison reads the current state of another comparison's
models, starts from one measured case on YDB 26.2.1.14:

- `orders` stays declared and declares no changefeed.
- The database holds `orders/feed`. A consumer of its topic names codec
  10042, which Ptah does not declare.
- The read records the changefeed set of `orders` as unrepresentable and
  sends no value for it.
- The declaration adds `ingest_orders`, which reads `orders/feed`.

The question is whether `orders/feed` is still there after the plan has run.
The plan neither drops nor changes what the read could not describe, so the
answer is yes, and the transfer may be created. That is the whole
conclusion. It does not mean that Ptah understands the changefeed's
configuration, and it does not mean that the transfer will run well once it
exists.

"The source exists now, so the transfer is valid" is not the rule. Change
one fact: the read describes `orders/feed` completely. The declaration,
which leaves the changefeed out, now decides: the plan drops it, and the
transfer would read a stream that is gone. The comparison refuses that
transfer, although the stream exists when the read runs.

The transfer comparison `ydb-transfers` does not own changefeeds, so it
learns what the read found of them only through `current_context` (contract
25.40, feature `models.current-context`). It needs the values as well as the
coverage. A facet's coverage record names its owner, the table `orders`, and
not one stream of it. A read that sent the set's value and marked inside it
the part it could not describe would leave nothing in coverage to read.

Where the host or the provider does not negotiate the feature, the host
sends no current context, and the transfer is refused as one of a missing
topic. Ptah v0.25.0 and YDB plugin v0.8.0 are the first releases that both
list it, measured on YDB 26.2.1.14; Ptah v0.24.1 and plugin v0.7.0 do not.

The measured acceptance is for a changefeed's topic. A standalone topic with
such a consumer is another case: a plan over its directory is refused,
naming the topic and the codec, whether the declaration declares the topic,
drops it or says nothing of it. A topic the read records as uninspected,
such as a queue of the older persistent queue kind, is accepted as a source
too, but only a unit test with a constructed request shows that, not a run
against a server.

## Scenarios

Each row changes one fact of the walkthrough. The comparisons run inside the
provider during `ptah schema compare`, and during `ptah schema apply` before
any statement runs, so a refusal leaves the database as it was. Each quoted
reason is part of the text the run prints, which names the transfer and
the topic or table.

| Change from the walkthrough | Decided by | Result | Next step |
| --- | --- | --- | --- |
| 1. None: every object declared, the transfer new | The comparisons, then Core's ordering of their operations | Four operations, the transfer last, after its changefeed | Apply |
| 2. None, a second run | The same comparisons, with the same checks | `Schema is synced, no changes to be made` | None |
| 3. The transfer's topic is neither declared nor present | `ydb-transfers` | Refused: "so the plan leaves no such topic" | Declare the topic or the changefeed |
| 4. The topic is present; the declaration drops it and keeps the transfer | `ydb-topics`, from the requirements Core collects | Refused: "is dropped while a declared transfer reads it" | Keep the topic, or drop the transfer too |
| 5. The changefeed is described, and its table does not declare it | `ydb-transfers`, from current context | Refused as a transfer of a missing topic | Declare the changefeed |
| 6. The changefeed is recorded, not described | `ydb-transfers`, from current context | A new transfer is created; a kept one plans nothing | None; nothing checked the changefeed |
| 7. A declared changefeed was dropped by hand; the transfer is unchanged | `ydb-row-table-properties` | One operation recreates the changefeed and its consumer | Apply |
| 8. The target table is not declared | `ydb-transfers`, from the declared relations | Refused: "which the schema does not declare" | Declare the table |
| 9. The topic is in a directory the run does not select | `ydb-transfers` | Refused; see below for the reason | Select the topic's directory too, or declare the topic |
| 10. The read fails | The provider's read | The command fails; nothing is compared | Fix what the error names |
| 11. The server has no transfers, such as a 25.1 server | `ydb-transfers`, from the server version | Refused: `provider compare models failed (InvalidArgument)` | Use a server with transfers |
| 12. The host does not send the declared topics (contract 25.33) | `ydb-transfers` | No topic check; YDB stops the transfer later if its topic is missing | Use a host that lists `models.declared-objects` |
| 13. The current context is malformed | The SDK, before the provider's comparison | A protocol error, such as `duplicate comparison record` | Report a host or provider defect |

Each result is measured by a test:

| Scenario | Test |
| --- | --- |
| 1, 2 | [`TestHostCreatesATransferAfterItsChangefeed_HappyPath`][order] |
| 3 | [`TestHostRefusesATransferOfAnUndeclaredTopic_FailurePath`][replication] |
| 4 | [`TestHostRefusesToDropATopicATransferReads_FailurePath`][replication] |
| 5 | [`TestTransferComparisonAcceptsARecordedSource_FailurePath`][unit], a unit test |
| 6 | [`TestHostCreatesATransferOfAChangefeedItCannotDescribe_HappyPath`][recorded] |
| 7 | [`TestHostRestoresTheChangefeedAKeptTransferReads_HappyPath`][sources] |
| 8 | [`TestHostRefusesATransferIntoAnUndeclaredTable_FailurePath`][replication] |
| 9 | [`TestHostRefusesATransferOfATopicOutsideTheScopeAsUnread_FailurePath`][sources] |
| 10 | [What a read promises](../models-and-planning/#what-a-read-promises) states the rule |
| 11 | [`TestHostRefusesATransferWithoutTheCapability_FailurePath`][replication] |
| 12 | [`TestTransferComparisonWithoutDeclaredTopics_HappyPath`][unit], a unit test |
| 13 | [`TestCurrentContext_FailurePath`][sdk], an SDK test |

In scenario 1, a row written to `orders` then reaches the transfer's target.
In scenario 4, the topic and the running transfer stay as they were. In
scenario 8, YDB would refuse the transfer anyway (`Path does not exist`).
In scenario 9, a run with current context gets "is in a directory this run
does not read, so the plan cannot tell whether it is there"; without it, the
run gets the reason of a missing topic. YDB plugin v0.8.0 gives the reason of
a missing topic in both cases.

Scenario 2 is a real no-op because the comparison runs its dependency
checks on every run, before it compares the transfer itself. A synced report
means those checks passed, not that they were skipped. A transfer whose
topic the schema leaves out is refused in a run with no other change too. A
check in a planner could not do that: Core plans only after a comparison
finds a change.

Scenario 13 is the only one in which YDB plays no part. The SDK refuses a
current context of a kind the comparison does not read, one in the desired
representation, a facet whose owner has the wrong kind, a record listed
twice, and one without coverage. A contract rejection says two peers
disagree about the request's shape; a refusal with a YDB reason says the
request was well formed and the plan would fail on the server. The SDK test
uses constructed requests, because no database is needed to show them.

The scenarios stay within one database. A transfer can read a topic of
another database through a connection string. This database's read says
nothing about that topic, so the comparison does not check it; a unit test
in [`TestTransferComparisonRefusesAnUndeclaredTopic_HappyPath`][unit] shows
the case.

## Where do I look?

| Question | Where to look |
| --- | --- |
| Which differences does the run find? | `ptah schema compare --db-url <url> --schema-file <file> --schemas <directory>` |
| Which statements would run? | `ptah schema apply` with the same flags and `--dry-run` |
| Why was the plan refused? | The command's error output, which carries the provider's reason |
| Which provider version runs, and which features does it list? | `ptah providers check --provider-engine ydb --json` |
| Which host version runs? | `ptah version` |
| What did Ptah send to the provider? | The same command with `--provider-exchanges <new directory>`, which writes each comparison request and its reply or error as JSON |
| Which checks does the contract apply to a request? | The [SDK reference](../sdk-reference/) |

## Further reading

- [Understanding the provider boundary](../ydb-walkthrough/) for the
  components and messages this page uses.
- [Run the YDB walkthrough](../ydb-walkthrough-run/) to run the example these
  scenarios change.
- [Using the YDB provider](../../providers/ydb/usage/) for installing and
  connecting the provider.

[order]: https://github.com/stokaro/ptah-ydb-plugin/blob/24d982d5c88aaa0e474e40a840dc5d4074a9b54e/integration/host_transfer_order_test.go
[replication]: https://github.com/stokaro/ptah-ydb-plugin/blob/24d982d5c88aaa0e474e40a840dc5d4074a9b54e/integration/host_replication_test.go
[recorded]: https://github.com/stokaro/ptah-ydb-plugin/blob/24d982d5c88aaa0e474e40a840dc5d4074a9b54e/integration/host_transfer_recorded_source_test.go
[sources]: https://github.com/stokaro/ptah-ydb-plugin/blob/24d982d5c88aaa0e474e40a840dc5d4074a9b54e/integration/host_transfer_sources_test.go
[unit]: https://github.com/stokaro/ptah-ydb-plugin/blob/24d982d5c88aaa0e474e40a840dc5d4074a9b54e/internal/backend/replication_test.go
[sdk]: https://github.com/stokaro/ptah/blob/9e02b55ca91d9f2d75165fc07bbcc4f9d82d1600/provider-sdk/provider/current_context_test.go
