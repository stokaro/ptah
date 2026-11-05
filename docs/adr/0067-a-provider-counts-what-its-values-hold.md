# ADR 0067: A provider counts what its values hold

Status: Proposed

Contract: 25.24

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0056](0056-a-comparison-service-counts-the-objects-it-compares.md),
[ADR 0062](0062-a-provider-reverses-the-changes-its-comparison-returned.md)

## Context

`ptah schema stats` writes one OpenMetrics gauge per object kind. The common
kinds, such as tables and columns, are counted by the host. Every other kind
belongs to a feature owner: the owner declares the metrics a value of its model
contributes to, and the host asks it for each value's counts through the
reporting service (`schemaext.ReportingService`). The host cannot count these
values itself, because a value is a payload only its owner reads: a YDB topic
value holds its consumers, and a changefeed value holds the consumers of its
topic.

A provider's model declares a plural label for omission reports, and nothing
else. The host registers each provider model with that label and no metric, so a
kind a provider owns never reaches the scrape.

Measured with Ptah master ba83f5966 and `stokaro/ptah-ydb-plugin` master
07068a0 on local YDB 25.1.4.7 and 26.2.1.14, with the same results on both
lines. The directory holds two tables (five columns, one index), a topic with
two consumers, a changefeed with one consumer, and a coordination node.
`ptah schema stats --schemas <dir>` prints:

| Metric | The provider path | The built-in YDB engine |
| --- | --- | --- |
| `tables`, `columns`, `indexes` | 2, 5, 1 | 2, 5, 1 |
| `topics`, `topic_consumers` | absent | 1, 2 |
| `changefeeds`, `changefeed_consumers` | absent | 1, 1 |
| `coordination_nodes`, `streaming_queries` | absent | 1, 0 |
| `external_data_sources`, `secrets`, `resource_pools` and the other YDB object metrics | absent | 0 |

The built-in YDB engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so a
pipeline that charts these gauges loses them when the provider becomes the only
YDB engine.

## Decision

### A model declares its metrics

`models.report-counts` is a two-way feature: the provider and each engine whose
models declare metrics list it, and the host lists it in its host features.
Contract 25.24 adds one field, one RPC and four messages:

```proto
message ValueModel {
  // ...
  repeated ModelMetric metrics = 10;
}
message ModelMetric {
  string name = 1;
  string help = 2;
}
rpc ReportValues(ReportValuesRequest) returns (ValueReports);
message ReportValuesRequest {
  string engine = 1;
  repeated string target_features = 2;
  repeated ValueInput values = 3;
}
message ValueReports {
  bool complete = 1;
  repeated ValueReport values = 2;
}
message ValueReport {
  string id = 1;
  repeated MetricCount counts = 2;
}
message MetricCount {
  string name = 1;
  uint64 value = 2;
}
```

A metric means what `schemaext.MetricDefinition` means: `name` is the gauge's
suffix, lowercase ASCII letters, digits and underscores starting with a letter,
and `help` is one line. A desired or observed model with a display name may
declare metrics, each name once. Two models of one representation that declare
one name give it one help text, since the scrape shows one help line per name,
and an engine that shares a model with another declares the same metrics for it.
Without the feature the host receives no metrics: the negotiated description
removes them.

### The provider counts

`ReportValues` is offline: no session, no database. The host sends values of
models that declare metrics, of one engine and one representation, and the
provider answers each, in order and under its `id`, with one count for each
metric its model declares, in the declared order. A count is what the value
holds, as a built-in owner's report states it: one topic and the number of its
consumers, not what the server would say about the topic's partitions.

Both transport boundaries refuse an unnegotiated call, a value of a model
without metrics, values of mixed representations, an incomplete reply, a reply
in another order or of another length, a count of a metric the model does not
declare, a metric counted twice or left out, and a count above the largest
signed 64-bit integer.

### What the host does

The host registers each provider model that declares metrics with them, and
dispatches the reporting batch of that model to `ReportValues`, one request per
engine and target. The counts then reach every consumer of the reporting
service, `schema stats` among them, exactly as a built-in owner's do. A model
without metrics keeps its label-only registration.

### What the YDB provider counts

The provider declares on its desired and observed models the metric names and
help texts the built-in YDB engine declares, so a scrape through either engine
has the same lines for these kinds:

- a topic counts `topics` 1 and `topic_consumers` its consumers;
- a table's changefeed set counts `changefeeds` its feeds and
  `changefeed_consumers` the consumers across them;
- a coordination node, a streaming query, an external data source, an async
  replication, a transfer, a secret, a resource pool and a resource pool
  classifier count 1 each under their metric;
- an external table counts `external_tables` 1 and `external_columns` its
  columns.

## Alternatives considered

**The host counts the values.** It would need to read each payload, which only
the owner reads, and the counts of a topic depend on what the payload holds.
Rejected.

**Counts in the inspection snapshot.** A read could state each native object's
counts. But reporting serves desired values too: `schema stats` counts the read
after it is converted to its declared form, and the export loss report counts
values decoded from a source, which no read produced. Rejected.

**Counts in the normalization reply.** Every value passes through
`NormalizeValues`, so the reply could carry its counts. The host would have to
keep counts it never uses with every captured value, and a value's normalized
form would change meaning by contract version. Rejected.

**An unnegotiated field.** A host that does not know the RPC would never call
it, so the call itself is safe. But the metrics in a description would reach a
host whose runtime registers them without a service that answers. Rejected.

## Consequences

- Contract 25.24 adds a field, an RPC, four messages and a two-way feature. It
  is numbered after contract 25.21, which ADR 0064 proposes, and the two
  numbers between them, which other open records of
  [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204) hold. The
  first of them to merge takes the lowest free number.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Without the feature nothing changes: a provider's kinds stay out of the
  scrape.
- The scrape through a provider still lists only the metrics of that
  provider's models. The built-in engines' runtime registers every dialect's
  owners, so its scrape of a YDB database also lists, at zero, the metrics of
  other dialects' owners, which say nothing about YDB.
- A provider metric named like a common metric, such as `tables`, makes
  `schema stats` refuse, as a built-in owner's would.

## Acceptance evidence

- SDK tests accept a negotiated model with metrics and a complete report, and
  refuse metrics on an engine or provider that does not list the feature, each
  malformed metric declaration above, an unnegotiated or malformed request, and
  each reply the checks above refuse.
- Through the SDK fixture provider, the host counts a fixture model's values in
  `schema stats` for a host that lists the feature, and a provider without the
  feature behaves as today.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14, `schema stats` over the
  directory measured above prints the same values as the built-in engine for
  topics, topic consumers, changefeeds, changefeed consumers, coordination nodes
  and streaming queries.
