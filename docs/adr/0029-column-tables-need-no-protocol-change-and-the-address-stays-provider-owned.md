# ADR 0029: Column tables need no protocol change, and the connection address stays provider-owned

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md)

## Context

A YDB column table (`store="COLUMN"` in Go, `column_store` in YAML,
`STORE = COLUMN` in YQL) is supported by the host today for inspection, creation,
comparison, planning and migration. The YDB plugin refuses every part of it, and
its README gives the reasons. The owner's brief asks what the plugin cannot do
because of the protocol. The answer from the code is: nothing in the protocol
blocks it. Every blocker is plugin work or test infrastructure. This ADR records
the measurement so that nobody adds a protocol field to solve a problem that is not
there.

### What the host does with a column table

- Reading. `internal/dbschema/ydb/column_table.go` calls the table service for the
  common description, then reads the monitoring endpoint
  (`/viewer/json/describe?path=...`) through `internal/ydbmonitor` and decodes it
  with `internal/ydbcolumn`. The table service's description omits local indexes
  and the shard count. The plugin README measured the same on 26.2.1.14: the
  description has the hash key (`partitioning_settings.partition_by`) and no
  `PARTITION_COUNT`.
- Endpoint. The monitoring endpoint is the `monitoring=http(s)://host:port` query
  parameter of the YDB URL (`ydburl.MonitoringParameter`). The host never sends it
  to the YDB driver. `ydbmonitor.Read` sends the connection's own credential,
  refuses redirects, bounds the body at 4 MiB, and `column_table.go` refuses a
  credential on a TLS connection unless the monitoring URL is HTTPS.
- Feature flags. The same endpoint serves `/viewer/json/feature_flags`.
  `internal/connector/ydbconnect` calls `ydbflags.Read` when the URL names a
  monitoring endpoint, and `ydbflags` turns the flags that gate a capability into
  the session's capability map. A cluster that runs with a non-default flag
  reports other capabilities than its release line's preset.
- Planning. `internal/planner/dialects/ydb/column_table.go` plans tiered TTL
  `RESET` and `SET`. The storage kind, the hash key and the shard count are fixed
  at creation, so a declaration that differs from a live table is refused with a
  reason. The table-rebuild policy (`--allow-table-rebuild`,
  `PTAH_ALLOW_TABLE_REBUILD`) does not extend to column tables:
  `internal/planner/dialects/ydb/rebuild.go` refuses with "column-table rebuilds
  require an explicit data migration". The plugin therefore needs no planning
  policy input for them.

### What the plugin does

The README ("Column tables") lists four gaps:

1. Inspection refuses a `COLUMN_TABLE` directory entry, so a database that holds
   one cannot be inspected at all.
2. The desired column storage model (`ColumnStorageProperties`,
   `ColumnStorageIntent`) lists no source format, and nothing decodes the Go and
   YAML attributes into it. The plugin claims the Go attributes and refuses them
   by name.
3. Creation requires row storage in the compiler, the operation grammar and the
   read-back confirmation.
4. Comparison owns no column storage kind. Only the tiered TTL changes in place.

Its address parser rejects `monitoring` and `dev_realm` as "unimplemented"
(`internal/connection/address_test.go`). The README lists the accepted
parameters as `database`, `token`, `go_balancer` and `use_env_credentials`. The CI
service for the plugin does not publish the monitoring port.

### Why the protocol is not the blocker

`OpenRequest.address` is an opaque string that the host forwards unchanged. The
SDK says it "can contain credentials" and that no log or durable record may hold
it. The host parses it only to pick the engine from the scheme. A provider is
free to read any query parameter in its own address, which the plugin does for
`database`, `token` and `go_balancer`. The monitoring endpoint can ride in the
address exactly as it does for the built-in engine, and the plugin has the
credential it needs, because it opens the connection.

The other pieces are in place too. `Inspect` returns native objects and facets as
envelopes with provider-owned kinds. `CompareModels` and `PlanFeatures` accept
provider-owned kinds. `PlanningTable.Action` already has `CREATE`, `ALTER`, `DROP`
and `REBUILD`. Session capabilities carry the flag-derived values. ADR 0024
already gives the lifecycle service the creation and alteration of tables.

The honest cost of the opaque address is that the host cannot tell an operator
about a missing parameter before it starts the provider. Today a column table
without `monitoring` fails with a message that names the parameter, and the
provider can produce the same message.

## Decision

**No protocol change for column tables.** The monitoring endpoint, the dev realm
and any other engine-specific connection setting stay inside the provider's
address grammar. The host does not declare, validate, print or route an auxiliary
endpoint. It keeps forwarding the address unchanged and keeps treating it as a
secret.

The work is in the plugin, in this order, and each step is its own pull request
with the corpus the README describes:

1. **Address grammar.** Accept `monitoring` and `dev_realm` with the host's rules
   from `internal/ydburl`: one value, an `http` or `https` URL with a host and no
   user, path or query; a refusal never repeats the value. Keep the rule that a
   credential on a TLS connection requires an HTTPS monitoring endpoint, and the
   rule that the credential is the connection's own. Move `internal/ydbmonitor`
   with its bounds (4 MiB, no redirects).
2. **Feature flags.** When the address names a monitoring endpoint, read the flags
   at `Open` and fold them into `Session.capabilities` with the host's gate table
   (`internal/ydbflags`), so a flag-dependent capability is reported as an
   established answer, `false` included. Without the endpoint the session reports
   the release-line preset only, as the host does.
3. **Reader.** Inspect `COLUMN_TABLE` entries with the table description plus the
   monitoring description (`internal/ydbcolumn`, fixtures for 25.1 and 26.2). A
   column table with no monitoring endpoint is refused with the message that names
   the parameter. It is never reported as a row table, and never reported with an
   unknown shard count.
4. **Source decoders.** Add Go and YAML decoding of `store`, `partition_by_hash`,
   `column_shards`, `column_ttl` and the `column_store` mapping into the column
   storage model. Declare the YAML key and the Go attributes in the provider
   description (ADR 0025). Accept the combinations the host accepts (read replicas,
   key bloom filter, starting layout, row TTL beside a column table) and refuse
   the ones it refuses, with the same reason.
5. **Creation and read-back.** Extend the lifecycle service of ADR 0024: create a
   column table with the native `CreateTable` call and `StoreType`, then confirm
   by reading it back, including the shard count from the monitoring description.
6. **Comparison and planning.** Add the column storage kind to the comparison.
   Tiered TTL is the only in-place change. A different kind, hash key or shard
   count is a refusal with the reason, and the plan is not partial. Under complete
   coverage, a table with no column storage facet declared is a request for row
   storage, as in the host.
7. **Migration.** Versioned migrations that create or alter a column table run
   through `Execute` with `--tx-mode none` for schema statements, which is the
   rule the plugin already documents for YDB.

The plugin's `README.md` section "Column tables" is rewritten when step 6 lands.
The README says the declaration stays refused until the reader, creation and
comparison land together. This ADR keeps that rule: no step turns on the
declaration alone, and the claim in the provider description is added in the same
release as the comparison.

### Trust and authorization

The provider sends the connection's credential to the monitoring URL the operator
wrote in the address. The operator trusts that endpoint exactly as they trust the
database endpoint, because both are in the one address they supplied. The provider
runs with the caller's OS privileges (ADR 0021, `provider-sdk/README.md`). The
rules above (own credential only, no redirect, HTTPS when the database connection
is TLS, bounded body) are conformance requirements for the plugin, not options.
The host sees none of this traffic and holds no copy of the credential.

### Errors and atomicity

Reading is read-only. Creation is a native call followed by a confirming read; a
confirmation that fails or cannot reach the monitoring endpoint reports `UNKNOWN`,
and the host stops without replay. Schema changes are not transactional on YDB, as
the plugin README says.

### Versioning

None. The SDK revision and tag do not change for this ADR. The plugin releases a
new version. The host pins the plugin version in the release (ADR 0026), so the
pin moves when step 6 is released.

## Alternatives considered

**Declare auxiliary endpoints in the connection model.** `Describe` lists named
endpoints, `Open` carries their URLs next to the address, and the host validates
and redacts them. The host would then know that a provider has an HTTP endpoint,
would need a scheme and credential policy for it, and would hold two secret
strings instead of one. It adds a protocol field to solve a problem the opaque
address already solves. It would help only if the host itself had to reach the
endpoint or to forward the credential on the provider's behalf, and neither is
true. Rejected. It stays an option if a second provider needs the host to render
or check a parameter before startup.

**A provider-declared list of address parameters.** `Describe` names the
parameters the address accepts, with a kind and a secret flag, so that `providers
check` can print them and the host can refuse an unknown one early. It is cheap,
and it would give better diagnostics than a refusal at `Open`. It is not needed
for column tables. The refusal at `Open` already names the parameter, and a list
that the host validates would become a second grammar to keep equal to the
provider's. Rejected for now; the owner may still want it for diagnostics.

**A separate `ReadAuxiliary` RPC the host calls with a URL.** The host would fetch
monitoring pages and hand the body to the provider. The host would carry the
credential and an HTTP client, and know the engine has a monitoring page.
Rejected: it contradicts ADR 0023.

**Read the shard count from the table service.** The table service does not carry
it (measured by the plugin on 26.2.1.14). Another native call may exist in a later
YDB release. If one does, the plugin can use it and drop the endpoint for that
read. The decision does not depend on it.

**Leave column tables out of the plugin and refuse them.** The owner decided
against cutting a YDB feature before the flip.

## Consequences

- The protocol is unchanged. The cost falls on the plugin: six pull requests and
  a CI service that publishes the monitoring port (8765) on both fixture
  versions, 25.1.4.7 and 26.2.1.14.
- A URL that works with the built-in engine, with `monitoring=` or `dev_realm=`,
  works with the plugin after step 1. Until then it is refused, and the flip waits.
- The monitoring pages are an unversioned interface of YDB. `ydbcolumn` and
  `ydbflags` check a page version and refuse a layout they have not measured. The
  plugin keeps those checks, so a new server release is a plugin fix and not a
  host release.
- The plugin's feature-flag capabilities decide which declarations the host
  accepts. A provider that reports a wrong `false` blocks a valid schema, so the
  gate table is part of the conformance corpus.

## Acceptance criteria

These are for the future implementation. None holds today.

- The plugin accepts and refuses each `monitoring` and `dev_realm` shape that
  `internal/ydburl` tests accept and refuse, with refusals that never echo the
  value.
- A differential corpus built from the host's bundled behavior covers the
  attribute grammar and refusals for column tables (the `*-store-column-*` rows in
  the plugin's corpus, today refused by name) and requires the plugin to decode
  every accepted row to the same model and refuse every refused row with the same
  reason.
- Live tests in the plugin's integration suite on 25.1.4.7 and 26.2.1.14: inspect a
  database holding a row table and a column table; create a column table with a
  hash key and a shard count and read both back; plan a second run and get no
  change; change only the tiered TTL and get `RESET` and `SET`; change the shard
  count and get a refusal that names the setting; run without `monitoring` and get
  the parameter-naming refusal.
- A host end-to-end test in `integration/` runs `schema plan`, `schema apply` and
  `migrations up` against a column table through the bundled provider.
- A flag-capability test turns a gating flag on and off on the fixture server and
  reads the changed capability in the session.
- A mutation check: stripping the credential from the monitoring request, or
  following a redirect, makes a named test fail.
- The census of YDB features in `internal/schemacensus` has no column-table row
  that only the built-in engine passes. The census is read by `git ls-files`, with
  a floor.

## Decided questions

- No declared address-parameter list. A refusal at `Open` names the parameter.
- The CI services in both the host workflow
  (`.github/workflows/go-integration-tests.yml`) and the plugin's own CI publish the
  monitoring port. Both are acceptance work for step 3 and step 6 above.

## Amendments

- [ADR 0031](0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md)
  supersedes ADR 0026. Its table replaces the sentence in "Versioning" about the
  pin: no pin moves, and the plugin release raises its `min_contract` if step 6
  needs a newer contract. No distribution carries a provider, so the host
  end-to-end criterion runs through an installed provider rather than a bundled
  one.
- ADR 0031 says Ptah's own pipelines do not run against optional plugins. No
  record decides whether the YDB provider is one, and so whether that host
  end-to-end test, and the host CI service in "Decided questions", run in Ptah's
  pipelines.
