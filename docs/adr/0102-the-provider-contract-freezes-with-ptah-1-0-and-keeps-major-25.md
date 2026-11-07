# ADR 0102: The provider contract freezes with Ptah 1.0 and keeps major 25

Status: Accepted

Contract: 25.31 (the freeze keeps major 25)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204),
item 6; tracked in [stokaro/ptah#5686](https://github.com/stokaro/ptah/issues/5686)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0031](0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md),
[ADR 0075](0075-a-provider-names-the-identities-its-models-had-before.md),
[ADR 0084](0084-a-captured-claim-names-a-providers-model-by-the-identity-the-provider-owns.md),
[ADR 0092](0092-a-provider-is-checked-by-a-suite-that-acts-as-a-strict-host.md)

## Context

Issue #4204 asks for a stable, versioned provider protocol and a public Go
SDK. The contract stays experimental while the providers are built, and Phase
4 freezes it. This record was proposed as a freeze into a new major 1, ahead
of Ptah 1.0; the maintainers decided otherwise, and the proposal is recorded
under the alternatives. By maintainer decision the YDB and Spanner providers gate the
freeze. The DuckDB provider adopts the frozen contract after it
([stokaro/ptah#5667](https://github.com/stokaro/ptah/issues/5667)).

The facts below were measured at host `31a08baed` (SDK at contract 25.30),
`stokaro/ptah-ydb-plugin` `870cd65c` and `stokaro/ptah-spanner-plugin`
`5e810c5`.

**The wire.** `provider-sdk/protocol/experimental/provider.proto` declares
package `ptah.provider.experimental` and Go package
`github.com/stokaro/ptah/provider-sdk/protocol/experimental`. It has 78 RPCs,
388 messages and 4,575 lines. 113 comments name the 25.x minor that added a
call or a field, and two `reserved` statements exist. `provider.Revision` (25)
is the major and `provider.ContractMinor` (30) the minor. `DescribeRequest`
carries the major twice, as `revision` and as `host_contract.major`, and
`Description` carries `revision` and `min_contract`. The README maps 27 SDK
tags, `provider-sdk/v0.2.0` to `v0.28.0`, to contracts 25.0 to 25.30.

**The features.** The SDK names 92 features: 62 two-way
(`provider.TwoWayFeatures()`) and 30 one-way. The host lists 61 of the 62 in
`internal/providerhost/host.go`. It leaves out `schema.common-view-facets` on
purpose: a view's own settings have no place on a provider's engine
(`internal/providerhost/sql_schema_model.go`). Three mechanisms exist only for
a host that lists fewer: the narrowing in `provider.NegotiatedDescription`,
the per-feature gates in the validators, and the conformance suite's narrowed
host profiles, one for each two-way feature a provider offers. On the YDB provider, 8 of 51
narrowed profiles leave a description `ValidateDescription` refuses, and the
provider refuses `Inspect` to a host that does not list
`schema.observed-columns` (ADR 0092).

**The bootstrap.** `provider-sdk/internal/bootstrap` uses the go-plugin v1.8.0
handshake with application protocol version 1 and the cookie
`PTAH_PROVIDER_BOOTSTRAP=ptah-provider-experimental-1`. Any launch failure
returns `bootstrap.ErrBootstrap`, so a host cannot say why a launch failed.
go-plugin selects a protocol version from the versions the client lists
(`PLUGIN_PROTOCOL_VERSIONS`), and the client exposes the result as
`Client.NegotiatedVersion`.

**Selection.** `providerrelease.Meets` compares majors exactly. Installation
(`internal/providerinstall/install.go`) and selection of a stored
installation (`internal/providerconfig/resolve.go`) call it before an
executable starts. The descending manifest search skips a release whose
minimum the host does not meet.

**The consumers.** `protocol/experimental` is imported by 167 files of the
host repository outside the SDK, 457 inside it, 642 in the YDB provider and 93
in the Spanner provider. The YDB provider pins `provider-sdk/v0.26.0`
(contract 25.28) with a minimum of 25.7 and lists 51 of the 62 two-way
features. The Spanner provider pins `v0.25.0` (25.28) with a minimum of 25.27
and lists 19. The DuckDB provider pins `v0.27.0` (25.29).

**The SDK's Go surface.** Package `provider` exports 264 functions, 20
methods, 49 types and 126 constants and variables, 92 of them feature names.
`runtime` exports 4 functions, 80 methods and 3 types; `conformance` 7
functions and 13 types; `modeljson` one function. 146 of the 264 `provider`
functions have no non-test caller in the host, either provider or
`examples/minimal-provider`, and 117 of those have no caller outside package
`provider`. `docs/public_api.md` and its gates cover the root module only.

**Model identities.** `ValueModel.definition_sha256` is the SHA-256 of the
provider's canonical JSON definition (`modeljson.Canonical`,
`provider/models.go`). It does not depend on the Protobuf package or encoding.
A saved plan binds the provider identity, its release version and the
contract major (ADR 0022, amendments).

**Cross-version tests.** `provider-sdk/integration/contract_e2e_test.go`
builds the fixture programs `provider`, `lint` and `contract-host` against
`provider-sdk/v0.2.0` and runs them against the in-tree SDK in both
directions. `go-module-tests.yml` runs that contour for every nested module.
ADR 0031 keeps Ptah's own pipelines off plugins: each provider repository runs
live legs against the newest Ptah release whose contract meets its minimum,
blocking, and against host master, not blocking. The YDB provider runs the
conformance suite as a Go test, offline in its unit job and live in its live
job. The Spanner provider does not, and cannot at its pin: the suite first
ships in `provider-sdk/v0.26.0`. Neither provider tests the archive its
release builds; both test a `go build -race` executable.

**Wording.** Six production sites in `internal/providerhost/session.go`,
`statementargs/statementargs.go` and `statementargs/typed.go` refuse with
"the experimental provider contract does not support ...". The Spanner
provider's `docs/limitations.md` quotes that text. The SDK README is titled
"Experimental provider SDK", and `ValidateDescription` refuses with "expected
experimental revision".

**Earlier records.** The amendments of ADRs 0021, 0022 and 0023 say that "v1"
there means the Ptah 1.0 release and that major 25 "is stable from Ptah 1.0
on". Issue #4204 keeps the application version and the protocol version
apart and names a protocol v1 with a `protocol/v1` package.

## Decision

The maintainers decided three questions this record put to them.

1. **The contract freezes together with Ptah 1.0.** Until that release the
   contract stays 25.x, and a minor may change it freely, an incompatible
   change included. Ptah owes no compatibility with its own earlier behavior
   before 1.0, and the contract follows that rule.
2. **The frozen major is 25.** The freeze adds no `ptah.provider.v1`
   package, no `protocol/v1` import path and no contract 1.0. The proto
   package `ptah.provider.experimental`, the Go import path
   `github.com/stokaro/ptah/provider-sdk/protocol/experimental` and the
   bootstrap, application protocol version 1 with the cookie
   `PTAH_PROVIDER_BOOTSTRAP=ptah-provider-experimental-1`, stay as they are.
   Ptah 1.0 declares the newest 25.x minor stable. This is what ADRs 0021,
   0022 and 0023 already say: major 25 is stable from Ptah 1.0 on.
3. **No support window is promised for an older major.** When a later major
   exists, how long a host keeps serving major 25 is decided then.

Because the major does not change, nothing has to refuse a provider of an
earlier major at the freeze: a provider of an older 25.x minor negotiates by
minor, as today, and `providerrelease.Meets` keeps comparing majors exactly.

### What freezing means

From Ptah 1.0 on, a minor of major 25 may add:

- an RPC, a message, a field with a new number, an enum value, or a feature;
- a two-way feature where the addition changes an existing message, and a
  one-way feature where it adds a service;
- a validator that checks only what the same minor adds.

A change needs a new major when it:

- removes, renumbers or retypes a field, renames it in the JSON mapping (the
  conformance fixtures use the Protobuf JSON mapping), or gives it another
  meaning;
- makes a field required, makes a one-way feature mandatory, or makes a
  validator refuse what an older peer of the same major sends;
- changes a default, an outcome, the meaning of a status code a host acts on,
  the bootstrap, or the rule by which a model identity is computed.

A deprecated field stays honored for the life of the major. Within a major, a
field number or name is never reused. A host keeps negotiating every minor of
its major for the life of that major. This record does not decide the shape
of a later major.

Until Ptah 1.0 these rules are a direction, not a constraint: a minor that
breaks one says so in its record, as the minors before the freeze do.

### What must be true at the freeze

The rest of this record is the checklist for the freeze, prepared when Ptah
1.0 is. Each item is decided or done by then.

- **The features.** [Appendix A](#appendix-a-the-62-two-way-features-at-the-freeze)
  sorts the 62 two-way features of contract 25.30 by four rules: the host
  half of the negotiation goes, a name stays where it declares a service or
  a capability, a name retires where it only unlocks a field, and a weaker
  representation goes where a feature selects between two forms. Folding the
  features is an incompatible change, which is allowed before Ptah 1.0 and
  not after. The freeze decides whether it folds them and which, starting
  from that classification. Features added since 25.30 are sorted by the
  same rules.
- **The open contract gaps.** The gaps [below](#the-open-contract-gaps) are
  closed, or deferred by name to a minor after the freeze.
- **The release gates.** The gates of [Appendix B](#appendix-b-release-gates)
  are green for the YDB and Spanner providers, or waived by name.
- **The SDK's Go surface.** An API review decides each exported `provider`
  function that no caller outside the SDK uses: kept, with the reason a Go
  host other than Ptah or a Go provider needs it, or unexported. The SDK
  module gains an apidiff gate from the release the freeze tags, and its
  README says which packages are stable: at least the protocol package,
  `provider`, `runtime`, `conformance` and `modeljson`, and the conformance
  command's flags, exit codes and JSON report fields.
- **Cross-version tests.** Fixture programs built against the SDK release
  of the freeze run against the in-tree SDK in both directions on every pull
  request; recorded encodings of that release's messages are decoded and
  validated by every later SDK; each provider runs the conformance suite from
  its pin, from the release that matches its `min_contract`, and from the
  newest release, with a live leg against the oldest Ptah release that meets
  its minimum.
- **Model identities.** Each provider records the model identities of its
  first release at the freeze (`ptah-provider-conformance models`), and each
  later release passes the suite's `models.recorded` check against them.
- **The wording.** The freeze decides whether "experimental" leaves the
  refusals that say "the experimental provider contract does not support X",
  the SDK README, the package comments and the executable-providers page.

### The open contract gaps

The maintainers' rule is to close what can be closed before the freeze, and
to let the freeze wait only for a gap that cannot be closed by addition
later. The gaps below come from the DuckDB provider's list
(`contract-gaps-next.md`, items 5 to 8), stokaro/ptah#5647, and the YDB and
Spanner gap registers. Contract 25.31 closes three others: observed UNIQUE
constraints over several columns, the conformance fix for offline calls that
need a two-way feature, and how a provider engine builds an enum column
(stokaro/ptah#5658).

| Gap | What it touches | Additive later? | Recommendation |
| --- | --- | --- | --- |
| A table owned with no table setting (DuckDB, item 5) | `ModelPlanning`: a planning service owns a table only through a table-attached model, so DuckDB declares a `table-lifecycle` model that never holds a value | Yes: a `ModelPlanning` declaration that the service owns table lifecycles | Contract 25.32, so the frozen contract does not keep a placeholder model as the only spelling. Not blocking |
| No collation aspect (DuckDB, item 6) | `ColumnAspect` has type, default, nullability and sequence; DuckDB compares collation inside `COLUMN_TYPE` | Yes, under a two-way feature, but `COLUMN_TYPE` then includes collation for one host and leaves it out for another, a feature that selects between two forms | Contract 25.32, under a two-way feature; the freeze decides whether it becomes the only form for an engine with collations. Not blocking |
| Removals ignore observed foreign keys (DuckDB, item 7) | The host orders table removals by declared dependencies and chains the steps; observed constraints are on the wire (25.28) | Yes: host planning only, and the contract promises no removal order | Host fix at any time; DuckDB's single `drop-tables` step stays valid. Not blocking |
| `live.locks` needs a second process (DuckDB, item 8) | SDK `conformance`: the check opens the database from a second process, which an embedded engine refuses | Yes: a `conformance` configuration field, Go API only | SDK change at any time, before DuckDB adopts the frozen contract (stokaro/ptah#5667). Not blocking |
| The view check-option level (stokaro/ptah#5647) | Desired `SQLView.with_check` is a bool; observed `ObservedView.check_option` is a string | Yes: an enum beside the bool under a two-way feature | Contract 25.32: an enum of none, `CASCADED` and `LOCAL` beside the bool, under a two-way feature. Not blocking |
| View queries compared with the host's case folds (G204) | No engine field states how a view query compares; the host folds unquoted text | Yes: an engine declaration under a two-way feature | Contract 25.33: the engine's owner decides whether two view queries are one (`CompareViewQueries`, ADR 0108). Not blocking |
| A transfer into an undeclared table or topic (G211) | `CompareModelsRequest.declared_relations` carries views only, by the host's choice, and no comment says what it holds | Only as a new field: once frozen, another kind in this list reaches a provider that assumed views | Contract 25.32: state that the list holds every declared relation, each with its kind, and have the host fill it so under a two-way feature. Contract 25.33 hands a comparison the declared objects of another comparison's models, such as topics (ADR 0109) |
| A `docker://` dev URL in context resolution (G215) | `DatabaseContextRequest.address` receives the dev URL raw; no rule says which side resolves `docker://` | Yes: the host resolving the address first changes nothing a provider relies on | Before the freeze, write that the host resolves a dev URL before asking for its context. Not blocking |
| Omissions of a rendering (G219, G220) | No field reports a declared fact a rendering leaves out, such as a type modifier or sequence settings | Yes: a list on the render result under a two-way feature | Contract 25.33: `RenderResult.omissions` under `render.omissions` (ADR 0110). Not blocking |
| The server's message on a failed execution (S74) | `Execution` carries the provider's reason and SQLSTATE, no redacted server text | Yes: a new field | Decide whether operators need it; a later minor if so |
| The lexical rules of a provider engine (stokaro/ptah#5754) | The shared lexer reads a provider engine's text by the SQL standard alone, and `sql.scan` makes the host refuse the engine's SQL schema documents | Yes: a field on the query dialect under a two-way feature | Contract 25.34: `QueryDialect.lexical` under `query.lexical` (ADR 0115). Not blocking |
| Where a connection reads refined capabilities (stokaro/ptah#5770) | The host names YDB's monitoring endpoint by comparing the dialect's name | Yes: an engine field under a two-way feature | Contract 25.35: `Engine.connection_capability_source` under `engine.connection-capability-source` (ADR 0116). Not blocking |
| Export omission counts (stokaro/ptah-ydb-plugin#206) | An export counts one omission per value, though a value can hold several objects | Yes: report fields under a two-way feature | Contract 25.36, under `models.omission-counts`. Not blocking |
| View queries a server rewrites (S91) | Under `schema.view-query-equivalence` the owner answers every pair from the two texts; the request carries no declared columns and no answer returns a pair to the host's fold | Yes: a request field and a reply field under a two-way feature | Contract 25.37: `CompareViewQueriesRequest.declared_columns` and `ComparedViewQuery.undecided` under `schema.view-query-context` (ADR 0118). Not blocking |
| A transfer of an undeclared changefeed (G228) | `declared_kinds` names object models only, so a comparison cannot see another comparison's facets, such as a table's changefeeds | Yes: a comparison field and a request field under a two-way feature | Contract 25.37: `ModelComparison.declared_facet_kinds` and `CompareModelsRequest.declared_facets` under `models.declared-facets` (ADR 0119). Not blocking |
| Change streams (S16, S47) | No model the planner or the export sees | Yes: a native context kind through `planning.object-context`, or a field on the observed table | Provider work. Not blocking |

None of these gaps needs what only a new major could do, so the freeze waits
for none of them. Four are cheaper before it than after it, and contract
25.32 adds them as negotiated additions: the check-option enum, the collation
aspect, the table-lifecycle declaration and the meaning of
`declared_relations`. Contract 25.33 closes three more as negotiated
additions: view query equivalence, the declared objects of another
comparison's models, and the omissions of a rendering. It also adds the
grants of a held table to planning (ADR 0111), the password a SQL document
gives a role (ADR 0112) and the statement facts of a SQL grammar
(ADR 0113), which the gap registers found after this record was written.

Later minors add one negotiated addition each:

- contract 25.34, the lexical rules of a provider engine's text (ADR 0115);
- contract 25.35, where an engine's connection reads refined capabilities
  (ADR 0116);
- contract 25.36, how an export counts the objects it leaves out of a reported
  model.

## Alternatives considered

**Freeze now as a new major 1, independent of Ptah 1.0.** This was the
proposal:

- The proto package would have become `ptah.provider.v1`, the Go import path
  `provider-sdk/protocol/v1`, and the contract 1.0. Each Describe message
  would have carried one version, without the duplicate `revision` fields.
- The bootstrap would have moved to application protocol version 2. A host
  would have listed version 1 only to refuse, by name and before dialing it,
  a provider built with an SDK older than the freeze.
- The 25.x minors would have folded into 1.0 under a host that lists every
  two-way feature, and the SDK would have shipped as `provider-sdk/v1.0.0`
  after release candidates.

It gives the contract a stability date of its own, on the providers' release
cadence (ADR 0031), and removes "experimental" from every generated
identifier. Renaming the package changes every method path and every import:
167 files of the host outside the SDK, 457 inside it, 642 in the YDB provider
and 93 in the Spanner provider. The maintainers chose to tie the freeze to
Ptah 1.0 and to keep major 25.

**Major 26 in package `ptah.provider.v1`**, so that a pre-v1 major would be
numerically older. The package suffix and the major would differ, which
Protobuf's package versioning convention does not expect.

**Keep every feature two-way at the freeze.** It costs least. It freezes 62
narrowed host profiles, 8 inconsistent combinations on YDB and the weaker
representations for the whole major. The freeze decides this with Appendix
A.

**Make every feature mandatory.** Every provider would implement embedding
stores, dev realms and index renames, which issue #4204 lists as optional
services.

**A promised support window for an older major**, such as 12 months and two
Ptah minor releases from the first Ptah release of a later major. The
maintainers promise none.

## Consequences

- The protocol package, the import paths of the host and the providers, the
  bootstrap and the contract major do not change at the freeze. No consumer
  changes an import path, and no installed provider is refused for its major.
- Until Ptah 1.0 a minor may still change the contract incompatibly, so a
  provider adopts each minor as it lands, as today.
- The freeze carries the work of the checklist above: the feature
  classification, the gates of Appendix B, the API review, the apidiff gate
  and the cross-version tests. An official provider that lacks a gate of
  Appendix B delays the freeze or is waived by name.
- Contract 25.32 closes the four gaps that are cheaper before the freeze.

## Open questions

The maintainers answered questions 1, 2 and 6 of the proposal with the three
decisions above. The proposal's question on the bootstrap cookie, and the one
on the window in which the providers' release legs would skip, do not arise
when the major stays 25. These are decided when the freeze is prepared:

1. Is the SDK release that the freeze tags a release candidate first?
2. Appendix A: does the freeze fold the features, and are
   `schema.index-nulls-order`, `schema.index-predicates` and `values.context`
   mandatory and `database.context` optional? Are the features no gating
   provider exercises removed or kept on a provider's evidence?
3. Should validators stay exported for Go hosts other than Ptah, or move
   behind the SDK's client and server?
4. Does the freeze wait for the gaps of Appendix B, or ship with them named
   as waivers?

## Appendix A: the 62 two-way features at the freeze

The classification the freeze starts from, written for the proposal that
folded the features. "Retired" removes the name; the field or encoding stays.
"Mandatory" retires the name and requires the complete form from a provider
that offers the base service. "One-way" keeps the name as a provider or
engine declaration. "Rule 5" marks a feature that neither gating provider
lists, which the proposal removed until a provider uses it. The last two
columns say whether the provider lists the feature at the commits measured
above.

| Feature | At the freeze | YDB | Spanner |
| --- | --- | --- | --- |
| `transport.large-messages` | retired, always on | yes | no |
| `transport.shared-coverage` | retired, always on | yes | no |
| `models.references` | retired, always on | yes | no |
| `errors.reasons` | retired, always allowed | yes | yes |
| `source.encode-groups` | retired, always allowed | yes | no |
| `source.go-placements` | retired, always allowed | yes | no |
| `query.dialect` | retired, `Engine.query_dialect` declares it | sets the field | no |
| `engine.default-capabilities` | retired, `Engine.default_capabilities` | yes | no |
| `engine.linked-release-lines` | retired, `ReleaseLine.linked`; rule 5 | no | no |
| `scope.selections` | retired, `Engine.scope_selections` | yes | no |
| `schema.database-kinds` | retired, `Engine.database_kinds` | yes | no |
| `models.predecessors` | retired, `Engine.model_predecessors` | yes | no |
| `models.own-predecessors` | retired, a predecessor of the same kind | yes | no |
| `source.hcl` | retired, `Engine.hcl_source` | yes | yes |
| `source.hcl-platform` | retired, the `TABLE_PLATFORM` site | yes | yes |
| `source.index-kinds` | retired, `Engine.index_kinds` | yes | no |
| `source.sql-table-clauses` | retired, `SQLSchemaDeclaration.table_clauses` | no | no |
| `source.entry-keys` | retired, the claims' scopes | yes | no |
| `source.limits` | retired, `Engine.go_header_kinds` and the claims | yes | no |
| `schema.role-scope-reasons` | retired, the reason field | yes | no |
| `planning.table-outlines` | retired, `ModelPlanning.tables` | yes | no |
| `schema.index-methods` | retired, `PreparedTable.indexes` | yes | no |
| `schema.observed-columns` | mandatory with `schema.inspect` | yes | yes |
| `schema.observed-primary-keys` | mandatory with `schema.inspect` | yes | yes |
| `schema.observed-indexes` | mandatory with `schema.inspect` | yes | yes |
| `schema.index-nulls-order` | mandatory with `schema.inspect` | no | yes |
| `schema.index-predicates` | mandatory with `schema.inspect` | no | no |
| `values.context` | mandatory with `values.normalize` | yes | no |
| `embeddings.store` | one-way service | yes | no |
| `schema.common-objects` | one-way service | yes | yes |
| `models.reverse` | one-way service | yes | yes |
| `models.report-counts` | one-way service | yes | yes |
| `units.migration-split` | one-way service | yes | yes |
| `schema.columns` | one-way service | yes | yes |
| `units.sensitive-inputs` | one-way service | yes | no |
| `schema.declarations` | one-way service | yes | no |
| `database.context` | one-way service | yes | no |
| `units.statement-timeout` | one-way capability | yes | no |
| `database.dev-realms` | one-way capability | yes | no |
| `lint.statement-facts` | one-way capability | yes | yes |
| `lint.transaction-owner` | one-way capability; rule 5 | no | no |
| `planning.index-renames` | one-way capability | yes | no |
| `planning.primary-key-changes` | one-way capability | yes | yes |
| `planning.constraint-changes` | one-way capability | no | yes |
| `planning.lifecycle` | one-way capability | yes | yes |
| `planning.object-context` | one-way capability | yes | no |
| `planning.policy` | one-way capability | yes | yes |
| `planning.statement-style` | one-way capability | yes | yes |
| `planning.context-inventory` | one-way capability | yes | no |
| `render.text-units` | one-way capability | yes | no |
| `models.compare-counts` | one-way capability | yes | no |
| `sessions.typed-arguments` | one-way capability | yes | no |
| `schema.password-inputs` | one-way capability | yes | no |
| `schema.unread-tables` | one-way capability | yes | no |
| `schema.subtree-coverage` | one-way capability | yes | no |
| `schema.observed-constraints` | one-way capability | no | yes |
| `schema.observed-index-facets` | one-way capability | yes | no |
| `schema.common-access` | one-way capability | yes | no |
| `schema.common-view-facets` | one-way capability; rule 5, and the host does not list it | no | no |
| `schema.common-namespaces` | one-way capability | no | no |
| `schema.reserved-seeds` | one-way capability | no | no |
| `schema.sql-facets` | one-way capability | yes | no |

## Appendix B: release gates

Issue #4204, section 9, makes these gates a condition of the freeze for each
provider within its declared scope. They were measured at the commits named
in the context, before contract 25.31. "Host and SDK" lists what the contract's
own tests prove for every provider; the provider entries list what each
provider proves for its engine.

### Interoperability

- Host and SDK: in `contract_e2e_test.go`, a host of the in-tree SDK against
  a 25.0 provider and a 25.0 host against the in-tree provider; suite checks
  `handshake.other-major`, `handshake.required-features`,
  `negotiation.description`;
  `TestProvidersInstallAReleaseThatNeedsTheContractThisPtahSpeaks`;
  `provider_incompatible` in `internal/providerconfig`
- YDB: `TestConformanceOfflineSubprocess`, `TestConformanceLive`; 379 host
  tests on the release and master legs;
  `TestDescriptionRefusesAnotherMajor_FailurePath`
- Spanner: release and master legs; `TestInspectionSubprocess_FailurePath`
  under narrowed hosts
- Missing before the freeze: baseline programs and protocol fixtures of the
  freeze release; the suite from three SDK releases in both providers,
  and in Spanner at all; a leg against the oldest Ptah release that meets the
  minimum

### Round trip

- Host and SDK: `TestIndependentProviderProcess` (inspect, render, execute,
  verify); `TestEncodeSourceConformanceRoundTrips`;
  `examples/minimal-provider` `TestReadRenderExecute_HappyPath`
- YDB: `TestHostAppliesTheRoundTripSchema_HappyPath`;
  `TestHostReplansAnExportedReadSubprocess`;
  `TestHostReadsAnEarlierProviderHCLExport_HappyPath`;
  `TestNativeFamiliesRoundTrip_HappyPath`
- Spanner: `TestHostAppliesAndConvergesE2E` and the convergence tests of
  tables, constraints, views, sequences and row deletion;
  `TestContent_ReadsBack`
- Missing before the freeze: Spanner: an export read back as the desired
  schema with no diff, and a service-level encode and decode round trip. YDB:
  one schema through YAML, Go, HCL and SQL with no diff between them

### Malformed input

- Host and SDK: about 30 runtime tests that discard malformed or untrusted
  replies, such as `TestComparisonClientDiscardsPartialAndUntrustedReplies`;
  the message bound in `TestIndependentProviderProcess`;
  `TestCanonicalJSON_RefuseAmbiguousOrLossyInput`
- YDB: `TestDifferential_FailurePath`;
  `TestHostReadsTheBuiltinEnginesHCLExports_FailurePath`;
  `TestSchemaValidationOfflineSubprocessOfManyTables_FailurePath`;
  `FuzzAnalysisFraming`
- Spanner: `TestCapture_FailurePath`; `TestDecode_FailurePath`;
  `TestOpen_FailurePath`; host refusals before any statement
- Missing before the freeze: Spanner: oversized messages, unknown model or
  payload versions. SDK: no fuzz target reads a reply through the validators

### Incomplete inspection

- Host and SDK: `TestIndependentProviderProcess` (an incomplete read refuses
  comparison input); `TestProviderInspectionRefusalDiscardsWholeCatalog`;
  `TestProviderUnreadTablesCompare_FailurePath`
- YDB: `TestWalkRecordsWhatItDoesNotDescribe`;
  `TestAccessFailureReturnsNoPartialProjection`;
  `TestSecretComparisonWithholdsTheRemovalOfAnUnreadSecret`;
  `TestHostLeavesWhatADotPrefixedDirectoryHoldsAlone_FailurePath`
- Spanner: the read refuses rather than returning part
  (`TestInspectionSubprocess_FailurePath`);
  `TestCompare_AnUnknownReadIsUndecided`
- Missing before the freeze: Both: a read under an account that cannot see
  part of the database, which plans no drop

### Missing plugin

- Host and SDK: `TestMissingOfficialProviderDiagnosticsAcrossNativeCommands`;
  `TestNativeVerbsReportAMissingProviderInTheirJSON`;
  `TestProviderSetupLinksMatchPublishedDocumentation`;
  `internal/cli/atlas/compat_provider_test.go`;
  `internal/cli/mcp/provider_test.go`;
  `TestSelect_DisabledManifestEntryIsNeverReplaced`;
  `TestProviderMigrationsRefuseUntrustedExecutableBeforeConnection`
- YDB: host-owned
- Spanner: host-owned
- Missing before the freeze: The language server: `internal/ptahls` has no
  provider test. The sequence "run without the plugin, follow the link,
  install, run again" is not one test

### Crash and cancellation

- Host and SDK: `TestIndependentProviderProcess` (a crash after a committed
  write is unknown, the provider ends when its host is killed);
  `TestIndependentProviderEmbeddingStoreCrashAfterCommit`;
  `TestIndependentProviderLockDeathAndLostReceipt`; about 40 cancellation
  tests
- YDB: `TestCancellationBetweenUnitsPreservesConfirmedPrefix`;
  `TestExecutionStoppedByADeadlineIsUnknown`;
  `TestLockLossDuringCommitRetainsUnknown`;
  `TestHostTransportFailureLeavesTheOutcomeUnknown_FailurePath`
- Spanner: canceled lint runs; `TestLocksSubprocess_LeaseEnds`
- Missing before the freeze: Everywhere: a panic in a provider handler, a
  malformed or stalled handshake, a provider killed during `Execute`. Spanner:
  cancellation before and during `Execute` and after a possible commit

### Ambiguous execution

- Host and SDK: `TestIndependentProviderProcess` (a partial result keeps the
  confirmed prefix); `TestClientFinishDoesNotRetryUncertainOutcome`;
  `TestIndependentProviderHistoryLostReplyRequiresReconciliation`;
  `TestProviderHistoryLostReceiptKeepsTheHostOutcomeUnknown`
- YDB: `TestExecutionDistinguishesRefusalFromUnknownDispatch`;
  `TestUncertainCommitCannotBeRetriedOrReclassified`;
  `TestHostRefusesToResumeOverAnEditedPrefix_FailurePath`; suite
  `live.refused`
- Spanner: `TestExecutionSubprocess_FailurePath` (the prefix read back);
  `TestHostRecordsAFailedMigrationE2E`; the `--tx-mode all` refusal
- Missing before the freeze: Spanner: an `UNKNOWN` outcome, a repeated request
  ID, the aborted-transaction path, resuming after `PARTIAL`. YDB: the
  provider process lost during `Execute`

### Fresh installed package

- Host and SDK: `internal/providerinstall` against a release fixture
  (`TestInstall_HappyPath_NewestCompatible`,
  `TestInstall_ReplacesACorruptInstallation`);
  `TestProvidersInstallFromAMirror`;
  `TestNativeVerbsServeAnInstalledProviderFromTheUserStore`
- YDB: GoReleaser snapshot, checksums and manifest archives checked in
  `goreleaser-check`; manifest tests
- Spanner: the same as YDB
- Missing before the freeze: Everywhere: `ptah providers install <engine>`
  from a published release on a clean machine, then a command through that
  executable; paths with spaces, symlinks, a first use offline. Neither
  provider runs its archived executable. Releases carry checksums and no
  signature
