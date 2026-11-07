# ADR 0099: The provider contract freezes as v1, and its 25.x minors fold into v1.0

Status: Proposed

Contract: 25.30 (this record proposes 1.0)

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
4 freezes it. By maintainer decision the YDB and Spanner providers gate the
freeze. The DuckDB provider adopts v1 after it
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

### What v1 is on the wire

- The proto package is `ptah.provider.v1`. The Go import path is
  `github.com/stokaro/ptah/provider-sdk/protocol/v1`, package name
  `protocol`, which is the name goimports assumes for a path that ends in a
  major version.
- The contract version is 1.0: major 1, minor 0. A v1.0 host announces
  `host_contract` 1.0, and a provider built with SDK v1.0 describes contract
  1.0 with a minimum of 1.0.
- Renaming the package changes every method path, so the change is
  incompatible by the README's own rule and needs a new major. Major 1 makes
  the package suffix and the major the same number.
- The freeze is the one window in which a cleanup costs nothing. Each Describe
  message carries one version: `DescribeRequest` keeps `host_contract`,
  required, beside `host_features` and `required_features`; `Description`
  keeps `min_contract` and gains `contract`, the version the provider's SDK
  speaks, so a refusal can name both sides. The `revision` fields are
  removed and their numbers and names reserved. The rules that read an absent
  version as minor 0 go with them: every v1 host announces its version.
- The comments that name a 25.x minor are removed. An addition from v1.1 on
  names the minor that added it.
- The bootstrap's application protocol version becomes 2, and from v1 on it
  selects the contract major: bootstrap 2 serves contract major 1. A host
  lists the bootstrap versions it serves, go-plugin picks the highest one
  both sides have, and a later major takes a new bootstrap version. The
  cookie key and value do not change; see the open questions.

### How a pre-v1 provider is refused

Each layer refuses before a database connection opens:

1. Installation and selection. A pre-v1 release declares `min_contract`
   major 25. `Meets` fails on the major, so the host reports
   `provider_incompatible` with the text it already writes, which names both
   versions and says to run `ptah providers install <engine>`. An older Ptah
   reading a v1 release's manifest fails the same check, and its descending
   search installs the newest 25.x release instead.
2. The bootstrap. The v1 host also lists bootstrap version 1. When go-plugin
   negotiates version 1, the host kills the process without dispensing a
   client and returns a typed error that says the executable speaks the
   experimental contract of an SDK older than `provider-sdk/v1.0.0`. It
   calls nothing on that process. A pre-v1 host that starts a v1 provider
   fails its own bootstrap, as any version mismatch does there.
3. Describe. A peer of another major that shares the bootstrap version is
   refused with `provider.ContractError` before `Open`, as now.

No v1 code reads, serves or converts the experimental package.

### What happens to the 25.x minors

v1.0 is the last 25.x contract, under a host that lists every two-way
feature, with everything deleted that exists only for a host that lists
fewer. The rules that sort the 62 two-way features:

1. **The host half goes for all of them.** Every v1.0 host acts on every v1.0
   addition, so `host_features` lists nothing at v1.0, and
   `TwoWayFeatures()` returns only features added from v1.1 on. The SDK
   deletes the narrowing and the feature gates for v1.0 features, and the
   suite drops their narrowed profiles.
2. **A feature name stays where it says something about the provider that
   the rest of the description does not**: a service the host may call, or a
   capability the host chooses a path by. It becomes one-way. There are 34.
3. **A name retires where it only unlocked a field for a host that might not
   read it.** The field is part of v1.0 and its presence is the declaration:
   `Engine.query_dialect` declares a query dialect without
   `query.dialect`. Transport encodings the SDK applies to every message
   retire the same way. There are 22. A retired name is never reused.
4. **A weaker representation goes where a feature selects between two
   forms.** v1.0 keeps only the complete form, and a provider that offers the
   base service must send it: an `Inspect` reply carries complete column,
   primary-key and index observations. There are 6, and an official provider
   that does not list one adopts it before the freeze.
5. **An addition nothing exercises leaves v1.0.** Issue #4204 calls the API
   stable only once the providers exercise it. A feature that neither YDB nor
   Spanner lists at the freeze, and that the host does not act on with one of
   them, is adopted by one of them first or removed: its fields are reserved
   and it returns in a later minor when something uses it. At the commits
   measured, `schema.common-view-facets` (which the host does not list
   either), `lint.transaction-owner` and `engine.linked-release-lines` (which
   the DuckDB provider uses) are in that position. The four 25.29 features
   are too, until the Spanner provider adopts them.

[Appendix A](#appendix-a-the-62-two-way-features-at-v10) classifies each
feature. The 25.31 additions are sorted by the same rules when they land.
Rule 5 makes the DuckDB provider's adoption of v1 depend on a minor that
returns `engine.linked-release-lines`, unless the maintainers keep it on the
SDK fixture's evidence.

The README's release table restarts at `provider-sdk/v1.0.0` with contract
1.0. The 25.x rows leave the README; the tags stay.

### Compatibility from v1 on

Within a major, a minor may add:

- an RPC, a message, a field with a new number, an enum value, or a feature;
- a two-way feature where the addition changes an existing message, and a
  one-way feature where it adds a service;
- a validator that checks only what the same minor adds.

A change needs the next major when it:

- removes, renumbers or retypes a field, renames it in the JSON mapping (the
  conformance fixtures use the Protobuf JSON mapping), or gives it another
  meaning;
- makes a field required, makes a one-way feature mandatory, or makes a
  validator refuse what an older peer of the same major sends;
- changes a default, an outcome, the meaning of a status code a host acts on,
  the bootstrap, or the rule by which a model identity is computed.

A deprecated field stays honored until the next major. Within a major, a field
number or name is never reused.

A host keeps negotiating every minor of its major for the life of that major.
A host at 1.N starts any provider whose minimum is 1.N or lower, and a provider
of a later minor negotiates down to what the host lists. Dropping a minor is a
change of major.

Across majors, v2 is the package `ptah.provider.v2`, a new bootstrap version
and the SDK module path `github.com/stokaro/ptah/provider-sdk/v2`. From the
first Ptah release that speaks v2, the host keeps serving v1 for at least 12
months and two Ptah minor releases, by listing both bootstrap versions. A
provider may register both majors during that window.

Model identities carry across the freeze unchanged where a provider's
definition is unchanged, because the digest reads the provider's canonical
JSON and nothing of the wire. Where the freeze changes a definition, the
provider's first v1 release names the 25.x identity as a predecessor of its
own model. The host keeps no reader for 25.x. Each provider records the model
identities of its first v1 release
(`ptah-provider-conformance models`), and each later release passes the
suite's `models.recorded` check against them. A plan saved under 25.x is
refused by a v1 host on the major, as the plan binding already decides.

### The SDK module at v1.0.0

- The module path stays `github.com/stokaro/ptah/provider-sdk` and the tag is
  `provider-sdk/v1.0.0`. The adoption window uses pre-release tags,
  `provider-sdk/v1.0.0-rc.N`, so the host and the providers pin a tag
  throughout, as the README requires.
- From v1.0.0 the module follows semantic import versioning. Packages
  `protocol/v1`, `provider`, `runtime`, `conformance` and `modeljson` make no
  incompatible change within v1. The flags, exit codes and JSON report fields
  of `cmd/ptah-provider-conformance` are stable and grow only by addition.
  `internal/...`, `integration` and `testdata` promise nothing.
- SDK releases and contract minors stay separate numbers. A release that
  changes only Go API raises the SDK minor and keeps the contract, and the
  README's table maps each release to the contract it speaks.
- Before v1.0.0 an API review decides each of the 146 `provider` functions
  with no caller outside the SDK: kept, with the reason a Go host other than
  Ptah or a Go provider needs it, or unexported. The constants of retired
  features are deleted.
- An apidiff gate for the SDK module compares its exported packages, found
  with `go list` and leaving out `internal`, with the newest
  `provider-sdk/v1.*` tag. It fails on any incompatible change and has no
  approval file: a break needs `/v2`. `docs/public_api.md` stays the root
  module's ledger and points to the SDK README for the SDK's promise.

### Cross-version tests

1. **The baseline.** `provider`, `lint` and `contract-host` build against
   `provider-sdk/v1.0.0` from then on, in place of `v0.2.0`, and keep to the
   v1.0 API. The in-tree host runs against the v1.0 provider and the v1.0
   host against the in-tree provider, in `go-module-tests.yml` on every pull
   request.
2. **The suite across versions.** The conformance command built at
   `provider-sdk/v1.0.0` checks a provider of the in-tree SDK, and the in-tree
   suite checks the v1.0 fixture provider.
3. **Protocol fixtures.** Encodings of v1.0 messages recorded at v1.0.0
   (a description, a describe request, an inspect request and its schema, a
   render result, an execution) sit under `provider-sdk/testdata/contract-1.0`.
   Every later SDK decodes and validates them unchanged.
4. **Refusals.** An executable that answers bootstrap version 1, built with
   no SDK dependency, is refused by name before `Describe`. A wire provider
   that describes major 2 is refused before `Open`. A provider of the in-tree
   SDK refuses a host that announces 2.0.
5. **The providers.** Each provider's CI runs the suite from three SDK
   releases: its pin, the release that matches its `min_contract`, and the
   newest v1 release, offline in the unit job and live in the live job. The
   live legs gain the oldest Ptah release that meets the provider's minimum,
   beside the newest release and host master.

The suite reports the contract it checks against. A provider's release
attaches the JSON report, and a report of another major fails at the
handshake check.

### Wording

| Where | Now | At v1 |
| --- | --- | --- |
| `providerhost/session.go`, `statementargs` (6 sites) and their tests | the experimental provider contract does not support X | the provider contract does not support X |
| `provider.ValidateDescription` | expected experimental revision 25 | names both contract versions |
| `provider-sdk/README.md` | Experimental provider SDK | Provider SDK |
| Package and bootstrap comments in the SDK | experimental | dropped |
| `docs/site/.../extend/executable-providers.md` | a trusted experimental provider | a trusted provider |

The `--provider-config` help, "(experimental JSON format)", stays: the
trusted manifest is Ptah's own configuration, which stays pre-1.0, not part
of the contract.

### Release order

1. Entry: 25.31 merged and tagged; the gaps marked "before the freeze" in
   [the open contract gaps](#the-open-contract-gaps) closed or deferred by
   name; both providers on the newest 25.x minor;
   both list the 6 mandatory features of Appendix A, which today means
   `schema.index-nulls-order` and `schema.index-predicates` for YDB and
   `schema.index-predicates` and `values.context` for Spanner; the gates of
   Appendix B green or waived by name.
2. An SDK-only pull request makes the change. The host and the providers keep
   their pins. A maintainer tags `provider-sdk/v1.0.0-rc.1`.
3. Draft pull requests in both providers pin rc.1, with `.github/host-ref`
   naming the host adoption branch. The host adoption pull request pins rc.1
   and changes the host. Fixes go to rc.2 and later.
4. The host adoption merges. Host master speaks 1.0. The providers drop
   `host-ref` and merge. Each provider's `scripts/resolve-host.sh` compares a
   release's SDK with `Revision == 25`, and moves to the major the provider
   describes. Until step 6 no Ptah release speaks 1.0, so the release legs
   find nothing and skip, and the master legs, which do not block, are the
   only host runs.
5. A maintainer tags `provider-sdk/v1.0.0` on the commit the providers
   validated. The host and the providers move their pins to it.
6. The providers release with `min_contract` 1.0, then Ptah releases.
   An installed 25.x provider then reports `provider_incompatible` with the
   install command.
7. The DuckDB provider adopts v1 in
   [stokaro/ptah#5667](https://github.com/stokaro/ptah/issues/5667).

### The open contract gaps

The maintainers' rule is to close what can be closed before the freeze, and
to let the freeze wait only for a gap that cannot be closed by addition
later. The gaps below come from the DuckDB provider's list
(`contract-gaps-next.md`, items 5 to 8), stokaro/ptah#5647, and the YDB and
Spanner gap registers. Contract 25.31 is adding three more now: observed
UNIQUE constraints over several columns, the conformance fix for offline
calls that need a two-way feature, and how a provider engine builds an enum
column (stokaro/ptah#5658).

| Gap | What it touches | Additive later? | Recommendation |
| --- | --- | --- | --- |
| A table owned with no table setting (DuckDB, item 5) | `ModelPlanning`: a planning service owns a table only through a table-attached model, so DuckDB declares a `table-lifecycle` model that never holds a value | Yes: a `ModelPlanning` declaration that the service owns table lifecycles | Next minor, before the freeze, so v1 does not keep a placeholder model as the only spelling. Not blocking |
| No collation aspect (DuckDB, item 6) | `ColumnAspect` has type, default, nullability and sequence; DuckDB compares collation inside `COLUMN_TYPE` | Yes, under a two-way feature, but `COLUMN_TYPE` would then include collation for one host and leave it out for another, the shape rule 4 removes | Next minor, before the freeze, and mandatory at v1.0 for an engine with collations. Not blocking |
| Removals ignore observed foreign keys (DuckDB, item 7) | The host orders table removals by declared dependencies and chains the steps; observed constraints are on the wire (25.28) | Yes: host planning only, and the contract promises no removal order | Host fix at any time; DuckDB's single `drop-tables` step stays valid. Not blocking |
| `live.locks` needs a second process (DuckDB, item 8) | SDK `conformance`: the check opens the database from a second process, which an embedded engine refuses | Yes: a `conformance` configuration field, Go API only | SDK minor before DuckDB adopts v1 (stokaro/ptah#5667). Not blocking |
| The view check-option level (stokaro/ptah#5647) | Desired `SQLView.with_check` is a bool; observed `ObservedView.check_option` is a string | Yes: an enum beside the bool under a two-way feature | Fold into the freeze: replace the bool with an enum of none, `CASCADED` and `LOCAL`, which the rename makes free. Not blocking |
| View queries compared with the host's case folds (G204) | No engine field states how a view query compares; the host folds unquoted text | Yes: an engine declaration under a two-way feature | Next minor or v1.x. Not blocking |
| A transfer into an undeclared table or topic (G211) | `CompareModelsRequest.declared_relations` carries views only, by the host's choice, and no comment says what it holds | Only as a new field: once frozen, another kind in this list reaches a provider that assumed views | Before the freeze: state that the list holds every declared relation, each with its kind, and have the host fill it so |
| A `docker://` dev URL in context resolution (G215) | `DatabaseContextRequest.address` receives the dev URL raw; no rule says which side resolves `docker://` | Yes: the host resolving the address first changes nothing a provider relies on | Before the freeze, write that the host resolves a dev URL before asking for its context. Not blocking |
| Omissions of a rendering (G219, G220) | No field reports a declared fact a rendering leaves out, such as a type modifier or sequence settings | Yes: a list on the render result under a two-way feature | Next minor or v1.x. Not blocking |
| The server's message on a failed execution (S74) | `Execution` carries the provider's reason and SQLSTATE, no redacted server text | Yes: a new field | Decide whether operators need it; v1.x if so |
| Change streams (S16, S47) | No model the planner or the export sees | Yes: a native context kind through `planning.object-context`, or a field on the observed table | Provider work. Not blocking |

None of these gaps needs what only a new major could do, so the freeze waits
for none of them. Four are cheaper before it than after it: the check-option
enum, the collation aspect, the table-lifecycle declaration and the meaning
of `declared_relations`.

### Documentation the freeze changes

`provider-sdk/README.md` (title, Module boundary, Versions, Contract versions,
the release table, Bootstrap specification, Conformance suite),
`docs/site/src/content/docs/extend/executable-providers.md`,
`docs/site/src/content/docs/extend/write-a-provider/` (the tutorial and SDK
reference pin `v0.26.0` and link `protocol/experimental`),
`examples/minimal-provider`, `docs/public_api.md` (one pointer), the
amendments of ADRs 0021, 0022, 0023 and 0031, and each provider's README and
limitations page.

## Alternatives considered

**Keep major 25 and declare it stable**, as the amendments of ADRs 0021 to
0023 describe. No method path changes. The package name keeps "experimental"
in every generated identifier and method path for the life of the contract,
and the fold removes behavior a 25.x peer relies on, so the major changes in
any case.

**Major 26 in package `ptah.provider.v1`**, so that a pre-v1 major (25) is
numerically older. A refusal could then say "older" truthfully. The package
suffix and the major would differ, which Protobuf's package versioning
convention does not expect, and nothing reads the order of two different
majors: a refusal can name both without ordering them.

**Keep every feature two-way at v1.0.** Renaming alone costs least now. It
freezes 62 narrowed host profiles, 8 inconsistent combinations on YDB and the
weaker representations for the whole major.

**Make every feature mandatory.** Every provider would implement embedding
stores, dev realms and index renames, which issue #4204 lists as optional
services.

**Keep bootstrap version 1 and detect a pre-v1 provider by `Unimplemented`
from `Describe`.** It changes less. The host cannot tell a pre-v1 provider
from a later v2-only one, it makes a call before refusing, and a major has no
negotiation point at launch.

**Freeze with Ptah 1.0.** One stability date is simpler to communicate. It
ties the providers' contract, released on its own cadence (ADR 0031), to the
stability of the host's commands and configuration. See the open questions.

**One version number for the SDK and the contract** (SDK v1.N speaks 1.N). A
Go-only SDK release would need an empty contract minor.

## Consequences

- Every consumer changes its import path once: 167 host files outside the
  SDK, 457 inside, 642 in the YDB provider and 93 in the Spanner provider.
  The YDB provider's refusals that print a field's full name, such as
  `ptah.provider.experimental.DeclaredTable.constraints`, change with the
  package, and so do the tests that pin them.
- A pre-v1 provider does not start under a v1 host. The refusal names the
  install command.
- The SDK loses the narrowing code and the suite its narrowed profiles for
  v1.0 features. Both mechanisms remain for features added from v1.1 on.
- From v1.0.0 a mistake in the contract is fixed by addition or in v2. The
  release-candidate window and the API review exist for that reason.
- New gates: the SDK apidiff gate, the suite from three SDK releases in each
  provider's CI, and a live leg against the oldest Ptah release that meets a
  provider's minimum.
- An official provider that lacks a mandatory feature, or a gate in Appendix
  B, delays the freeze.

## Open questions

1. Is contract v1 independent of Ptah 1.0, as proposed, or do they ship
   together?
2. Major 1, as proposed, or major 25 declared stable?
3. The cookie value `ptah-provider-experimental-1`: keep it, so that a pre-v1
   provider reaches the version check and is refused by name, or rename it to
   a token without "experimental" and accept a generic bootstrap failure for
   a pre-v1 executable started directly?
4. Are `provider-sdk/v1.0.0-rc.N` tags acceptable for the adoption window?
5. Appendix A: confirm each class, in particular `schema.index-nulls-order`,
   `schema.index-predicates` and `values.context` as mandatory and
   `database.context` as optional. Under rule 5, remove
   `schema.common-view-facets`, `lint.transaction-owner` and
   `engine.linked-release-lines` from v1.0, or keep one on the SDK fixture's
   evidence?
6. The window for an older major: 12 months and two Ptah minor releases?
7. Should validators stay exported for Go hosts other than Ptah, or move
   behind the SDK's client and server?
8. Between steps 4 and 6 the providers' release legs skip and their master
   legs do not block, and on YDB the 25.1 line then runs nothing. Make the
   master legs blocking for that window, or keep the window to one day?
9. Does the freeze wait for the Spanner gaps in Appendix B, or ship with
   them named as waivers?

## Appendix A: the 62 two-way features at v1.0

"Retired" removes the name; the field or encoding stays in v1.0. "Mandatory"
retires the name and requires the complete form from a provider that offers
the base service. "One-way" keeps the name as a provider or engine
declaration. The last two columns say whether the provider lists the feature
at the commits measured above.

| Feature | At v1.0 | YDB | Spanner |
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
provider within its declared scope. "Host and SDK" lists what the contract's
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
- Missing before the freeze: v1.0 baseline programs and protocol fixtures; the
  bootstrap 1 refusal; the suite from three SDK releases in both providers,
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
