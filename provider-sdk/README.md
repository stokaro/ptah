# Experimental provider SDK

This module is for developers implementing Ptah subprocess providers. It defines
explicit protocol messages, service interfaces, and authenticated local transport.
Its Go module path is `github.com/stokaro/ptah/provider-sdk`.

The contract is experimental. A different protocol revision is refused before a
session opens. No stable SDK release or compatibility with earlier experimental
revisions is promised. [Issue #4204](https://github.com/stokaro/ptah/issues/4204)
owns integration with the application and extraction of the official providers.

## Module boundary

The SDK imports no Ptah application package or database driver. Providers own
their drivers and may build with a different dependency graph from the host.
The independent fixture module under `testdata/fixture` uses SQLite through its
own driver. Its local `replace` is development wiring, not a published dependency.

`protocol/experimental` contains generated Protobuf messages and the gRPC service.
`provider` defines service contracts and validates complete results.
`runtime` starts and serves authenticated subprocesses. The runtime does not
compare schemas, order migration dependencies, or maintain migration history.

## Service contract

| Operation | Required result |
| --- | --- |
| `Describe` | Exact revision, provider identity, semantic identity, engines, identifier comparison rules, and advertised features. |
| `Open` | A nonempty session handle for the requested canonical engine. Credentials belong in this request, never in process arguments. |
| `Close` | Release one session. Repeated closes succeed. |
| `Inspect` | A complete snapshot with explicit dispositions for observed fields. Incomplete state is an error, never an empty database. |
| `Render` | One executable unit for every input change, in input order. A refused batch exposes no executable prefix. |
| `AnalyzeSQL` | Complete owner-defined statement boundaries, tokens, and explicit safety dispositions. Unsupported syntax refuses the whole batch. |
| `DecodeSource` | Complete offline interpretation of captured extension fragments, frozen model scope, and original source traces. |
| `ConvertValues` | An offline conversion between desired and observed models with preserved identity and a complete normalized result. |
| `CompareModels` | A complete offline comparison of captured objects or facets, independent coverage sources, common context, and typed changes. |
| `PlanFeatures` | Complete offline graph contributions and ordered change and parent receipts, or diagnostics with no successful output. |
| `NormalizeValues` | A complete offline batch of owner-validated values, structured normalized subjects, and versioned canonical projections. |
| `Execute` | A terminal outcome and the number of consecutive units confirmed applied. Request IDs do not enable automatic retries. |

Common records represent tables and columns explicitly. Native records use a
versioned provider-owned envelope. Structured references keep database, schema,
path, kind, name, and overload components separate. The transport does not fold
names or concatenate components into identity strings.

The fixture accepts a narrow table grammar. It refuses views, triggers, indexes,
and table definitions it cannot reconstruct exactly. Its coverage does not
establish support for any official engine.

## Execution uncertainty

`APPLIED` confirms the whole batch. `NOT_STARTED` and `NOT_APPLIED` confirm no
applied units. `PARTIAL` confirms a proper prefix. `UNKNOWN` leaves the final
effect unresolved and may carry a confirmed prefix.

A lost response, cancellation after dispatch, malformed execution reply, or
provider crash reports `ErrOutcomeUnknown`. The SDK does not reconnect or replay
the request. The caller must inspect actual state before deciding how to proceed.
Cancellation before dispatch sends nothing. Killing a provider does not prove
that a database transaction rolled back.

## Trust and process lifetime

`runtime.Start` requires an absolute executable path, its SHA-256 checksum, and
an expected provider identity. The caller authorizes that executable. A checksum
detects changed bytes; it does not establish publisher authenticity.

The runtime selects an explicit environment and refuses reserved bootstrap names
and duplicate names, including case variants. It does not search the working
directory or `PATH` for providers. Subprocesses have the caller's OS privileges;
they are not a security sandbox.

Mutual TLS authenticates the local gRPC connection. The bootstrap cookie only
identifies the expected launch protocol. Messages are limited to 4 MiB in each
direction. Startup defaults to ten seconds; service calls are limited to thirty
seconds or the caller's earlier deadline. Raw provider logs and RPC error text
are discarded because they may contain credentials.

The context passed to `Start` bounds startup. The returned client owns the process
until `Close`, which is safe to call repeatedly. Session handles become invalid
when the process exits. The provider also watches its parent and exits when the
parent dies, including when the parent cannot run deferred cleanup.

## Bootstrap specification

The experimental bootstrap uses the gRPC transport of
[go-plugin v1.8.0](https://github.com/hashicorp/go-plugin/tree/v1.8.0).
Its control services are part of this experimental bootstrap specification.
A non-Go implementation needs those control services and Ptah's
[`Provider` service](protocol/experimental/provider.proto).

The host supplies `PTAH_PROVIDER_BOOTSTRAP=ptah-provider-experimental-1`, protocol
version `1`, `PTAH_PROVIDER_PARENT_PID`, and `PLUGIN_CLIENT_CERT` containing the
client certificate in PEM format. The provider requires the client certificate.
The provider authenticates it, generates its server certificate, and writes one
bootstrap line to standard output:

```text
1|1|network|address|grpc|server-certificate
```

`network` is the local listener's network, such as `unix` or loopback `tcp`.
`address` is that listener's address. `server-certificate` is the DER certificate
encoded with unpadded standard Base64. The host pins this certificate. The selected
provider service name is `ptah-provider`. Subsequent standard output belongs to
the transport's standard-I/O service and must not add bootstrap records.

The bootstrap also implements go-plugin's health, shutdown, broker, and standard-I/O
services. The pinned upstream
[`grpc_*` protocol files](https://github.com/hashicorp/go-plugin/tree/v1.8.0/internal/plugin)
define those control messages. Application traffic uses only gRPC and Protobuf;
net/rpc and gob are not supported.

## Validation

From the repository root, the existing contour runner executes every SDK process
test and refuses skipped tests:

```bash
go run ./internal/cmd/testcontour --dir provider-sdk --tags integration --race
```

The tests build the fixture in its own module with `GOWORK=off`. They exercise
inspect, render, execute, and inspect again; partial execution; crashes and
cancellation after committed writes; environment isolation; message limits;
checksum and identity refusal; and parent death.

This SDK-level evidence does not establish a completed native CLI workflow.
That acceptance condition belongs to #4204's application integration.

Each engine declares its offline identifier comparison rules. Each opened session
reports the effective rules and default schema. Missing rules, unknown comparison
modes, and unknown index namespaces are refused. The experimental contract
supports exact and ASCII-insensitive comparison; it makes no claim about other
collation semantics. Hosts must not infer these rules from a built-in target.

The experimental application adapter currently requires each engine to declare
`schema.common-table-identity.v1`. This promises common table and column comparison
semantics without implicit schema objects, undeclared facets, additional creation
defaults, or type coercions. A provider must refuse declarations outside that
contract. A missing feature does not select a built-in implementation. Engines
with additional semantics need corresponding protocol operations before this
adapter can support them.

`AnalyzeSQL` accounts for every source byte with a statement span or whitespace.
It preserves source tokens, canonical tokens, and line numbers. A provider refuses
comments or syntax it cannot represent. `COMMON_DDL` permits the host's common DDL
assessment; it does not mean safe. Native behavioral and destructive effects carry
explicit reasons. `UNKNOWN` remains destructive for approval policy. Unknown
numeric dispositions, incomplete batches, gaps, and overlapping spans are errors.

A failed process launch or transport bootstrap wraps `runtime.ErrStartup`.
A rejected protocol revision, service contract, or provider identity wraps
`provider.ErrProtocol`. Cancellation and deadline errors preserve their context
sentinels. A caller can distinguish these failures from a missing installation.

## Owner-defined value normalization

The `values.normalize` service uses a `ValueNormalizer` that validates
and normalizes a complete batch without a session or connection address. Inputs
identify their desired, observed, operation, or change representation. A provider
refuses representations and payload fields it cannot interpret.

A normalized envelope retains its input owner, kind, schema version, encoding,
definition digest, and required features. The response supplies a separate normalized subject and
an owner-defined canonical format and projection. The client and server check
ownership, feature selection, ordered correlation IDs, and complete results.
A failed or canceled call exposes no usable prefix.

Canonical projections are representation-local. Bind them to the negotiated
provider semantic identity, engine, target features, representation, kind,
schema version, model definition digest, and canonical format. Never substitute raw payload bytes or
serialized Protobuf for that owner-defined contract. A projection does not
replace contextual desired/observed comparison or authorize a migration.

The independent fixture normalizes a synthetic retention value offline. It
preserves integers beyond the exact range of floating-point numbers, distinguishes
omission from null and zero, and refuses unknown or duplicate fields. The fixture
SQL service does not support retention. The host captures YAML declarations and
observed objects and facets, and dispatches contextual comparison to their owner.
Native planning and migration backend operations remain required work in #4204; see [ADR 0022](../docs/adr/0022-provider-values-are-resolved-in-semantic-batches.md).

## Contextual model comparison

The optional `models.compare` service uses a `ModelComparer`. Each engine names
its comparison services and assigns model kinds, change kinds, and either native
object placement or common facet owner kinds. Object services also declare their
supported explicit actions. A model has one comparison owner per engine.

`CompareModels` receives desired and observed states, identifier rules, capability
facts, parent lifecycles, and the context relevant to its placement. Object
context includes explicit actions, declared relations, roles, columns, required
objects, exclusions, and the database path. Facet context includes common
constraint definitions. A missing capability differs from an explicit false.

Bound identity components retain source and normalized spelling, quoting, and
connection defaults. Parent, namespace path, and signature remain separate.
Coverage retains each independent source and its outside and bounded claims.
Sources intersect; a complete region cannot supply another source's missing
knowledge. Empty values do not establish absence. An excluded facet retains its
model, subject, and target binding, and permits no change or diagnostic.

The owner validates complete payload semantics again. Captured canonical
projections do not prove desired/observed equivalence. Results include effective
desired schema, normalized `CHANGE` values, and explicit undecided diagnostics.
Both transport boundaries refuse incomplete replies and unowned models. The
host's comparison runtime enforces common migration policy before planning.

Normalization of a change requires explicit replacement and state-removal facts,
impact and access assessments, and reasons. Unknown impact and access remain
explicit assessments. Required object identities are structured values. These
facts must be reconstructed from the complete payload when it is decoded;
persisted metadata cannot stand in for owner validation. Planning and execution
of native changes require their own services and are not supplied by comparison.

## Desired and observed conversion

The optional `values.convert` service uses a `ValueConverter` alongside
`values.normalize`. It receives one complete batch, its source and destination
representations, and the captured target features. No session is needed.

The owner interprets source payloads again. Supplied canonical projections are
context, not proof of validation. Destination schema versions, encodings,
definition digests, payloads, and canonical formats follow the destination model.
The ordered IDs, model kinds, source identities, and normalized identity tuples
must remain unchanged. Required features may change only within the selected
profile. An error, cancellation, or incomplete response discards the whole batch.

The host registers conversion only for advertised services and paired desired
and observed models. A batch mixing captured engine or feature profiles is
refused. Conversion cannot establish inspection coverage. The shared conversion
pipeline retains bounded knowledge, object identities, and facet placement;
unknown namespaces remain unknown. Clone, encoding, and equality remain local.

This service does not resolve schema selectors or implement contextual
comparison and planning. Inspection binds selectors separately through the
owner's namespace declarations.

## Captured extension sources

Experimental revision 13 carries `source.decode`, complete `ValueModel` definitions,
and explicit YAML key ownership.
It refuses earlier revisions. The SDK has no compatibility decoder or built-in
fallback. A `SourceDecoder` receives complete captured documents without a
session, connection address, host AST, or callback. The host evaluates frontend
expressions and enforces file access policy before capture.

Each document explicitly enrolls desired models from the selected engine's
negotiated description. A model identifies its kind, representation, schema
version, encoding, SHA-256 definition digest, canonical projection format, and
accepted source formats. The full definition is a nonempty JSON object
normalized by `modeljson.Canonical`. The SDK verifies its exact SHA-256 digest
and refuses duplicate keys, lossy strings, or a changed definition. Number text
remains exact. Protobuf bytes and value payloads are not model definitions.
These checks bind the declared format; they do not prove engine semantics.

Each YAML-capable engine declares `yaml_source_keys`. A claim names the key,
its `DOCUMENT` or `TABLE` scope, a decoder selector, and the desired model kinds
it can declare. The SDK refuses duplicate keys within a scope, missing or
unsupported models, and a YAML-capable model with no key claim. The host must
also refuse collisions with common frontend keys or another selected owner.
A model's JSON definition and kind name do not imply grammar ownership.

Every YAML fragment carries the exact negotiated `yaml_key` claim. Its selector
must match, and its document must enroll every model the claim names. A table
key needs a common table binding with an entry key; a document key has no
implicit table attachment. The client and server validate these facts before
owner code runs. Other source formats cannot carry a YAML claim. Grammar claims
are metadata, not a promise that the host has connected every frontend adapter.

Captured values preserve missing entries, explicit null, false, empty values,
exact number text, expressions, and structured references. Mapping keys,
block attributes, child blocks, and labels retain their separate namespaces.
Bindings carry common object identity and frontend naming. The owner receives
all bindings before interpreting fragments, so declaration order need not limit
reference resolution. It must interpret every supplied field or refuse the
whole batch, including inherited attributes it requested from the host.

A successful response contains a result for every document and fragment in
input order. Every emitted object or facet names an enrolled model, provides
its normalized value and canonical projection, and has matching subject
coverage. Duplicate normalized identities are refused. Coverage retains the
original model scope; empty scope never means all kinds in the provider's
current registry. Complete, absent, defaulted, uninspected, and unrepresentable
knowledge are distinct. Absence and defaults need a specific subject; observed
coverage cannot request defaults. An incomplete source result is refused.

Source traces name an original document and fragment and follow typed keys,
indices, attributes, child blocks, or labels. Coordinates use zero-based offsets into the original encoded bytes and
one-based lines and byte columns in that encoding. A BOM contributes bytes to
the first column; UTF-16 source is not silently re-encoded. Authored values require
positions; inherited defaults can lack an authored position. A missing path is
an error, distinct from an inherited default with no position. Validation reads
no files, checks bounds and cycles, and limits depth and batch size.

The independent fixture exercises evaluated YAML, HCL, and Go annotation
fragments. It accepts a synthetic retention mapping or HCL block, resolves its
`on` binding, and preserves exact integers. It refuses SQL and does not apply
retention to SQLite. The host YAML frontend captures negotiated document and
table keys for this RPC. The host HCL and Go annotation adapters, official-engine
adapters, native planning, transactions, and history
remain part of #4204; this transport is not a claim of YDB source support.

## Observed model coverage

`Inspect` returns its engine, selected target features, and explicit observed
model coverage even for an empty database. The host matches the engine and
features to the inspected session. A missing coverage record is not evidence
that a model has no objects.

`provider.InspectionValues` validates native objects, table facets, and column
facets against their negotiated model definitions and complete subject records.
It returns an independent batch for `NormalizeValues`. Native objects use their
model kind as their identity kind. Table facets name their table, and column
facets use `provider.ColumnReference` to keep the table and column names as
separate components.

After normalization, `provider.ValidateNormalizedInspection` requires every
normalized owner to match its coverage subject. A failure discards the entire
snapshot. Hosts retain the engine, target features, model contract, and
normalized subject when capturing the owner-defined canonical projection.

Each model declares knowledge outside its namespace regions and within each
region. Database, catalog, and schema qualifiers preserve source and normalized
spelling. An absent qualifier is a wildcard; a present empty qualifier matches
only an empty component. Path prefixes retain whole components. Regions belong
to the operation's selected engine. An uninspected or unrepresentable region
requires a reason and overrides overlapping completeness. Explicit subject
records take precedence. Duplicate normalized regions, invalid components, and
namespace-wide absence or defaults are refused. A model can carry at most
4,096 regions.

The host requires the exact session database on bindings and coverage regions.
It retains native coordinates and maps common table and column regions to shared
identities. Subject-kind limits and exact empty paths keep these coordinate
spaces separate. A column path becomes its parent table; unknown parent regions
remain unknown after this mapping. Unbound, foreign, or renamed databases and
inconsistent common identifier normalization cause refusal.

A filtered inspection returns one `scope_bindings` entry per requested selector,
in request order. Each entry maps that selector to structured owner namespaces.
An empty namespace list means an empty selection, not global authority. Bindings
never grant coverage. Every returned table has a `normalized_identity`, even
without facets. Tables, coverage subjects, and whole coverage regions must fit
inside the bound namespaces. Filtered replies cannot claim global completeness
or include dispositions without normalized identities. Requests and bindings
are limited to 4,096 entries, with at most 4,096 namespaces across all bindings.

Desired and observed models may declare a plural `display_name` for omission
reports. It must be trimmed single-line text and agree across engines that share
a model representation. The host freezes this label in the contract and reports
captured values locally, without interpreting payloads or declaring count metrics.
An absent label does not get a host-invented substitute.

Native `schema inspect --format json` returns common schema data and warns about
omitted objects and facets using these labels. It is not a lossless model export.
Shared conversion and codecs retain the captured payloads and bounded coverage.
Full-document source decoding still requires unrestricted complete model
knowledge and refuses regions. Native migration planning and faithful source
export remain work in #4204.

## Captured native operations

An `OPERATION` model is distinct from desired values and comparison changes.
Normalization reconstructs its impact and access assessments from the complete
payload. Hosts can clone and persist captured operations without reaching the
provider; decoding requires owner normalization again.

A native `Render` input carries the captured operation and its grammatical role.
A table alteration also carries its actual parent. The owner renders a complete
execution unit, including the table statement. Capability facts preserve a known
false value separately from a missing answer. Providers must revalidate payload
semantics and refuse any role or context they cannot implement.

Rendering validates the whole batch at both transport boundaries. Cancellation,
foreign models, incomplete replies, and changed provenance discard all output.
The independent operation fixture executes table creation, renaming, and removal
through this contract. It does not establish a feature-planning, transaction, or
history service; these remain required before official-engine extraction.

## Contextual feature planning

`models.plan` advertises `PlanFeatures`. Each engine assigns a service its change,
parent, and operation model kinds. Parent assignment is independent of changed
facets: a table operation must account for attached state even when no setting
changed.

The request captures declaration and inspection records separately, including
common children, normalized feature values, exclusions, and independent coverage
sources. Missing observations do not establish absence. Identifier components,
capability values, and common graph step identities retain their original
boundaries. An established false capability is distinct from a missing fact.

A successful reply accounts for every input change and every assigned parent
model. Each contributed operation must appear in a receipt. Dependencies may
refer to common steps; explicit rewrites name the common operations they replace.
The host must validate ownership, rewrite effects, and the complete graph before
using a contribution. The SDK never orders a partial graph or treats a receipt
as evidence of execution.

Transaction requirements, operation placement, phases, and conservative impact
assessments travel as explicit metadata. Unknown values remain unknown. A
completed refusal contains diagnostics and no operations, rewrites, or successful
receipts. Cancellation and malformed replies discard the complete batch.

The host registers these services and binds their ownership into captured model
identities. Its adapter preserves common definitions, normalized feature values,
coverage sources, graph metadata, and receipts. Unmapped common properties and
conflicting primary-key facet slots are refused before the RPC.

An independent SDK-only fixture exercises change planning through the selected
runtime, shared graph, rendering, statement execution, and inspection. It also
checks captured ALTER parents, graph conflicts, completed refusals, and
cancellation. The test caller explicitly executes without a transaction.

The common migration planner still needs to dispatch these services and retain
transaction requirements through execution. Parent receipts and rewrites still
need independent-process acceptance. This work remains part of #4204; the
transport does not qualify YDB extraction or the migration history backend.
