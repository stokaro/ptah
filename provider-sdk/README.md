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
