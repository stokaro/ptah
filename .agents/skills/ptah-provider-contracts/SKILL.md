---
name: ptah-provider-contracts
description: Use when changing Ptah provider selection, host adapters, SDK contracts, feature negotiation, subprocess transport, generated protocol bindings, or shared sqlsyntax APIs. Use for changes spanning the root and nested Go modules or requiring a provider companion release.
---

# Ptah provider contracts

## Establish the module and owner

Run `scripts/list-go-modules.sh` and inspect the affected `go.mod` files. Read
the relevant heading of `provider-sdk/README.md`, which specifies the contract,
and the owning host adapter under `internal/providerhost`. Search for the RPC,
feature, or service name; do not read the entire SDK specification by default.

- `provider-sdk` owns protocol messages, service validation, and authenticated
  subprocess transport. It imports no Ptah application package or driver.
  Providers own engine semantics and their drivers.
- `sqlsyntax` is a separate module for shared lexical syntax. Its README states
  what its scanner and splitter recognize. Keep catalogs, DDL planning, and
  engine execution out of it.
- ADR 0023 owns engine knowledge; ADR 0031 owns separate provider delivery.
  Read their decision sections when ownership changes. Read current versions
  from declarations, not historical examples in an ADR.

## Check what the test actually builds

Root `go test ./...` neither visits nested modules nor substitutes their local
sources for published requirements. Confirm resolution with `go env GOWORK`
and `go list -m -json ptah.run/provider-sdk ptah.run/sqlsyntax` from the host
module before claiming a joint change was tested.

For joint development, use a temporary, uncommitted workspace selecting only
the modules under test. Keep evidence from that development workspace distinct
from `GOWORK=off` builds against published requirements. Do not commit a
`replace`, alter a fixture pin, or change a release version to make local
sources appear published.

`provider-sdk/testdata/fixture` deliberately has an independent published SDK
pin. Host process acceptance uses that pin with `GOWORK=off`; SDK process tests
use their temporary workspace to exercise the in-tree SDK. Preserve both
purposes. A required unreleased SDK change needs a stated release/consumer
dependency; a workspace-only pass does not make the host releasable.

## Carry the contract through each boundary

For a changed request, result, capability, or feature, trace:

1. The source contract and its compatibility rule: major/minor revision,
   required and advertised features, and behavior of a peer lacking the feature.
2. The `.proto` declaration and generated bindings, when wire data changes.
   Use the pinned generation setup and `scripts/check-provider-protocol.sh`;
   never hand-edit generated `.pb.go` files.
3. Server and client validators, including malformed, incomplete, unrelated,
   duplicate, and unknown values that this contract must refuse.
4. Host conversion and the selected runtime service, preserving schema
   knowledge, identity, ownership, errors, and negotiated limits.
5. The production consumer and its fixture/conformance evidence. A service
   tested only in isolation may never be selected by the host.

Negotiate additions before sending data an older peer cannot safely consume.
Do not silently drop required information or treat a missing service as an
empty successful answer. Keep provider recognition separate from permission
to execute it; preserve selection, identity, digest, and handshake checks.

## Prove transport and semantic behavior separately

- Test encode/decode and validation together where data crosses the wire.
  Include a negative control for the omitted or malformed fact the fix addresses.
- For transport changes, cover cancellation, process exit, bounded cleanup,
  session/lock ownership, and uncertain execution outcome as applicable.
  `internal/providerhost/session.go` shows bounded cleanup after a failed open.
- Use the SDK fixture for host-contract and process evidence. Keep real-engine
  SQL, catalog, live execution, and parity tests in the provider repository.
  Do not describe a fixture pass as DuckDB, Spanner, or YDB engine coverage.
- Test each changed module and affected host consumer. Apply the relevant
  module, protocol, and generated-output gates from the current workflows and
  [validation policy](../../references/validation.md).

## State the delivery boundary

Update the affected SDK specification and host documentation. Report whether
the change is in-tree only, works against the published pins, or needs an SDK
release and companion provider change. Identify the required dependency without
claiming that another repository's tests or release have already succeeded.
