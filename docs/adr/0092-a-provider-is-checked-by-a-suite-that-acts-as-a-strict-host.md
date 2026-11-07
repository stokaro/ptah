# ADR 0092: A provider is checked by a suite that acts as a strict host

Status: Proposed

Contract: 25.28 (no change of its own)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0059](0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md),
[ADR 0075](0075-a-provider-names-the-identities-its-models-had-before.md),
[ADR 0082](0082-a-call-validates-its-request-once-on-each-side.md),
[ADR 0084](0084-a-captured-claim-names-a-providers-model-by-the-identity-the-provider-owns.md)

## Context

Issue #4204 asks for "a reusable conformance suite sufficient to build a
provider outside this repository", shipped "as a consumable SDK
component/command with machine-readable results". Until now the contract was
enforced only while a host ran: the SDK validates each request and reply on
both sides of the process boundary, and each provider repository carried
process tests of its own. The developer guide said so in as many words.

Those runtime checks hide part of what a provider sends:

- The SDK's client never shows a status message, and shows a reason only when
  the host negotiated `errors.reasons`. A provider outside the SDK that sends a
  reason to a host that did not negotiate it, or a reason that quotes a
  credential, passes every host check.
- The SDK's server removes such reasons and refuses a reply that uses an
  addition the host did not list. For a provider built on the SDK, that defect
  reaches the host as a failed call, which a test sees only if it calls the
  service under a host that does not list the feature.
- `runtime.Start` sends one `Describe`, with this SDK's version and the
  features its caller lists. A test built on it cannot send a host of another
  major, a malformed feature list, or a host of contract 25.0.

Measured with the suite this record introduces, on the YDB provider at
stokaro/ptah-ydb-plugin 111c04cc (YDB 25.1.4.7 and 26.2.1.14):

- Of the 51 two-way features the provider offers, 8 leave a description that
  `ValidateDescription` refuses when a host lists every two-way feature but
  that one: `planning.statement-style` without `planning.policy`, password
  inputs without `units.sensitive-inputs`, and so on. A host that lists such a
  set is inconsistent, and the provider refuses it in `Describe`.
- The provider refuses `Inspect` with `FailedPrecondition` and its own reason
  when the host does not list `schema.observed-columns`. That refusal is
  deliberate: its unit test says a recent contract alone does not opt a host
  into complete column observations.
- Forcing the planning context inventory into every filtered read, a field of
  `planning.context-inventory`, is invisible under a host that lists no
  two-way feature, because the refusal above comes first. It shows only under
  a host that lists every feature but that one, where the provider's own check
  of its reply answers `Internal`.
- Eight concurrent catalog reads overload local YDB's system views
  (`concurrent scans limit exceeded`) in one run of five.

## Decision

### The suite is a package and a command of the SDK

Package `provider-sdk/conformance` holds the suite, and
`provider-sdk/cmd/ptah-provider-conformance` runs it against an executable and
prints the report, as text or as JSON. Neither imports a Ptah package, so a
provider repository runs it from the SDK version its `go.mod` pins, and a
provider in another language runs the command.

### It speaks the raw wire

The suite dials the provider itself and calls each RPC with the generated
client. It runs, on each reply, the validator the SDK's client runs, with the
description both sides negotiated, and resolves model references and shared
coverage as the client does. It reads each refusal's status, details
included. A target is how it reaches the provider: the executable, started by
the bootstrap `runtime.Start` uses, which moves to the internal package
`internal/bootstrap` so that both share it; a `runtime.Server`, served by the
new `runtime.ServeInMemory` through the checks, interceptors and limits
`Serve` applies; or a `protocol.ProviderServer` with no SDK server in front of
it. Each check that needs a negotiation of its own connects again, as each
start of an executable is a new process.

### It acts as several hosts

The suite describes the provider under a host that lists every two-way
feature, one that lists none, one that announces no contract, and, for each
two-way feature the provider offers, one that lists every feature but that
one. A description must equal the full one narrowed by
`provider.NegotiatedDescription`, or, where the narrowed one is a description
`ValidateDescription` refuses, the provider must refuse the host. Each host
that lists every feature but one also sends the author's offline calls and,
with a database, a read, so that a reply using the one feature it left out
is found where a host of no features never reaches it.

### Engine knowledge comes from the author

The suite knows no engine. Requests to offline services, changes to apply and
revert, units the database refuses, the address of a database the suite may
change, an address that carries the suite's canary as its credential, and the
model identities of the last release come from the configuration. The offline
part runs with none of them and with no database.

### What it requires beyond the validators

Where the contract states a property the validators cannot check alone, the
suite checks it, and each check names what it requires
(`conformance.Checks`):

- An offline service answers the same request with the same reply, twice in
  one process and once in another.
- A unit the database refuses is `NOT_APPLIED` with nothing applied, and a
  confirmed prefix is what the database then holds.
- A lock excludes a session of another process, a lock ID is used once,
  releasing an unused ID fences it, and closing a session releases its locks.
  A transaction echoes its isolation, a finish repeats its stored result, a
  rollback of an unused ID fences it, and closing a session rolls back.
- A reason reaches only a host that negotiated `errors.reasons`, and no
  reason, result message or session field quotes the address, a credential in
  it, or the canary.
- A desired or observed model identity an earlier release described still
  reads, as the model's identity or as a predecessor. An operation or a
  change binds to the release that planned it, so a later release owes it no
  reading.

Under a narrowed host a provider may refuse what it cannot serve without the
features left out, as the YDB provider refuses `Inspect`. A refusal with
`Internal`, or one whose message is the text of `provider.ErrProtocol`, is
not such a refusal: it is how the SDK's server answers a reply its validator
refuses, and how a provider answers a defect of its own.

### The report is the interface

`Check` returns a `Report` that marshals to JSON: the contract, the target,
the provider's identity, and one result per check in a fixed order. `Run`
reports each check that applies as a subtest and logs the rest rather than
skipping them, because Ptah's test contour fails a skipped test.

### No contract change

The contract is unchanged. `runtime.ServeInMemory`, package `conformance` and
the command are Go API, so they ship in the next SDK tag and raise its minor
version.

## Alternatives considered

**A suite in the host repository.** It could reuse Ptah's own provider host.
A provider module cannot import the host, which is the boundary #4204 draws,
so a provider repository could not run it.

**A suite built on `runtime.Client`.** It needs no new SDK surface, and every
call goes through the checks a host makes. It cannot send a host of another
major or a malformed feature list, and the client discards the status
details that show a reason leaking. It would test the client as much as the
provider.

**A public raw client in `runtime`.** Exporting the dial or a raw connection
from `Client` would give the suite what it needs from the runtime alone. It
would also hand every host a way around the validators, for the benefit of a
test tool. The internal bootstrap package and an in-memory server keep that
path inside the SDK.

**Deep checks of each service.** The suite could plan, compare and decode with
built-in fixtures. Every such fixture is engine knowledge, which belongs to
the provider. The suite instead drives the author's requests and changes
through the validators.

## Consequences

- A provider author has one command to run before a release, and its report
  can gate continuous integration. The minimal provider example and the YDB
  provider run it, offline in their unit jobs and against a database in their
  live jobs.
- Each two-way feature a provider offers costs one provider start; the suite
  runs eight at a time and reads one database at a time. The YDB provider's
  offline run takes about 30 seconds, with the race detector.
- The probe of every RPC is a table, and a white-box test holds it equal to
  the generated service, so a new RPC fails that test until it has a probe.
- The suite is stricter than the validators in the places listed above. A
  provider that answers a refused statement with `UNKNOWN` passes the
  validators and fails `live.refused`, which a provider author opts into by
  naming a refused unit.
- Telling a refusal the provider meant from a reply the SDK refused reads the
  status message for the text of `provider.ErrProtocol`. A provider that puts
  that text in a refusal of its own under a narrowed host fails the check.
