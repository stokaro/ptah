# ADR 0101: The SDK server's refusal of a request is not the provider's

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Amends: [ADR 0092](0092-a-provider-is-checked-by-a-suite-that-acts-as-a-strict-host.md)

## Context

The conformance suite sends each offline call a provider author lists in
`Config.Calls` to a host that lists every two-way feature, to a host that
lists none, and to each host that lists every feature but one. Under a
narrowed host it accepts a refusal the provider meant, and holds two kinds of
refusal against the provider: `Internal`, and a message that starts with the
text of `provider.ErrProtocol`. Both are how the SDK's server refuses a reply
its validator rejects.

The SDK's server also uses that text for a request. It validates a request
with the description it negotiated, before any provider code runs, and refuses
one that needs what the host did not list with `InvalidArgument` and the
validator's error. A call to `PrepareCommonObjects`, which needs
`schema.common-objects`, therefore fails under a host that does not list that
feature:

```text
PrepareCommonObjects failed under a host that does not list schema.common-objects:
InvalidArgument: invalid provider protocol exchange: common objects: the wire was not negotiated
```

The provider never saw the request, so such a call cannot be listed. The
DuckDB provider found this with SDK v0.26.0 and v0.27.0.

## Decision

Under a narrowed host the suite runs the SDK's request validator on the call's
request with that host's negotiated description, which is the description the
SDK's server validates with. Where the validator refuses the request and the
provider answers `InvalidArgument` with exactly the validator's text, the
refusal is the SDK server's, and the call passes under that host.

Every other refusal is classified as before. A reply the server refuses is
`Internal`, and a refusal a provider writes in its own words has another text,
so both still fail the call.

## Alternatives considered

**Skip a call under a host whose description refuses its request.** A strict
host's client would not send such a request. The suite would then no longer
check that a provider outside the SDK refuses a request that uses an addition
the host did not list.

**Accept any `InvalidArgument` under a narrowed host.** A provider that
answers its own defect with `InvalidArgument` and the protocol error's text
would pass.

**Mark the server's refusal with a status detail.** The server could attach a
detail the suite reads. That changes what every SDK server sends, and a
provider outside the SDK could attach it too.

## Consequences

- A call of a service that needs a two-way feature can be listed, and passes
  where the server refuses it for the host.
- A provider outside the SDK passes such a call when it refuses with the
  SDK validator's exact text, which is the same refusal.
- The classification depends on the validator's text staying the same on both
  sides, which holds because the suite and the server are one SDK version.

## Acceptance evidence

- `TestCheck_NegotiatedCall_HappyPath` in `provider-sdk/conformance` lists a
  `PrepareCommonObjects` call for the memory provider in process and behind a
  wire service, and the run conforms. Without the classification the call
  fails under three narrowed hosts.
- `TestCheck_NegotiatedCall_FailurePath` fails the call for an incomplete
  reply, and for a wire provider that refuses with a protocol error in its own
  words. Accepting any protocol text with `InvalidArgument` makes the second
  row pass.
