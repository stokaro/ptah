# ADR 0082: A call validates its request once on each side

Status: Proposed

Contract: 25.27 (no change of its own)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0080](0080-a-call-that-grows-with-a-schema-is-bounded-by-its-caller.md)

## Context

The SDK validates every call at both ends. The client checks the request
before it sends it and the reply before it returns it; the server checks the
request before a service reads it and the reply before it sends it. Each
check is a function of `provider`: a request validator such as
`ValidatePlanningRequest`, and a reply validator such as
`ValidatePlannedFeatures`.

The reply validators check the request again before they check the reply.
`ValidatePlannedFeatures` begins with `ValidatePlanningRequest`, and 58 of
the reply validators the runtime calls do the same. A planning request was
therefore validated twice on each side, four times per call, and a request of
500 tables carries the captured state of every table, so each validation
walks, compares and checks all of it. Measured with CPU profiles of a
`migrations generate` of 500 new YDB tables (host #5464 and #5471, SDK
#5472, provider stokaro/ptah-ydb-plugin#291 and #293, YDB 25.1.4.7): the
provider's second validation of the planning requests alone, inside
`ValidatePlannedFeatures`, took a third of the time its first did, and the
creation projection, the preparation and the schema validation repeated
theirs the same way on both sides.

The second check finds nothing the first did not. On the client the request
is the client's own copy, which nothing changes while the call runs; on the
server it is the request the service never sees, since the service receives
a copy. Validation is a function of the description and the request alone.

## Decision

### Each reply validator has a reply-only form

For each reply validator `ValidateX` that checks the request again,
`provider` exports `ValidateXReply`, with the same parameters, which checks
the reply against a request that the request validator accepted with the
same description and that has not changed since. `ValidateX` is the request
validator followed by `ValidateXReply` and refuses what it refused before,
in the same words.

### The runtime asks each question once

The SDK's client and server call the request validator once, before the
request crosses the connection or reaches the service, and the reply-only
validator for the reply. A host adapter, a provider service or a test that
validates a reply it did not see validated keeps calling `ValidateX`.

### Nothing is negotiated

The change is local to each side. It changes no message and no refusal: a
request either side refuses is refused at the same place as before, and a
reply either side refuses is refused with the same reason. A peer of any
contract minor sees the same exchange.

## Alternatives considered

**Remember which requests were validated.** The validators could keep the
requests they accepted and skip a request seen before. Nothing tells a
validator that a request it saw has not changed since: an address says
nothing about the content, and hashing the content costs a walk of the whole
request, which is most of what a validation costs.

**A token for an accepted request.** The request validator could return a
value that only it can make, and the reply validator could take that value
instead of the request. It would make a reply check without a request check
impossible to write, at the cost of a type and a change to every caller of
every validator, for a precondition the runtime already keeps in one place.

**Drop the request check from the reply validators.** A caller of
`ValidateX` that relies on it to check the request too would then accept a
request nobody checked. The reply-only form is a new name, so no caller
loses a check it had.

**One generic reply validator chosen by the request's type.** A single
exported function would dispatch on the request message. It hides which
reply a request expects from the compiler, and the lock calls take an
operation besides the request.

## Consequences

- Each side validates each request once per call: a call of a schema-sized
  service costs one request validation on each side instead of two.
- `provider` exports 58 more functions. Each one's documentation names its
  request validator and the precondition.
- A caller that uses a reply-only validator owns its precondition. One that
  passes a request it did not validate, or changed after validating it, may
  accept a reply the full validator refuses.

## Acceptance evidence

- SDK unit tests: `TestReplyValidators_HappyPath` and
  `TestReplyValidators_FailurePath` in `provider-sdk/provider`;
  `TestPlanningClientValidatesTheRequestOnce` in `provider-sdk/runtime`. The
  existing tests of each full validator pass unchanged.
- CPU profiles of a migration of 500 new YDB tables show one request
  validation per call on each side.
