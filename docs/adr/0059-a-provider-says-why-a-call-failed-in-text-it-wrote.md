# ADR 0059: A provider says why a call failed, in text it wrote

Status: Proposed

Contract: 25.16

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md),
[ADR 0053](0053-a-machine-readable-failure-names-why-the-target-could-not-be-opened.md)

## Context

A provider fails a call with a gRPC status: a code and a message. The SDK
client turns it into the host's error with the operation and the code alone,
`provider compare models failed (InvalidArgument)`, and drops the message. The
comment beside the code says why: the message is provider-authored and can
contain connection secrets. The YDB provider builds most of its statuses as
`status.Error(code, err.Error())`, where `err` is whatever its error chain
holds: a sentence the provider wrote, such as `the streaming query capability
was not established`, or a driver's error, which can quote the connection
address, a credential or a statement with the values bound into it
([ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md)
binds secret inputs into statement text for the dispatch RPC alone).

So a refusal the provider wrote for the operator never reaches the operator.
Through the fixture provider, a role the fixture cannot hold reads `provider
render failed (Unknown)`, while the fixture wrote `role admin: A fixture role
has a login and nothing else`. The built-in YDB engine runs in the host
process and prints its full reason, and it is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)), so the
provider has to say why too.

Passing the message on is not safe. Neither the SDK nor the host can tell
a sentence a provider wrote from a driver's text it forwarded, and a provider
built before this decision never promised which one its messages are.

The contract already lets a provider write text the host shows, in typed
replies: a render refusal, a row-write refusal, an embedding refusal, a dev
cleanup refusal and an encode loss each carry a message the SDK checks with
one rule. The text is one line of at most 512 bytes, and it names a subject
and never quotes statement text or a value. The error path is the one place
with no such channel.

## Decision

### A reason travels beside the status, under a feature

`errors.reasons` is a two-way feature of the provider alone. A call names no
engine for its error, so no engine lists it. Contract 25.16 adds one message
and one field:

```proto
message ProviderDiagnostic {
  string reason = 1;
}

message LockResult {
  // ...fields 1 to 3 unchanged...
  ProviderDiagnostic diagnostic = 4;
}
```

A failed call's status carries at most one `ProviderDiagnostic` in its
details, beside its code and message. A lock receipt carries one only with an
outcome its operation does not succeed with, since a `BUSY`, `LOST`,
`NOT_ACQUIRED` or `UNKNOWN` lock is a reply, not a failed call. It never
accompanies `ACQUIRED` from `TryLock` or `CheckLock`, or `RELEASED` or
`NOT_ACQUIRED` from `ReleaseLock`. `CheckLockResult` and the receipt's check
read the success outcomes from one predicate, so they cannot disagree about
which outcome is a failure.

### The provider is responsible for the content

Listing the feature is the provider's promise about every reason it sends.
The provider writes the reason itself. It names what failed and why, and it
never quotes statement text, a value, an address or a credential. A reason is
therefore never the text of an error the provider received: a driver's text
stays in the status message, which no host shows. `runtime.ErrorWithReason(code,
reason)` builds the error, so the reason is a separate argument a reviewer can
see at the call site.

Only the provider knows which of its texts are its own. A host or an SDK check
that searched a reason for values would have to know the values: it could look
for the bound inputs of one request, and not for a password in a driver's
error about another.

### What the SDK enforces

The SDK checks the shape of a reason, with `provider.ValidateDiagnostic`: one
line of at most 512 bytes of valid UTF-8, with no control character, no line
or paragraph separator and no white space around it. It is the rule the typed
refusals already follow. A stack trace, a dumped multi-line driver message and
a terminal escape fail it.

Both transport boundaries enforce the negotiation and the shape, each alone:

- The SDK server checks every call's error. Unless both sides list the
  feature, it removes every diagnostic, so a provider sets a reason without
  reading `HostFeatures`. With the feature, it sends the one valid diagnostic,
  and `the provider's reason is malformed and not shown` in place of an invalid
  one or of several. The code, the message and other details are kept. For a
  lock receipt it removes the diagnostic the same way and refuses a receipt
  whose diagnostic is malformed or accompanies a success, as it refuses any
  invalid receipt.
- The client reads a reason only when the negotiated description lists the
  feature, and writes the same sentence for a malformed reason or several, so a
  server outside the SDK cannot place text in the host's error that the SDK
  server would not.

A cancellation and a deadline stay the context's errors, with no reason.

### What the host prints

The reason follows the code, after a colon, as a wrapped Go error reads:

```text
provider compare models failed (InvalidArgument): the streaming query capability was not established
provider lock outcome is BUSY: another session holds the lock
```

The status message is never printed. The host lists the feature, so its JSON
documents carry the same sentence in their error message.

### A peer without the feature

Without the feature on both sides, the host's error is the operation and the
code, and a lock error is the outcome. A host built before the feature does
not list it, so the SDK server sends it no reason, and a provider built before
the feature sends none.

## Alternatives considered

**Pass the status message on after sanitizing it.** A single line, a length
cap, valid UTF-8 and no control character make the text printable. They do not
make it free of secrets: a connection address with a password, or a statement
with a bound value, passes every one of those checks. It would also show the
messages of every existing provider, none of which chose to be shown.
Rejected.

**Show the message in a verbose mode only.** A verbose run is the run an
operator pastes into an issue or keeps in a CI log, so a secret there leaks
the same way, and the reason is needed in an ordinary run: the built-in
engine prints it without a flag. The message would still mix a provider's
sentence with a driver's text. Rejected.

**Standard `google.rpc` details, such as `LocalizedMessage` or
`ErrorInfo`.** They would add a dependency on the error-details package, and
a library a provider uses can attach them for its own purposes, so their
presence promises nothing about the text. A message the contract owns, under
a feature the provider lists, is the promise. Rejected.

**A reason field on each reply.** A failed call returns no reply, so a reason
there could not explain one, and every reply message of the service would
need the field. The status details reach every call at once. Rejected for
calls; the lock receipt, whose failures are replies, takes the field.

**Refuse a malformed reason as a protocol error.** An error that already
failed would become `Internal` and lose the code the provider chose. The
sentence keeps the code and tells the operator the provider sent text it may
not show. A lock receipt is a reply, so it is refused, as an invalid receipt
always is.

## Consequences

- Contract 25.16 adds a message, a field and a two-way feature.
- A provider can say why it refused a call, which the removal of the built-in
  YDB engine needs. The YDB provider gives its reasons through
  `runtime.ErrorWithReason`, and keeps a driver's error code-only.
- Keeping user data out of a reason is a review duty in each provider. The
  SDK checks the shape, not the content.
- `provider.LockError` gains `Reason`, and its text carries the reason when
  there is one.
- `Execution.message` and `TransactionResult.message` keep their meaning: the
  host does not print them, and this feature does not cover them.
- The SDK server's own refusals, such as a request its checks refuse, carry no
  reason.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.

## Acceptance evidence

- SDK tests accept a reason at 512 bytes and outside ASCII, and refuse a
  missing, empty, longer, invalid, padded or multi-line one and one with a
  control character or a separator. They accept a lock reason with each
  failure outcome and refuse one with each success outcome.
- Over a gRPC connection, with the SDK server and with a server outside the
  SDK, the client shows a negotiated reason and nothing of the status message,
  and shows no reason that the host or the provider did not list, no text of a
  plain error and no malformed reason. The SDK server removes an unnegotiated
  reason, replaces a malformed one and keeps the code, the message and other
  details.
- An SDK-only proof provider, started as a process, refuses an inspection with
  a reason while its status message quotes the database path, and answers a
  busy lock with a reason. A host that lists the feature reads both reasons and
  not the path; a host that does not reads the code and the outcome alone.
