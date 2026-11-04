# ADR 0061: A failed execution says why, in text the provider wrote

Status: Proposed

Contract: 25.18

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0059](0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md),
[ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md),
[ADR 0055](0055-a-provider-splits-a-migration-body-into-the-units-its-engine-runs.md)

## Context

[ADR 0059](0059-a-provider-says-why-a-call-failed-in-text-it-wrote.md) lets a
provider say why a call failed and why a lock has the outcome it has. It left
two replies out. `Execute` and `FinishTransaction` report most failures in a
reply, not in a failed call: the units did not apply, or the commit rolled
back. Each reply has a `message`, which the host never prints, and ADR 0059
kept it that way.

So a migration whose statement the engine refused shows the outcome and the
statement, and not why. Measured on YDB 26.2.1.14 through
`stokaro/ptah-ydb-plugin` and Ptah v0.19.0:

| Case | The provider path prints | The built-in YDB engine prints |
| --- | --- | --- |
| a column of a type YDB does not have | `provider execution: NOT_APPLIED (0 units confirmed applied)` | `Unknown simple type 'NoSuchType'` |
| an `INSERT` of a key that exists | `provider execution: NOT_APPLIED (0 units confirmed applied)` | `Conflict with existing key` |

The revision records the same text. The built-in engine is being removed
([stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)).

Printing `message` is not safe. Its rule forbids a credential and a raw URL,
and nothing else: a provider may quote the
statement in it, or the text the engine answered with. The YDB provider puts
YDB's issue text there, and YDB quotes the token it refused, which can be a
literal of the statement. A statement can carry a secret: an operator writes
one into a migration, and
[ADR 0043](0043-sensitive-input-references-are-approved-before-values-are-bound.md)
binds a sensitive input into the text of the statement the provider sends.
ADR 0059 also told every provider that lists `errors.reasons` that `message`
is never shown, so none of them chose to have it shown.

## Decision

### A result carries a reason beside its outcome

Contract 25.18 adds one field to each result, under the existing feature
`errors.reasons`:

```proto
message Execution {
  // ...fields 1 to 6 unchanged...
  ProviderDiagnostic diagnostic = 7;
}

message TransactionResult {
  // ...fields 1 to 4 unchanged...
  ProviderDiagnostic diagnostic = 5;
}
```

An execution result carries a diagnostic only with an outcome other than
`APPLIED`. A transaction result carries one only with an outcome its action
does not succeed with: never `COMMITTED` in answer to a commit, or
`ROLLED_BACK` or `NOT_STARTED` in answer to a rollback.
`provider.CheckTransactionResult` decides which outcome succeeds, and the
transport reads the same check, so the two cannot disagree.

The rules of ADR 0059 hold unchanged. The provider writes the reason itself,
names what failed and why, and never quotes statement text, a value, an
address or a credential. The SDK checks the shape with
`provider.ValidateDiagnostic`. `message` keeps its meaning and is never
printed.

### The feature stays one

A provider built with an SDK of contract 25.16 or 25.17 cannot set the new
fields, and a host built with one drops them as unknown fields. So every peer
that can send or read the field reads `errors.reasons` as one promise: the
provider wrote every reason it sends. A second feature would ask a provider to
list two names for one promise, and a host to negotiate them apart.

### What the SDK enforces

Both ends of `Execute` and `FinishTransaction` apply the same rule, each alone:
they remove the diagnostic unless both sides list the feature, remove it
beside a success, and put `the provider's reason is malformed and not shown`
in place of one `ValidateDiagnostic` refuses.

A malformed reason does not make the result invalid. A lock receipt with one
is refused (ADR 0059), and a refused receipt proves no ownership, which costs
nothing a caller relies on. A refused execution result is an unknown outcome:
a statement the engine refused would become one the host cannot resume
without asking the operator. Only the text is at fault, so only the text is
replaced.

A result the client finds malformed in another way, such as an outcome its
count contradicts, is an unknown outcome, and it carries no reason: its facts
are not trusted, so its text is not either.

### What the host prints

`provider.ExecutionError` and `provider.TransactionError` gain `Reason`, and
their text ends with it:

```text
provider execution: NOT_APPLIED (0 units confirmed applied): <reason>
provider transaction outcome is ROLLED_BACK: <reason>
```

The host prints the error, so the reason reaches standard error, the run
document and the revision's recorded error alike.

## Alternatives considered

**Print `message` under `errors.reasons`.** It needs no new field, and the
host change would be one line. It would show text whose rule allows a
statement's literal, from providers that were told it is never shown.
Rejected.

**Pass the engine's own text on after a sanitizer.** A filter that drops
quoted tokens and paths depends on how each engine quotes, changes meaning
with every server release, and cannot find a secret it does not know.
Rejected for the same reason ADR 0059 rejects it for the status message.

**Refuse a result with a malformed reason.** It is the lock receipt's rule.
Here it would turn a confirmed `NOT_APPLIED` into `UNKNOWN`, and an operator
would have to inspect the database before resuming a migration that applied
nothing. Rejected.

**Fail the call with a status that carries the reason.** A provider could
answer a refused statement with a failed call instead of a reply. A failed
call is an unknown outcome to the host, so it would lose the confirmed prefix
and the abort flag. Rejected.

## Consequences

- Contract 25.18 adds two fields under an existing feature.
- A migration whose statement the engine refused says why through a
  provider, as the built-in YDB engine does.
- The YDB provider writes its reasons from YDB's status, its issue codes and
  the position of the refused text, and never from YDB's issue text.
- Keeping user data out of a reason stays a review duty in each provider.
- `Execution.message` and `TransactionResult.message` keep their meaning, and
  no host prints them.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.

## Acceptance evidence

- SDK tests: an execution result that is not `APPLIED` gives its reason to
  `ExecutionError`, after the statement timeout clause too, and a transaction
  result its action does not succeed with gives its reason to
  `TransactionError`. A malformed reason, and any reason of a malformed result,
  gives none. A reason beside a success fails nothing.
- Each end alone, the SDK server and the client of a peer outside the SDK,
  removes an unnegotiated reason and one beside a success, and puts its own
  sentence in place of a malformed one while the outcome stays.
- An SDK-only proof provider, started as a process, answers an execution with
  `NOT_APPLIED` and a commit with `ROLLED_BACK`, each with a reason and with a
  message that quotes the database path. A host that lists the feature reads
  both reasons and not the path; a host that does not reads the outcomes
  alone.
