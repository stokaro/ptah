# Architecture decision records

An ADR records a decision that shapes more than one package: the alternatives
weighed, the one chosen, and why. It exists so a later reader can tell a
decision from an accident, and so revisiting one starts from what was known at
the time rather than from a reconstruction.

A record is never edited to reflect a later change of mind. It is superseded by
a new record that names it, and its own status line is updated to point there.

| Number | Title | Status |
| --- | --- | --- |
| [0001](0001-canonical-schema-state-and-pipeline-boundaries.md) | Canonical schema state and pipeline boundaries | Proposed |
| [0002](0002-read-only-agent-mvp-scope-and-transport.md) | Read-only agent MVP scope, operation ownership, and transport | Proposed |
| [0003](0003-agent-surface-trust-boundaries.md) | Agent-surface trust boundaries and threat model | Proposed |
| [0004](0004-constrained-artifact-mutation.md) | Capability broker, artifact digests, and constrained mutation | Proposed |

## Writing one

- Number it in sequence, and name the file for the decision rather than for the
  issue that prompted it.
- State the status, the deciders, and the issue it answers.
- Give every decision at least two credible alternatives, or write down why only
  one is viable. An alternatives section that lists options nobody considered is
  worse than none.
- Ground the context in something measured. A record that argues from how the
  code ought to look cannot be checked by the next reader.
- End with the consequences accepted, including the ones that are costs.
