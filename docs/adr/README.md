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
