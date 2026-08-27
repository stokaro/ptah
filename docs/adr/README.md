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
| [0005](0005-agent-surface-inventory.md) | The agent-exposure inventory is generated, and classified by what a verb does to a database | Proposed |
| [0006](0006-one-authorized-agent-runtime.md) | One policy-bearing agent runtime, and an operator-bound database target | Proposed |
| [0007](0007-agent-error-taxonomy.md) | A closed agent error taxonomy, assigned at the sentinel and carried in `_meta` | Proposed |
| [0008](0008-agent-surface-promotion.md) | The agent surfaces are experimental, and these are the criteria that end that | Proposed |
| [0009](0009-remote-transport-authentication.md) | What a remote MCP transport would have to decide before it could exist | Proposed |
| [0010](0010-retrieval-quality-is-not-a-schema-property.md) | Retrieval quality is not a property of the schema, and what follows for verification | Proposed |
| [0011](0011-database-code-analysis-starts-from-what-the-tree-already-derives.md) | Database-code analysis starts from what the tree already derives | Proposed |
| [0012](0012-the-canonical-core-is-removed-and-the-shipping-pipeline-migrates-in-place.md) | The canonical core is removed, and the shipping pipeline migrates in place | Proposed |
| [0013](0013-inference-state-transitions-answer-with-their-provenance.md) | An inference-state transition answers with its provenance, and its approval binds to exact content | Proposed |
| [0014](0014-the-outbox-boundary-is-a-transaction-and-the-order-is-a-sequence.md) | The outbox boundary is a transaction, the order is a sequence, and the trigger does not watch itself | Proposed |

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
