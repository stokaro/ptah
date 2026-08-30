---
title: AI and agents overview
description: Choose Ptah Assist or an MCP client, then give the model only the schema, database, and artifact authority it needs.
type: landing
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "How can a model work with Ptah without receiving unrestricted project access?"
goal: "Choose an AI surface and route to its setup and safety guidance."
sourceOfTruth:
  - "cmd/assist"
  - "cmd/mcp"
  - "internal/mcpserver"
generated: false
searchAliases:
  - MCP
  - AI database agent
overlaps:
  - "/operate/ai-assist/"
  - "/operate/ai-agent-connect/"
disposition: split
---

Ptah exposes its schema and migration operations to a model without granting a
general shell or unrestricted filesystem access. Choose who drives the session:

| Situation | Use | Who owns the model connection |
| --- | --- | --- |
| Ask Ptah a question or hold a Ptah-focused conversation | [`ptah assist`](../ai-assist/) | Ptah calls the provider profile you configure. |
| Add Ptah tools to Claude, Cursor, VS Code, Zed, or another MCP client | [`ptah mcp`](../ai-agent-connect/) | The client starts Ptah and chooses the model. |

Both surfaces use the same Ptah tool and capability model. Neither grants a
model permission to apply migrations to a database. A client may provide its own
shell, filesystem, or network tools; Ptah cannot constrain tools it does not
serve.

:::note[Experimental]
The agent contract may change with `agentapi.Version`, and capability names are
not frozen. Read the reported version instead of assuming it.
:::

## Start with no authority

A bare `ptah mcp` process serves reading tools but has no schema root, database,
or workspace to reach. You add each scope explicitly:

```bash
ptah mcp --schema-source-root ./models
```

Adding a workspace exposes artifact read and preview tools. It still does not
permit writes:

```bash
ptah mcp \
  --workspace . \
  --migrations-dir ./migrations \
  --dialect postgres
```

[Connect an MCP client](../ai-agent-connect/) gives the client-specific
configuration. [Agent permissions](../ai-agent-permissions/) explains schema,
database, and artifact authority before you widen it.

## A change is always previewed

Ptah artifact changes follow one sequence:

```text
read current digest -> preview a patch -> approve when required -> apply -> verify or undo
```

The preview writes nothing. Its token is single-use, expires after fifteen
minutes, and belongs to one content-addressed patch. Apply checks that the
artifact still has the digest the preview saw. It recomputes migration
integrity, runs artifact-specific gates, and undoes a patch that introduces an
error.

[Review and apply an agent patch](../ai-agent-changes/) walks through that
sequence. [MCP tool reference](../../reference/mcp-tools/) lists every tool and
argument without mixing the lookup material into the workflow.

## Decide what to trust

Ptah constrains Ptah operations, not the model. The useful controls are:

- configured roots and directories bound when the server starts;
- explicit database identity and class, never a database URL supplied by the
  model;
- capability verdicts of `allow`, `ask`, or `deny`;
- repository policy that can only remove authority;
- artifact digests, single-use previews, verification gates, and rollback;
- an audit record containing permissions, refusals, paths, digests, and gates.

Repository text is returned as data, not instructions, but labels are not a
security boundary. Capability checks, path containment, and verification gates
are the controls.

## Common next tasks

- [Connect an MCP client](../ai-agent-connect/)
- [Configure agent permissions](../ai-agent-permissions/)
- [Review and apply an agent patch](../ai-agent-changes/)
- [Troubleshoot an MCP call](../ai-agent-troubleshooting/)
- [Configure a Ptah Assist provider](../ai-assist-providers/)
