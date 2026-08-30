---
title: Configure agent permissions
description: Limit which schemas, databases, and artifacts a Ptah agent session may read or change.
type: how-to
audience:
  - "platform-engineer"
readerQuestion: "How do I constrain a Ptah MCP or Assist session?"
goal: "Configure explicit read and write authority for one agent session."
sourceOfTruth:
  - "internal/agentpolicy"
  - "internal/mcpserver"
generated: false
searchAliases:
  - MCP approval policy
  - agent allow-write
overlaps:
  - "/operate/ai-agents/"
  - "/operate/ai-agent-changes/"
disposition: split
---

Every Ptah tool asks the capability policy before doing work. Authority comes
from the operator who starts the process; repository content can narrow it but
cannot grant more.

## Choose artifact access

These configurations widen access one step at a time:

```bash
# Reading tools, with no configured source.
ptah mcp

# Read a declared schema.
ptah mcp --schema-source-root ./schema

# Read and preview migration artifacts.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres

# Ask before applying each migration patch.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations

# Apply without a prompt when the client cannot ask.
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations --auto-approve
```

`--allow-write` accepts `migrations`, `schema`, and `tests`. Each class also
requires its directory flag. A class you do not name cannot be written,
regardless of the model's request.

## Configure database inspection

The operator supplies the connection and class; the model never supplies or
sees the URL:

```bash
ptah mcp \
  --database-url "postgres://reader@db.internal:5432/app?sslmode=require" \
  --database-class dev \
  --allow-database-inspect ask
```

Without an explicit override, database classes resolve as follows:

| Class | Default verdict |
| --- | --- |
| `ephemeral` | allow |
| `dev` | ask for each read |
| `target` | ask for each read |
| `production` | deny; no flag on this surface widens it |
| `unclassified` | deny |

Ptah does not infer the class from a database or hostname. Approval binds to the
exact configured target, not to every database with the same class.
`--auto-approve` applies to artifact patches and grants no database access.

## Understand `allow`, `ask`, and `deny`

| Verdict | Result |
| --- | --- |
| `allow` | The operation proceeds. |
| `ask` | The call returns an input request; a capable client presents it and retries with the answer. |
| `deny` | The call refuses and names the capability or configuration that would change the result. |

A client that cannot present a prompt treats `ask` as a refusal. It never
promotes the operation to `allow`. An approval prompt identifies the capability,
artifact, paths, before and after digests, and patch content address.

## Narrow authority from the repository

A project may contain `.ptah/agent-policy`:

```text
# No agent session may change declarative tests in this repository.
artifact.write:tests deny
```

Repository policy can only remove authority. A rule that tries to grant more
than the process flags is ignored and reported by `describe_session` under
`ignored_policy_rules`.

## Run non-interactively

A CI job has nobody to ask. Grant only the artifact class the job needs and add
`--auto-approve` deliberately:

```bash
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations --auto-approve
```

Auto-approval removes the prompt. Path containment, capability policy, digest
checks, and verification gates still run.

`ptah assist explain` can drive the same tools in a one-shot job:

```bash
ptah assist explain "does the newest migration match the declared schema?" \
  --workspace . --migrations-dir ./migrations --dialect postgres \
  --non-interactive --ephemeral --format jsonl > agent-run.jsonl
```

Exit `0` means the run finished, `1` means a provider or execution limit stopped
it, and `2` means configuration was invalid. The answer record's `verified`
field is true only when at least one Ptah tool answered.

Keep the audit log as a CI artifact. The server announces its path at startup;
each JSON line records the schema version, decision, policy layer, approvals,
paths, digests, and gates, including refusals.
