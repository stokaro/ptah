---
title: Use Ptah Assist
description: Ask one verified question or hold a conversation with a model provider you control.
type: how-to
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "How do I ask Ptah a model-assisted question and see what it verified?"
goal: "Run one Ptah Assist question and distinguish the model answer from Ptah tool evidence."
sourceOfTruth:
  - "cmd/assist"
  - "internal/assistloop"
generated: false
searchAliases:
  - Ptah AI assistant
  - explain migrations with AI
overlaps:
  - "/operate/ai-agents/"
  - "/operate/ai-assist-providers/"
  - "/operate/ai-assist-sessions/"
disposition: split
owns:
  - cli-ptah-assist-context
  - cli-ptah-assist-explain
---

Ptah Assist calls a model you choose through a provider profile you control.
There is no Ptah account, hosted model, or Ptah AI token. The model uses the same
scoped Ptah tools and verification gates that an external MCP client reaches.

:::note[Experimental]
The agent contract may change with `agentapi.Version`, and capability names are
not frozen. Read the reported version instead of assuming it.
:::

## Prerequisites

Configure and test one model profile first:

```bash
ptah assist provider test
```

If no profile resolves, follow [Configure a Ptah Assist provider](../ai-assist-providers/).

## Ask one question

```bash
ptah assist explain \
  --workspace . \
  --migrations-dir ./migrations \
  --dialect postgres \
  "what changed in the last migration?"
```

Without `--workspace`, the model receives reading tools only. A workspace adds
artifact read and preview tools; writes remain refused until `--allow-write`
names an artifact class. [Agent permissions](../ai-agent-permissions/) explains
the shared policy.

Use `--trace` to see each Ptah tool call. A text answer that used no Ptah tool
ends with:

```text
-- No Ptah tool answered, so nothing above was checked against this project.
```

The answer is the model's prose. The tool trace is the evidence that Ptah read,
validated, or rendered something. Do not treat fluent prose as verified merely
because it came through the `ptah` process.

## Preview what leaves the machine

```bash
ptah assist context "what changed in the last migration?" \
  --workspace . --migrations-dir ./migrations --dialect postgres
```

`context` builds the same first provider request as the real run but sends
nothing. It reports bytes for Ptah instructions, tool schemas, conversation,
and total input. On a new session, project content reaches the provider only
after a tool answers. A resumed session includes its prior conversation in the
first request.

Every text-mode run reports how many bytes of project content reached the
provider and how many tool answers supplied them. JSON and JSONL outputs carry
the tool records themselves instead of a summary line.

## Hold a conversation

```bash
ptah assist
```

Interactive commands begin with `/`:

```text
/tools     list the Ptah tools this session can reach
/session   show where the conversation is saved
/trace     show or hide the tool trace
/help      list interactive commands
/exit      leave; Ctrl-D does the same
```

When a patch requires approval, Ptah shows the artifact, paths, and exact digest
before asking whether to allow it once or for the session. The one-shot
`--non-interactive` mode refuses an operation that needs approval instead of
assuming consent.

## Read output from a program

```bash
ptah assist explain "what changed?" \
  --workspace . --migrations-dir ./migrations --dialect postgres \
  --format jsonl
```

JSONL writes session, request, tool, and answer records as each completes. The
final answer carries `verified`, `stop_reason`, and any error. Exit `0` means the
run finished, `1` means a provider or execution limit stopped it, and `2` means
the configuration was invalid.

Use `--ephemeral` when the conversation must not be saved. The separate agent
audit log still records Ptah's permission decisions. Read
[Manage Ptah Assist sessions](../ai-assist-sessions/) for storage, resuming, and
retention.
