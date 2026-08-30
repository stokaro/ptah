---
title: Manage Ptah Assist sessions
description: Inspect, resume, retain, or remove Ptah Assist conversations and keep their audit record separate.
type: how-to
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "What does Ptah Assist save, and how do I manage it?"
goal: "Inspect and manage saved conversations without confusing them with the agent audit log."
sourceOfTruth:
  - "internal/assistsession"
  - "cmd/assist"
generated: false
searchAliases:
  - Ptah Assist history
  - .ptah sessions
overlaps:
  - "/operate/ai-assist/"
disposition: split
owns:
  - cli-ptah-assist-sessions-delete
  - cli-ptah-assist-sessions-list
  - cli-ptah-assist-sessions-prune
  - cli-ptah-assist-sessions-show
---

Ptah writes each conversation as JSON Lines under `.ptah/sessions/` in the
project. Append-only records preserve every complete event when a process ends
mid-conversation.

## Inspect and resume

```bash
ptah assist sessions list
ptah assist sessions show <id>
ptah assist --resume <id>
```

An ID may be abbreviated to a unique prefix. Resuming replays the conversation
but not old tool results: those described an earlier project state, so the model
must call the tool again for a current answer.

The JSONL schema is the same one `ptah assist explain --format jsonl` writes.
Each record carries `schema_version`; tool records are appended when the tool
answers, and a failed run still ends with an answer record carrying
`stop_reason` and an error.

## Understand stored content

A session contains the conversation and the project data Ptah read for the
model, including migration text, schema files, and database object names. It
stores the provider profile name but no provider credential. Database URLs are
redacted before recording.

On Unix, Ptah creates the directory with mode `0700` and files with `0600`.
Windows access is governed by ACLs that Ptah does not inspect. Keep the directory
out of version control:

```text
.ptah/sessions/
```

Use `--ephemeral` for a one-shot run that must not retain a conversation.

## Remove retained conversations

```bash
ptah assist sessions delete <id>
ptah assist sessions prune
```

`prune` removes sessions untouched for thirty days. There is no automatic
retention policy: records disappear only through an explicit command.

## Keep the audit record separate

The session file records conversation and tool results.
`.ptah/agent-audit.jsonl` records Ptah decisions: requested capabilities,
permissions and refusals, approvals, paths, digests, and gates. Deleting a
session never deletes the audit log, and `--ephemeral` does not disable it when
the run has a workspace.
