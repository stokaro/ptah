---
title: Review and apply an agent patch
description: Use Ptah's digest-bound preview, approval, application, and verification workflow for migration, schema, or test files.
type: how-to
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "How does a model propose and safely apply a Ptah artifact change?"
goal: "Review and apply one digest-bound agent patch, or stop after preview."
sourceOfTruth:
  - "internal/mcpserver"
  - "internal/agentpatch"
generated: false
searchAliases:
  - MCP preview patch
  - agent migration patch
overlaps:
  - "/operate/ai-agent-permissions/"
  - "/reference/mcp-tools/"
disposition: split
---

An agent change uses the same sequence for migration, schema, and test
artifacts: read the current digest, preview a patch against that digest, approve
when policy requires it, then apply the exact preview.

## Prerequisites

Start the session with a workspace, the artifact directory, and the target
dialect. Add `--allow-write` for the class only if this session may apply:

```bash
ptah mcp --workspace . --migrations-dir ./migrations --dialect postgres \
  --allow-write migrations
```

## 1. Read the artifact

`read_artifact` lists the directory or returns one file. The response includes a
digest for the whole artifact and a digest for each entry. Repository content is
labeled as data, not instructions.

The agent must carry the artifact digest into `preview_patch` as
`expected_digest`. Omitting it can produce a preview, but apply will refuse after
spending that token.

## 2. Preview the patch

The preview request names the artifact, current digest, summary, and file
changes:

```json
{
  "artifact": "migrations",
  "expected_digest": "sha256:90fde6…",
  "summary": "add a created_at column to users",
  "changes": [
    {
      "path": "1700000100_users_created_at.up.sql",
      "operation": "create",
      "content": "ALTER TABLE users ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();\n"
    }
  ]
}
```

`preview_patch` writes nothing. It returns a diff, base and projected digests,
whether approval is required, a patch ID, and a preview token. The token expires
after fifteen minutes, is single-use, and belongs to that patch ID.

Review-only work stops here. Let the token expire.

## 3. Apply the exact preview

`apply_patch` takes only `preview_token` and `patch_id`. It refuses a spent,
expired, unknown, or mismatched token. It also refuses when the artifact digest
changed after preview; read the artifact again and compose a new patch against
the new state.

Ptah runs gates before and after the write:

| Artifact | Verification |
| --- | --- |
| `migrations` | integrity file matches; SQL parses and lints |
| `schema` | schema loads, validates, and renders for the configured dialect |
| `tests` | every declarative test file parses |

Migration patches cannot write `ptah.sum`; Ptah refreshes it after the patch.
The apply response reports baseline and verification results plus diagnostics
introduced or resolved by this patch.

## 4. Verify the outcome

A successful response has `rolled_back: false` and no introduced errors. If the
patch introduces a gate error, Ptah restores the previous files, recomputes
integrity, and returns `rolled_back: true` with the gate, rule, path, line, and
message. That is a verified rollback, not a refusal.

Review the domain meaning of the change even when gates pass. Ptah proves path
containment, artifact integrity, and the configured structural checks. It does
not prove that a syntactically valid migration represents the business change
you intended, and it does not analyze destructive SQL on this surface.

## Audit the session

With a workspace, Ptah writes one JSON line per decision to
`.ptah/agent-audit.jsonl` unless `--audit-log` chooses another path. Records
include refusals, capability decisions, approval, paths, before and after
digests, and gates. `caller_summary` is the model's untrusted text and is
excluded from the patch identity.
