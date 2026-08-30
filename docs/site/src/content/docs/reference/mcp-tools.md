---
title: MCP tools
description: Complete lookup reference for the tools and argument shapes served by ptah mcp.
type: reference
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "Which tools does ptah mcp serve, and what arguments do they accept?"
goal: "Look up a Ptah MCP tool and construct a valid call."
sourceOfTruth:
  - "internal/mcpserver"
  - "internal/agentapi"
generated: false
searchAliases:
  - MCP tool schema
  - tools list
overlaps:
  - "/operate/ai-agent-connect/"
  - "/operate/ai-agent-changes/"
disposition: split
---

This page records the Ptah-owned tool surface. The running server's `tools/list`
response is authoritative for its `agentapi.Version`; use this page to find the
tool, then use that response for machine-generated JSON Schema.

The server exposes tools only. It declares no MCP resources or prompts.

## Tool index

| Tool | Purpose | Needs workspace | May write |
| --- | --- | --- | --- |
| `describe_session` | Report policy, configured scopes, targets, and version. | no | no |
| `validate_schema` | Validate a declared schema for one dialect. | no | no |
| `render_schema` | Render declared schema DDL in dependency order. | no | no |
| `schema_lineage` | Trace base columns feeding view columns. | no | no |
| `search_docs` | Search Ptah documentation and return document and heading evidence. | no | no |
| `read_database` | Introspect an operator-configured database target. | no | no |
| `inference_plan` | Plan an embedding-generation change and report outbound text. | no | no |
| `inference_status` | Report generation progress and cutover blockers. | no | no |
| `read_artifact` | List an artifact directory or read one file with digests. | yes | no |
| `preview_patch` | Validate and diff a proposed artifact patch. | yes | no |
| `apply_patch` | Apply one preview, verify, and undo introduced failures. | yes | yes, when policy permits |

Nothing on this surface applies a migration to a database.

## Session and schema tools

`describe_session` takes no arguments.

`validate_schema`, `render_schema`, and `schema_lineage` accept:

| Argument | Type | Requirement |
| --- | --- | --- |
| `dialect` | string | Required. One dialect accepted by the running server. |
| `source` | object | Required. `root_dirs` and `schema_files`; at least one entry across both. |

Schema paths resolve inside the configured schema source roots. Sources with a
scheme are refused.

`search_docs` accepts the query defined by the running tool schema and returns
the matching Ptah document and heading with its answer.

## Database and inference tools

`read_database` accepts:

| Argument | Type | Requirement |
| --- | --- | --- |
| `target` | string | A configured database name; omit only when exactly one exists. |
| `schemas` | array | Schemas to inspect; empty uses the connection default. |

There is no URL argument. The operator configures the connection when starting
the process.

`inference_plan` and `inference_status` use the argument schemas advertised by
the running server. Their responses distinguish measured state, inferred state,
unsupported behavior, and text that would leave the database.

## Artifact tools

`read_artifact` accepts:

| Argument | Type | Requirement |
| --- | --- | --- |
| `artifact` | string | Required: `migrations`, `schema`, or `tests`. |
| `path` | string | A file inside the configured directory; omit to list it. |

`preview_patch` accepts:

| Argument | Type | Requirement |
| --- | --- | --- |
| `artifact` | string | Required. |
| `changes` | array | Required file operations. Use the field names from `tools/list`. |
| `expected_digest` | string | The artifact digest this patch was composed against. Required for a later apply. |
| `summary` | string | The model's untrusted description shown in approval and audit output. |

Paths are relative to the artifact directory and use forward slashes.

`apply_patch` accepts:

| Argument | Type | Requirement |
| --- | --- | --- |
| `preview_token` | string | Required; returned by `preview_patch`. |
| `patch_id` | string | Required; must belong to the same preview. |

The token is single-use and expires after fifteen minutes. Apply compares the
artifact digest again, runs the configured gates, and either keeps the result or
restores the prior bytes.

## Operations deliberately absent

`schema inspect`, `schema diff`, and `migrations lint` need a scratch database
that Ptah resets destructively. They are not exposed behind read-only MCP names.
Run them through the native CLI, where the person starting the operation also
names the disposable database.

See [Configure agent permissions](../../operate/ai-agent-permissions/) for the
capability model and [Troubleshoot Ptah agent calls](../../operate/ai-agent-troubleshooting/)
for stable failure codes.
