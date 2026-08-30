---
title: Troubleshoot Ptah agent calls
description: Resolve MCP scope, permission, digest, preview, database, and verification failures by diagnostic code.
type: troubleshooting
audience:
  - "application-developer"
  - "platform-engineer"
readerQuestion: "Why did a Ptah MCP tool call fail, and what should I change?"
goal: "Resolve a Ptah agent diagnostic without granting unrelated authority."
sourceOfTruth:
  - "internal/agentapi"
  - "internal/mcpserver"
generated: false
searchAliases:
  - MCP error
  - approval_unavailable
  - digest_mismatch
overlaps:
  - "/operate/ai-agent-permissions/"
  - "/reference/mcp-tools/"
disposition: split
---

Ptah agent failures begin with a stable diagnostic code. Branch on the code,
not the rest of the sentence.

| Code | Likely cause | Resolution |
| --- | --- | --- |
| `invalid_request` | An argument is missing, malformed, or contradicts another. | Correct the tool arguments. |
| `no_source_scope` | No schema root was configured. | Restart with `--schema-source-root`. |
| `no_database_target` | No live target was configured. | The operator adds `--database-url` and a class. |
| `no_workspace` | Artifact tools were not served. | Restart with `--workspace`. |
| `artifact_class_not_configured` | The requested class has no directory. | Add `--migrations-dir`, `--schema-dir`, or `--tests-dir`. |
| `capability_denied` | Policy refuses the operation. | Call `describe_session`; grant only the named capability if appropriate. |
| `approval_unavailable` | Policy says `ask`, but the client cannot prompt. | Grant the specific capability outright or use a client that can ask. |
| `approval_refused` | A person declined the operation. | Do not retry as a wider operation. |
| `unsafe_path` | The path is absolute, outside scope, ambiguous, or contains a scheme. | Choose a local path inside a configured root. |
| `schema_source_unreadable` | The source is absent or invalid. | Check the path and parse the source with the native schema tools. |
| `render_failed` | The schema cannot render for the requested dialect. | Fix the declaration or choose the intended dialect/version. |
| `database_unreachable` | Ptah could not open the configured target. | Check the operator-supplied URL and server availability. |
| `database_read_failed` | The connection opened but catalog inspection failed. | Check read privileges and the engine-specific guide. |
| `digest_mismatch` | The artifact changed after the agent read it. | Read again and compose a new patch against the new digest. |
| `unknown_preview` | The token expired, was spent, or belongs to another patch. | Preview again. |
| `invalid_patch` | The patch has an invalid operation, path, content, or duplicate target. | Correct the patch; never include `ptah.sum`. |
| `gate_failed` | The patch introduced an error and was undone. | Use the returned gate diagnostics to repair the patch. |
| `artifact_too_large` | A file, patch, or directory exceeds the limit. | Reduce the operation's scope. |
| `not_regular_file` | The path names a directory, symlink, or device. | Use a regular file inside the artifact directory. |
| `verification_unavailable` | Verification could not run; nothing was written. | Restore the required verifier or configuration, then preview again. |
| `write_failed` | The filesystem write failed and was undone. | Inspect filesystem permissions and available space. |
| `internal` | Ptah violated its own contract. | Preserve the diagnostic and report a Ptah defect. |

## First checks

Run `describe_session` before widening policy. It shows both the capability
verdict and what this process can actually reach.

An artifact tool missing from `tools/list` means the process was started without
`--workspace`; a missing tool is different from a tool that returned a refusal.

`unsafe_path` for `oci://registry/schema:v1` is expected. Pull the artifact with
the native OCI command first, place it under an authorized root, and pass the
local path. No MCP permission grants arbitrary fetching.

After `gate_failed`, confirm that the response says `rolled_back: true` and read
the artifact again. Its digest should match the state before the failed apply.

## Safety implications

Do not fix a narrow refusal by granting a general shell or a broader filesystem
root. Ptah's refusal is evidence that its scoped operation stayed inside the
declared boundary; a client-provided tool may bypass that boundary entirely.
