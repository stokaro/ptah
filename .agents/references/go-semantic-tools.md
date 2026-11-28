# Go semantic navigation

Use this reference for symbol changes, unfamiliar package relationships, or
setting up Go language tools. The repository's Go, provider, and validation
policies still determine ownership and required evidence.

## Establish the analyzed build

From the affected module in the linked worktree, check the toolchain and
workspace before interpreting a semantic result:

```console
go version
go env GOMOD GOWORK GOOS GOARCH GOFLAGS
```

Read that module's `go.mod`. A newer installed toolchain does not authorize
language features or standard-library APIs above its `go` directive. Identify
the relevant build tags with `scripts/list-build-tags.sh` from the repository
root. Record any different target or tags used by the semantic session.

For host/SDK changes, follow the
[provider workspace policy](../skills/ptah-provider-contracts/SKILL.md).
Check the resolved module paths and versions from the host module:

```console
go list -m -json ptah.run/provider-sdk ptah.run/sqlsyntax
```

An adjacent module directory is not proof that the host imports it. A temporary
development workspace and `GOWORK=off` release acceptance answer different
questions. Point the language tools at the same workspace as the check being
discussed, and refresh the session after changing its build configuration.

## Optional gopls setup

The [official gopls guide](https://go.dev/gopls/features/mcp) documents its
experimental Model Context Protocol (MCP) server. Use an existing server when
it already analyzes the intended worktree. Otherwise, check the available
binary before configuring a client:

```console
gopls version
gopls help mcp
```

MCP support requires gopls v0.20 or later. Select a released version compatible
with the module toolchain and record it in the evidence. If installation is
needed, use that exact version through the developer's existing tool setup;
do not silently install `@latest` or edit a global client configuration.

Configure the client's local or session MCP server using these values. This
table describes the process, not a configuration format shared by all clients.

| Setting | Value |
| --- | --- |
| Executable | The verified `gopls` executable, with an absolute path if needed |
| Arguments | `["mcp"]` |
| Transport | Standard input and standard output |
| Working directory | The affected module in the linked worktree; launch from there if the client has no working-directory setting |
| Environment | The intended Go toolchain and the same workspace, target, and build flags as the analyzed build |

Verify the connection using the client's server and tool listing. This detached
server reads saved files, including files created or deleted after it started.
Save edits before querying it. An attached editor session can include unsaved
buffers; record that distinction when results differ.
The server may load modules and populate caches. Do not run a separate
whole-repository build merely to warm it up.

A server started in the main checkout does not analyze the linked worktrees
parked under it. Each worktree has its own `go.mod`, so it is a nested module
outside that server's workspace. A query on a worktree file then answers from
that file alone, for example one reference where the worktree has nine, and
reports no error. Start the server from the root of the worktree being changed,
one server per worktree. A client that starts its servers once, in the
directory where the session began, serves every worktree agent from the wrong
module; run `gopls mcp -listen=127.0.0.1:<port>` from the worktree root instead
and connect to that. Before trusting a short or empty reference list, query a
symbol whose callers you already know.

`gopls mcp -instructions` prints the installed version's usage guidance. Read
it when configuring or troubleshooting the integration; avoid injecting it into
every session or duplicating it here. Inspect the advertised tools rather than
assuming that an editor feature is also an MCP tool.

## Navigate with a bounded question

1. Locate the declaration and its package with symbol search. Read its local
   context and API contract before following references.
2. Inspect references and affected consumers for signature, field, error, or
   ownership changes. Use implementation or call-hierarchy support only if the
   connected tool offers it. Supplement with scoped text searches for dynamic
   dispatch, serialized names, generated declarations, and non-Go consumers.
3. After a coherent edit, inspect diagnostics for the affected packages.
   Resolve new parse and type errors before a more expensive test run. Avoid
   repeating diagnostics after each mechanical edit.
4. Run focused behavioral checks and relevant consumers through
   [validation guidance](../skills/ptah-validation/SKILL.md). Diagnostics do not
   establish database effects, race freedom, or release compatibility.

The [gopls workspace documentation](https://go.dev/gopls/workspace) explains
how build configurations determine analysis. A clean result covers the loaded
packages and configurations, not every module, tag, platform, or external
provider. If loading fails, report the missing toolchain, dependency, or scope
once and continue with scoped reads where possible. Never describe incomplete
analysis as an empty reference set.
