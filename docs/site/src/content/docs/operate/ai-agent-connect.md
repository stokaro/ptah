---
title: Connect an MCP client
description: Start ptah mcp from Claude, Cursor, VS Code, Zed, or another stdio MCP client.
type: how-to
audience:
  - "application-developer"
readerQuestion: "How do I connect my MCP client to Ptah?"
goal: "Start a Ptah MCP session and verify the client can describe it."
sourceOfTruth:
  - "cmd/mcp"
  - "internal/mcpserver"
generated: false
searchAliases:
  - Claude Ptah MCP
  - Cursor Ptah MCP
  - VS Code Ptah MCP
overlaps:
  - "/operate/ai-agents/"
disposition: keep
---

`ptah mcp` speaks the Model Context Protocol over standard input and output. An
MCP client starts the process; running it interactively in a terminal is not the
intended workflow.

## 1. Verify the executable

```bash
ptah mcp --help
```

The process offers tools only. It declares no MCP resources or prompts.

## 2. Add the server

Claude Code, Claude Desktop, and Cursor use the `mcpServers` key:

```json
{
  "mcpServers": {
    "ptah": {
      "command": "ptah",
      "args": ["mcp"]
    }
  }
}
```

VS Code uses the same server object under `servers`:

```json
{
  "servers": {
    "ptah": {
      "command": "ptah",
      "args": ["mcp"]
    }
  }
}
```

Common configuration locations:

| Client | Configuration |
| --- | --- |
| Claude Code | `.mcp.json` in the project, or `~/.claude.json` |
| Claude Desktop | `claude_desktop_config.json` |
| Cursor | `.cursor/mcp.json`, or `~/.cursor/mcp.json` |
| VS Code | `.vscode/mcp.json`, or user settings |
| Zed | `settings.json` under `context_servers` |

Claude Code can add the same process from its CLI:

```bash
claude mcp add ptah -- ptah mcp
```

The `--` separates Claude Code options from the Ptah command.

## 3. Add one readable scope

The bare server cannot read a schema because no directory has been authorized.
Add only the roots that contain desired-schema input:

```json
{
  "mcpServers": {
    "ptah": {
      "command": "ptah",
      "args": [
        "mcp",
        "--schema-source-root", "./schema",
        "--schema-source-root", "./models"
      ]
    }
  }
}
```

References with a scheme such as `oci://` or `https://` are refused. Fetching is
a network operation and this surface has no capability that grants it.

## 4. Verify the session

Ask the client to call `describe_session`. The answer separates authority from
reachability: it lists the capability table, configured schema roots, database
targets by name and class, and the workspace when one exists.

A capability row can say `database.inspect:dev ask` while the database list is
empty. The first is policy; the second says no target was configured.

## 5. Add a workspace only when needed

To let the client read and preview migration changes:

```json
{
  "mcpServers": {
    "ptah": {
      "command": "ptah",
      "args": [
        "mcp",
        "--workspace", "/path/to/project",
        "--migrations-dir", "./migrations",
        "--dialect", "postgres"
      ]
    }
  }
}
```

This exposes artifact tools but still permits no write. Read
[Agent permissions](../ai-agent-permissions/) before adding `--allow-write` or a
database target.
