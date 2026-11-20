---
title: Install the agent skill
description: Teach Claude Code, Codex, Cursor or GitHub Copilot to change a schema through Ptah, with the skill Ptah ships.
type: how-to
audience:
  - "application-developer"
readerQuestion: "How do I teach my coding agent to change a schema with Ptah?"
goal: "Install the Ptah skill where the agent reads skills, and keep it current."
sourceOfTruth:
  - "docs/skills"
  - "internal/skills"
  - "internal/cli/skill"
owns:
  - cli-ptah-skill-install
  - cli-ptah-skill-list
generated: false
searchAliases:
  - agent skill
  - SKILL.md
  - Claude Code skill
  - Codex skill
overlaps:
  - "/operate/ai-agents/"
  - "/operate/ai-agent-connect/"
disposition: keep
---

A coding agent that is asked to add a column tends to write the `ALTER TABLE`
itself. The `ptah-schema` skill tells it the Ptah way instead: change the
desired schema, then let Ptah validate it, plan and generate the migration,
lint it and apply it to a disposable database. The agent loads the skill when a
task matches its description, so a schema task starts from the right workflow
without a prompt that explains it.

The skill is a `SKILL.md` file. Its source is
`docs/skills/ptah-schema/SKILL.md` in the Ptah repository, and every `ptah`
binary carries a copy, so the skill you install matches the binary you run.

## 1. See what Ptah ships

```bash
ptah skill list
```

Each skill is listed with its description, the text an agent reads to decide
when to use it.

## 2. Install the skill for your agent

Run this from the project root, and name your agent:

```bash
ptah skill install --agent claude
```

`--agent` takes `claude`, `codex`, `cursor` or `copilot`, and is repeatable.
Each agent reads skills from a fixed directory under the project:

| Agent | Directory |
| --- | --- |
| Claude Code | `.claude/skills/` |
| Codex | `.agents/skills/` |
| Cursor | `.agents/skills/` |
| GitHub Copilot | `.agents/skills/` |

Codex, Cursor and GitHub Copilot share `.agents/skills/`, so naming more than
one of them writes the skill once. Commit the directory, so everyone who works
on the project, and every agent run in CI, gets the same skill.

To install for every project on your machine, add `--user`. The skill then goes
under your home directory, in the same relative directory:

```bash
ptah skill install --agent codex --agent cursor --user
```

For an agent that reads skills from somewhere else, name the directory with
`--skills-dir`. The skill is written to `<directory>/ptah-schema/SKILL.md`:

```bash
ptah skill install ptah-schema --skills-dir ./tools/skills
```

The command prints one line per file, `written` or `unchanged`.

## 3. Keep it current

The skill names commands and flags, which change between Ptah releases. After
an upgrade, run the same install command again. A file already holding the new
content stays `unchanged`.

A file you edited holds other content, and the install stops before it writes
anything, naming the file. Add `--overwrite` to replace it with the shipped
skill. Nothing is ever deleted, so a skill Ptah stops shipping stays until you
remove it.

## How the skill stays true to the CLI

Every command in the skill runs in CI, on Linux, macOS and Windows, the way the
quick starts do: from an empty directory, in order, with the output the skill
shows checked against what the command prints. The example uses SQLite, so it
needs no server. A change to a verb or a flag that breaks a step fails the pull
request that makes it.

## Use it with the MCP server

The skill also covers a session where the agent has Ptah's MCP tools. It
tells the agent to prefer `validate_schema`, `render_schema`, `read_database`
and `search_docs` where they fit, and to change files only through
`preview_patch` and `apply_patch`. [Connect an MCP client](../ai-agent-connect/)
sets that up, and [Configure agent permissions](../ai-agent-permissions/)
decides what the session may do. The skill grants nothing: it describes a
workflow, and the session's policy still decides what the agent may read or
change.
