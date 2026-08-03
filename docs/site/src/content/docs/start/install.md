---
title: Install Ptah
description: Install, build, and verify the Ptah CLI before using it in a project.
---

Use this page to get a `ptah` binary onto a developer machine or CI runner.

## Choose an install path

| Situation | Recommended command |
| --- | --- |
| You are developing Ptah itself | `GOWORK=off go build -o ./bin/ptah ./cmd/ptah` |
| You want the latest module version in another project | `go install go.5x5.cz/ptah/cmd/ptah@latest` |
| You want a reproducible CI toolchain | Pin a version or pseudo-version in the install command |

Ptah is pre-GA, so pinning is better for automation than relying on `latest`.
Install `go.5x5.cz/ptah/cmd/ptah-compat` separately when replacing an
Atlas binary in existing scripts.

## Build from a checkout

From the repository root:

```bash
GOWORK=off go build -o ./bin/ptah ./cmd/ptah
./bin/ptah version

GOWORK=off go build -o ./bin/ptah-compat ./cmd/ptah-compat
./bin/ptah-compat migrate --help
```

Expected shape:

```text
Version: ...
Commit: ...
Date: ...
Go: ...
Platform: ...
```

`ptah --version` and `ptah -v` print the same block, so a script does not have
to know which spelling it used in order to parse the answer.

Use the local binary in examples:

```bash
./bin/ptah schema render --root-dir ./examples/viz/models --dialect postgres
```

## Install with Go

```bash
go install go.5x5.cz/ptah/cmd/ptah@latest
ptah version

go install go.5x5.cz/ptah/cmd/ptah-compat@latest
ptah-compat migrate --help
```

If `ptah` is not found after `go install`, add `$(go env GOPATH)/bin` to your
`PATH`.

```bash
export PATH="$(go env GOPATH)/bin:$PATH"
ptah version
```

## Optional tools

Some Ptah features need extra local tools:

| Tool | Needed for |
| --- | --- |
| Graphviz `dot` | `ptah viz --format svg` |
| Database client/server | Live `db`, `schema compare`, and `migrations` workflows |
| Atlas CE binary | Differential checks in the external conformance repository |
| `ptah-ls` | Annotation hover, completion, and diagnostics in your editor |

Mermaid and DOT visualization output do not require Graphviz.

Install the annotation language server with
`go install go.5x5.cz/ptah/cmd/ptah-ls@latest` and wire it into your
editor as described in
[Editor support](../../reference/go-annotations/#editor-support).

## Verify command shape

```bash
ptah --help
ptah migrations --help
ptah-compat migrate --help
```

The native `ptah` binary has no Atlas command paths: Atlas spellings such as
`ptah migrate apply` or a `ptah atlas` namespace are intentionally not part of
its command tree.

Use `ptah-compat <command> ...` when a script needs Atlas-style root commands.
For existing scripts that call an executable named `atlas`, the drop-in
install steps are on the
[Atlas compatibility overview](../../atlas/overview/#installing-under-the-name-atlas).

## Next steps

- Run the [Quick start](../quick-start/) for a complete SQLite smoke test.
- Use [Native commands](../../reference/native-commands/) when wiring Ptah into scripts.
- Use [CI](../../testing/ci/) when pinning Ptah in pull-request checks.
