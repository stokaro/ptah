---
title: Install Ptah
description: Install, build, and verify the Ptah CLI before using it in a project.
---

Use this page to get a `ptah` binary onto a developer machine or CI runner.

## Choose an install path

| Situation | Recommended command |
| --- | --- |
| You are developing Ptah itself | `GOWORK=off go build -o ./bin/ptah ./cmd/ptah` |
| You want the latest module version in another project | `go install github.com/stokaro/ptah/cmd/ptah@latest` |
| You want a reproducible CI toolchain | Pin a version or pseudo-version in the install command |

Ptah is pre-GA, so pinning is better for automation than relying on `latest`.

## Build from a checkout

From the repository root:

```bash
GOWORK=off go build -o ./bin/ptah ./cmd/ptah
./bin/ptah version
```

Expected shape:

```text
ptah version ...
```

Use the local binary in examples:

```bash
./bin/ptah schema render --root-dir ./examples/viz/models --dialect postgres
```

## Install with Go

```bash
go install github.com/stokaro/ptah/cmd/ptah@latest
ptah version
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

Mermaid and DOT visualization output do not require Graphviz.

## Verify command shape

```bash
ptah --help
ptah migrations --help
ptah atlas migrate --help
```

Atlas-compatible commands are nested under `ptah atlas <command> ...`. Root-level
Atlas spellings such as `ptah migrate apply` are intentionally not part of the
supported command tree.

## Next steps

- Run the [Quick start](../getting-started/) for a complete SQLite smoke test.
- Use [Commands](../reference/commands/) when wiring Ptah into scripts.
- Use [CI](../workflows/ci/) when pinning Ptah in pull-request checks.
