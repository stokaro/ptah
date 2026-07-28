---
title: Atlas compatibility overview
description: How the ptah atlas command surface and the ptah-compat binary relate to native Ptah, and how Atlas-style flags translate.
---

You run scripts, CI jobs, or habits built around the Atlas CLI and want to know
how Ptah fits them. This page explains the two Atlas-compatible surfaces, how
Atlas-style flags translate to Ptah concepts, and where the compatibility
boundary is — before you pick up the per-command usage pages.

## Command surfaces

Atlas-compatible command paths live under `ptah atlas <command> ...` inside the
native Ptah CLI tree.

The separate `ptah-compat` binary is a binary-level drop-in replacement for
scripts that need Atlas-style root commands, including scripts that call an
executable named `atlas`:

```bash
install_dir="$(go env GOPATH)/bin"
ln -sf "$(command -v ptah-compat)" "$install_dir/atlas"
atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Ptah does not add root-level Atlas spellings such as `ptah migrate apply` or
`ptah schema inspect` to the native `ptah` binary. Those paths are intentionally
invalid because the native Ptah command tree is being designed separately before
GA. When converting scripts, keep the `atlas` namespace in the Ptah command:

| Do | Do not |
| --- | --- |
| `ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `ptah atlas schema inspect --url "$DATABASE_URL"` | `ptah schema inspect --url "$DATABASE_URL"` |

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR for supported `ptah atlas ... --env` defaults |
| `--config`, `-c` | Local Atlas project config path for `schema` and `migrate` commands |
| `--var name=value` | Atlas HCL variable override for supported local expressions |
| Atlas revision table mode | Ptah revision format and table settings |

Atlas project flags are persistent on the Atlas-compatible `schema` and
`migrate` command groups, so both of these forms are valid:

```bash
ptah atlas migrate --config project.hcl --env local hash
ptah atlas migrate hash --config project.hcl --env local
```

The supported `atlas.hcl` subset those flags read is documented in
[Atlas project config](../project-config/).

Atlas OSS shorthand aliases are part of the compatibility surface. Ptah accepts
`-u` for `--url`, `-c` for `--config`, `-s` for `--schema` on Atlas commands
that register schema selection, and `-f` for `schema diff --from`. `schema apply`
also accepts Atlas's hidden `--file/-f` input alias for local HCL or SQL paths;
prefer `--to` in new Ptah-authored scripts.

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah atlas version` | Prints Ptah build information. |
| `ptah atlas license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah atlas completion <shell>` | Generates Cobra completion output for the full `ptah` command tree, including the Atlas-compatible namespace. |

## Format reports and redaction

Atlas-compatible `--format` reports use the same data shape for `ptah atlas`
and `ptah-compat`. URL fields render as redacted URL strings in Go templates
such as `{{ .Env.URL }}`, but `{{ json . }}` emits an Atlas-like URL object
with `Scheme`, `User`, `Host`, `Path`, `RawQuery`, `Fragment`, `RawPath`,
`RawFragment`, `ForceQuery`, `OmitHost`, and, for SQLite URLs, `Schema`. Query
keys that look like passwords, tokens, secrets, or API keys are replaced with
`xxxxx`; URL userinfo passwords are removed.

The per-command template fields are listed on
[Atlas migrate commands](../migrate-commands/#format-template-fields) and
[Atlas schema commands](../schema-commands/#format-template-fields).

## Compatibility boundaries

Some Atlas Pro commands and flags are bound to Atlas Cloud services Ptah
intentionally has no counterpart for. Those paths are recorded waivers or Atlas
CE boundary stubs: they are registered so scripts fail in the right namespace,
and they reject execution loudly with their rationale instead of being silently
ignored. The largest examples are `migrate push` and `schema push` (the Atlas
Registry protocol is proprietary and account-bound; the native
`ptah migrations push` and `ptah schema push` verbs publish to any
[OCI registry](../../workflows/oci-registry/) instead) and the `schema plan`
registry sub-verbs. The per-command pages name each waiver where it appears.

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../conformance/) for current evidence and
[Comparison](../comparison/) for tracked product, coverage, and
documentation gaps.

## Next steps

- Running migration directories with Atlas verbs:
  [Atlas migrate commands](../migrate-commands/).
- Inspecting, diffing, and applying schemas with Atlas verbs:
  [Atlas schema commands](../schema-commands/).
- Evaluating the compatibility claims first:
  [Conformance](../conformance/).
