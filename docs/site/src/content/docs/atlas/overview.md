---
title: Atlas compatibility overview
description: How the ptah-compat drop-in binary replaces the Atlas CLI, and how Atlas-style flags translate to native Ptah concepts.
---

You run scripts, CI jobs, or habits built around the Atlas CLI and want to know
how Ptah fits them. Ptah ships a separate drop-in binary, `ptah-compat`, that
presents the Atlas-compatible command surface. This page explains how to
install it, how Atlas-style flags translate to Ptah concepts, and where the
compatibility boundary is — before you pick up the per-command usage pages.

## The drop-in binary

The main `ptah` binary is a purely native CLI — it has no Atlas-style command
paths. The separate `ptah-compat` binary is a binary-level drop-in replacement
for the Atlas CLI, built for scripts that need Atlas-style root commands:

```bash
go install go.5x5.cz/ptah/cmd/ptah-compat@latest

ptah-compat migrate apply --url "$DATABASE_URL" --dir ./migrations
```

Command examples on the Atlas compatibility pages are written as
`ptah-compat <command> ...` — the name the binary ships under.

What the two binaries share is capabilities, not command lines. A generally
useful capability you reach through `ptah-compat` is reachable through native
`ptah` as well, under native names and flags: `ptah-compat migrate apply` and
`ptah migrations up`, or `ptah-compat schema inspect` and `ptah schema inspect`.
Atlas-specific machinery has no native twin at all.

Use the native tree for new Ptah-authored work and the compat binary for
existing Atlas scripts; the per-verb mapping is listed in the
[Atlas-compatible commands reference](../../reference/atlas-commands/).

### Capability parity, not interface parity

Ptah's Atlas compatibility layer does not define a separate feature set.

Capabilities implemented for Atlas compatibility are also available through
Ptah's native workflows when they are generally useful database-schema or
migration capabilities.

The interfaces may differ: `ptah-compat` preserves Atlas-shaped commands and
compatibility contracts, while `ptah` uses Ptah-native commands and
configuration.

Atlas-specific adapters and compatibility representations — for example
`atlas://` resolution, Atlas file and config codecs, revision-history
compatibility, or Atlas-specific CLI and output behavior — are not duplicated
in the native interface unless they have independent Ptah value.

So the promise is about capabilities, not about command lines. The native
binary accepts no Atlas CLI aliases, and the two binaries are not
command-for-command equivalent: Atlas command spellings live only in
`ptah-compat`. The [command parity table](#command-parity) below shows which
native verb answers each Atlas one.

## Command parity

| Task | Native Ptah | `ptah-compat` | Atlas OSS |
| --- | --- | --- | --- |
| Apply migrations | `ptah migrations up` | `ptah-compat migrate apply` | `atlas migrate apply` |
| Roll back migrations | `ptah migrations down` | `ptah-compat migrate down` | `atlas migrate down` |
| Migration status | `ptah migrations status` | `ptah-compat migrate status` | `atlas migrate status` |
| Hash migrations | `ptah migrations hash` | `ptah-compat migrate hash` | `atlas migrate hash` |
| Validate migrations | `ptah migrations validate` | `ptah-compat migrate validate` | `atlas migrate validate` |
| Lint migrations | `ptah migrations lint` | `ptah-compat migrate lint` | Pro CLI feature, basic Open rule set [^lint] |
| Create an empty migration | `ptah migrations create` | `ptah-compat migrate new` | `atlas migrate new` |
| Set revision state | `ptah migrations set` | `ptah-compat migrate set` | `atlas migrate set` |
| Checkpoint / squash migrations | `ptah migrations checkpoint` | `ptah-compat migrate checkpoint` | Pro only |
| Inspect schema | `ptah db read` | `ptah-compat schema inspect` | `atlas schema inspect` |
| Diff schema | `ptah schema compare` | `ptah-compat schema diff` | `atlas schema diff` |
| Format schema files | `ptah schema fmt` | `ptah-compat schema fmt` | `atlas schema fmt` |
| Clean schema objects | `ptah db drop-all` | `ptah-compat schema clean` | `atlas schema clean` |
| Atlas CE community-version unsupported commands | Not native Ptah features | `ptah-compat migrate push`, `ptah-compat schema push`, `schema plan` registry sub-verbs | Registered, unsupported [^ce] |

[^lint]: Current Atlas docs list the migration linting CLI feature as Pro while
    the same feature availability page also lists a basic Open lint-rule set.

[^ce]: Atlas CE registers these command paths and reports the community-version
    unsupported boundary. `migrate test`, `schema test`, `migrate edit`,
    `migrate rebase`, `migrate rm`, and `schema plan` forward to or implement
    native Ptah behavior instead of aborting.

Some Atlas command paths are registered before complete runtime behavior
exists, and some accepted Atlas flags fail explicitly rather than being silently
ignored. The [Feature matrix](../feature-matrix/) carries the status of each,
with the tracking issue where one is open.

For a page-by-page crosswalk against the official Atlas documentation, see
[Atlas docs coverage](../docs-coverage/).

## What Atlas keeps outside its community build

Atlas has both open and commercial feature sets, and which side a capability
falls on decides whether `ptah-compat` is replacing something the community
binary does or something it refuses. The Atlas
[feature availability](https://atlasgo.io/features) page lists database
inspection, schema diffing, versioned migrations, and declarative migrations as
open CLI features. The same page lists the migration linting CLI feature as Pro
while also listing a basic Open lint-rule set.

Checkpoints, visualization, interactive migrations, testing, deployment
rollout, database security as code, and declarative data management are listed
as Pro features. The [Feature matrix](../feature-matrix/) states, per
capability, what Ptah does, what the pinned community binary does, and what
Atlas keeps outside that build.

## Strict Community Edition mode

The normal `ptah-compat` process keeps every Atlas Pro-like and best-effort
capability Ptah implements. That is the migration surface for existing Atlas
pipelines, so it is also the default.

For a pinned Atlas Community Edition oracle or conformance run, select the
separate strict policy before starting the process:

```bash
PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat schema inspect --help
```

Strict mode changes command construction, not only runtime validation. Its help
tree exposes CE commands and flags, and gated verbs use a Ptah-owned diagnostic
that tells the operator to unset `PTAH_ATLAS_STRICT_COMPAT` for the full
compatibility surface. It never links to an Atlas installer. Ptah's generic
`PTAH_<FLAG>` environment twins are disabled. A present extension variable is
rejected before help, version, argument handling, configuration, filesystem, or
database work. The selector has no CLI flag, so it cannot change the surface
being measured.

This validation targets known Ptah flag bindings and feature toggles. It does
not reserve the whole `PTAH_*` namespace: values read explicitly by an
`atlas.hcl` `getenv` expression remain project inputs in strict mode.

Strict mode also refuses authored or inspected content that CE cannot represent
safely. This includes Pro-only schema objects and extended `atlas.hcl`
evaluation. Strict schema workflows refuse YAML sources and an authored
`schema apply` lint policy that the CE path cannot enforce.

Commands that execute, convert, or replay migration bodies refuse Atlas txtar,
Ptah directives, and SQL templates; checksum-only reads preserve those bytes.
A live Pro-only object stops schema inspect, apply, diff, or clean before
output, comparison, or mutation. Inspect, apply planning, and database-backed
or replayed schema- and migration-diff sources supplement the ordinary reader
with a read-only inventory of catalog-only kinds in the selected schema scope.
Cleanup validates the writer's full destruction inventory, including dependent
objects and the same PostgreSQL catalog kinds absent from the schema reader.

Strict mode never emulates a CE behavior that would silently drop authored
data, hide a live object, or corrupt state. Default mode retains every listed
extension. Deliberate safety and correctness improvements remain enabled and
are listed in [Retained divergences](../retained-divergences/).

Do not enable strict mode in ordinary migrated Pro pipelines. To verify both
contracts, run CE parity tests with the variable set and Pro-retention tests
with it absent. Native `ptah` does not read this variable.

### Installing under the name `atlas`

For a byte-level drop-in with existing scripts that call an executable named
`atlas`, install the binary under that name:

```bash
# Build it under the name your scripts expect:
go build -o atlas ./cmd/ptah-compat

# Or install it and link the atlas name:
go install go.5x5.cz/ptah/cmd/ptah-compat@latest
install_dir="$(go env GOPATH)/bin"
ln -sf "$install_dir/ptah-compat" "$install_dir/atlas"
```

The binary adopts the name it is invoked as, so usage, help, and error output
read `atlas migrate apply ...` when the executable is named `atlas`.

## Translation model

Implemented Atlas-compatible commands either execute dedicated Atlas-shaped
behavior or translate Atlas-style flags into the closest native Ptah command
model. Unsupported flags fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR for supported `ptah-compat ... --env` defaults |
| `--config`, `-c` | Local Atlas project config path for `schema` and `migrate` commands |
| `--var name=value` | Atlas HCL variable override for supported local expressions |
| Atlas revision table mode | Ptah revision format and table settings |

Atlas project flags are persistent on the Atlas-compatible `schema` and
`migrate` command groups, so both of these forms are valid:

```bash
ptah-compat migrate --config project.hcl --env local hash
ptah-compat migrate hash --config project.hcl --env local
```

The supported `atlas.hcl` subset those flags read is documented in
[Atlas project config](../project-config/).

Atlas OSS shorthand aliases are part of the compatibility surface. Ptah accepts
`-u` for `--url`, `-c` for `--config`, `-s` for `--schema` on Atlas commands
that register schema selection, and `-f` for `schema diff --from`. `schema apply`
also accepts Atlas's hidden `--file/-f` input alias for local HCL or SQL paths;
prefer the native `ptah schema apply` verb in new Ptah-authored scripts.

## Utility commands

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat version` | Prints Ptah build information. |
| `ptah-compat license` | Prints Ptah MIT license and license-clean Atlas compatibility notice. |
| `ptah-compat completion <shell>` | Generates Cobra completion output for the Atlas-compatible command tree. |

## Format reports and redaction

Atlas-compatible `--format` reports use the Atlas data shape. URL fields render
as redacted URL strings in Go templates such as `{{ .Env.URL }}`, but
`{{ json . }}` emits an Atlas-like URL object with `Scheme`, `User`, `Host`,
`Path`, `RawQuery`, `Fragment`, `RawPath`, `RawFragment`, `ForceQuery`,
`OmitHost`, and, for SQLite URLs, `Schema`. Query keys that look like
passwords, tokens, secrets, or API keys are replaced with `xxxxx`; URL userinfo
passwords are removed.

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
[OCI registry](../../operate/oci-registry/) instead) and the `schema plan`
registry sub-verbs. The per-command pages name each waiver where it appears.

## Compatibility never costs you a capability

Ptah models things the Atlas community CLI does not. PostgreSQL extensions,
sequences and row-level security policies are the clearest examples: that CLI
answers `postgres: extensions are not supported by this version` and refuses a
schema file that declares any of them, while Ptah reads, diffs and applies all
three.

Being a drop-in for that CLI never means giving those up.

The compatibility surface **defaults** to what the community CLI accepts, so
output you hand back to it stays readable. What that default leaves out is
reported rather than dropped in silence — you are told what was omitted and
why, so a compatibility-shaped inspect never describes less of your database
than it found without saying so.

The fuller behavior stays available on the same `ptah-compat` surface through a
`PTAH_*` environment variable. It is an environment variable rather than a flag
on purpose: the compatibility binary's flags are held to parity with the pinned
community CLI, so a flag Atlas does not have would break the very drop-in
promise it was added to serve. Native `ptah` verbs always emit everything Ptah
models, with no switch to set.

This matters most if you are coming from Atlas **Pro** rather than CE. The
compatibility surface is the migration path for Pro scripts and configuration
too, not only CE ones — a capability you could only reach by rewriting your
pipeline against native `ptah` verbs would not be a migration path at all.

The rule runs the other way too. A capability built for Atlas compatibility
does not stay on the compatibility surface: where it is generally useful for
schemas or migrations, native `ptah` reaches it under native names.

The inventory — every variable, its default, what enabling it turns on, and
what the community CLI does on the same input — is
[Extension environment variables](../../reference/extension-variables/).


## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the per-capability status support that claim. Use
[Conformance](../conformance/) for current evidence and the
[Feature matrix](../feature-matrix/) for tracked product, coverage, and
documentation gaps.

## Next steps

- Running migration directories with Atlas verbs:
  [Atlas migrate commands](../migrate-commands/).
- Inspecting, diffing, and applying schemas with Atlas verbs:
  [Atlas schema commands](../schema-commands/).
- Evaluating the compatibility claims first:
  [Conformance](../conformance/).
