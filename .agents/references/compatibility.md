# Native and Atlas compatibility policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Contents

- [Compatibility Policy](#compatibility-policy)
- [Boolean `PTAH_*` environment variables are strict](#boolean-ptah_-environment-variables-are-strict)
- [A `PTAH_*` value is consumed once, by the surface that decides with it](#a-ptah_-value-is-consumed-once-by-the-surface-that-decides-with-it)
- [Compatibility with older Ptah is not owed](#compatibility-with-older-ptah-is-not-owed)
- [Native And Compatibility Capability Ownership](#native-and-compatibility-capability-ownership)
- [Dependency direction](#dependency-direction)

## Compatibility Policy

Ptah aims to be a drop-in replacement for the Atlas CLI under three rules;
stating only the first is how a capability gets thrown away.

**(a) Never be looser.** What the community binary refuses must not succeed
here. Where Ptah cannot yet implement a construct the community binary enforces,
refuse loudly rather than accept and ignore.

**(b) Matching is the floor, not the ceiling. We do not copy defects.** Where
the community binary silently drops input, corrupts state or fails for an
unrelated reason, do not reproduce it: `-- atlas:txmode none` works without a
blank line after it, and `file()` in `atlas.hcl` stays inside the project,
where the community binary reads `/etc/passwd`. Say so in the commit and
issue: "we are stricter here, deliberately, and here is the measurement."

**(c) Compatibility never removes a capability.** `ptah-compat` is also the
migration path for Atlas Pro pipelines, so:

- the default surface keeps every implemented capability reachable;
- `PTAH_ATLAS_STRICT_COMPAT=1` selects a CE-only policy for reference and
  conformance runs. It refuses extension values; it refuses Atlas txtar, Ptah
  directives and SQL templates where a command executes, converts or replays a
  migration body, but keeps the bytes in a checksum-only read (`migrate hash`,
  `migrate status`, `migrate validate` without `--dev-url`) for `atlas.sum`
  parity; it refuses the known `PTAH_*` toggles, never a `PTAH_*` prefix,
  since `atlas.hcl` reads user input through `getenv`;
- the strict selector is an environment variable, never a flag (the
  `cli-surface` conformance tier asserts flag parity);
- what the default leaves out is reported, not dropped in silence;
- the capability is written down: a feature-matrix row, documentation, a test.

To decide whether to match, ask what it costs the user. A different spelling
of the same outcome (wording, exit codes, flag names, output shape): match. A
loss of something they asked for: do not.

### Boolean `PTAH_*` environment variables are strict

- Absence selects the default; a present value that is not a boolean makes
  the owning command refuse before work. **Never turn a parse error into the
  default.** Use `os.LookupEnv`: `PTAH_X=` is not absence.
- Declare each variable once with `envbool.New(name, default, class)` and
  read it with `Var.Resolve` (`internal/cli/internal/envboolguard` refuses
  `strconv.ParseBool(os.Getenv(...))`). `class` is `Gated` (CE lacks it;
  strict mode refuses it), `Retained` (CE has it; strict mode keeps it) or
  `Selector` (reserved), with a comment on what the pinned binary has. An
  unclassified declaration fails closed.
- **Resolve the variables a command owns before its early returns**, on every
  invocation of each command that recognizes them and no other, so a malformed
  value cannot stay dormant.
- Toggles opt in to the permissive side, so a typo fails closed; a new
  restrictive boolean documents why it cannot be a capability gate.

### A `PTAH_*` value is consumed once, by the surface that decides with it

`internal/cli/internal/cmdadapter` installs the same `PTAH_*` binding on the
native target, which can then read a variable its adapter scoped away. **When
an adapter resolves a native flag's whole value, disable the target's binding
with `cmdflags.DisableEnvBinding`**, and test the refusal plus a control in
which the variable still reaches an unscoped run.

### Compatibility with older Ptah is not owed

Ptah is pre-GA: until v1, prefer the cleaner architecture and keep no
fallback, alias, wrapper, old spelling or second reader for an older shape or
an earlier Ptah's output; update callers, tests and docs. Never soften a
refusal or keep a default to preserve output. Say so in the issue and commit:
"this changes behavior; pre-v1, so no compatibility is owed." This never
justifies breaking parity with the community binary or discarding user data.
It also does not bypass the Stable Embedder API's reviewed breaking-change
approval in [the public API ledger](../../docs/public_api.md).

## Native And Compatibility Capability Ownership

`ptah-compat` is an adapter over Ptah capabilities, not a second product; the
`ptah` binary stays purely native.

- A **general capability** means something without Atlas (schema plan
  testing, migration testing, drift detection, schema security analysis,
  checkpoints, pre-apply checks, planning, validation, artifact publishing,
  directory import). It lives in a shared package below the CLI, reachable
  natively, never only in `internal/cli/atlas`.
- **Compatibility machinery** may stay compat-only: `atlas://` resolution,
  flag spelling and precedence, `atlas.hcl` evaluation, the `.plan.hcl` and
  `.test.hcl` codecs, revision-table and checksum format, Atlas-shaped output,
  exit codes, diagnostics. A codec feeding a shared capability is the
  intended shape.
- Adapt the compat surface to native capabilities; never narrow a native one
  to what Atlas can express. A deliberate divergence is a policy the caller
  picks in the shared package, not a second implementation.
- A capability reaches the native surface as a verb or flag; the compat
  surface takes no new flag and keeps fuller behavior behind a `PTAH_*`
  variable, as `PTAH_ALLOW_EXTERNAL_SCHEMA` does.
- **Every PR that adds behavior under the compat surface says which it is**:
  `GENERAL CAPABILITY` (where it lives, how native Ptah uses it, and if no
  native surface is added, why and which issue tracks it) or `COMPATIBILITY
  ADAPTER` (naming the Atlas contract). This applies to new work.

### Dependency direction

Native code and shared packages never import `internal/cli/atlas`. **No gate
checks this**: `scripts/check-architecture-boundaries.sh` and
[`docs/architecture_boundaries.md`](../../docs/architecture_boundaries.md) do not
name it. Besides `cmd/ptah-compat`, only `internal/cmdrefviews` imports it, to
show what strict mode removes, and says why at the top of the file; a second
importer needs a written reason too.
