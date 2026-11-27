---
name: ptah-compatibility
description: Use when changing ptah-compat commands, Atlas flag or output parity, atlas.hcl, strict CE mode, environment forwarding, revision or checksum formats, or the ownership of behavior shared with native Ptah.
---

# Ptah compatibility

## Name the surface and contract

Identify the native command, compatibility command, or shared capability the
request changes. Read the applicable section of
[compatibility policy](../../references/compatibility.md), then inspect the
command's production path under `internal/cli/atlas`, its native target, and
the shared implementation. Use the current command tree and help as evidence
for flags and environment bindings.

For a native-only CLI change, start with its native leaf package and shared
helpers. Inspect the adapter only if it forwards to or shares that behavior.

Classify added compatibility behavior in the PR:

- `GENERAL CAPABILITY`: name the shared implementation and native entry point.
  If a native surface is deferred, give the reason and the issue that owns it.
- `COMPATIBILITY ADAPTER`: name the Atlas spelling, codec, layout, output, or
  policy contract the adapter implements.

General behavior belongs below both CLIs. Do not implement planning, testing,
validation, or publishing only in the adapter. Native and shared packages must
not import `internal/cli/atlas`; the architecture gate does not enforce this
particular dependency rule, so inspect imports and callers yourself.

## Compare the right policies

- Preserve every implemented capability on the default compatibility surface.
  Use `PTAH_ATLAS_STRICT_COMPAT` for the CE policy; do not add a compatibility
  flag or narrow native behavior to fit Atlas.
- Match the pinned community reference for spelling, exit codes, flags, and
  output shape. Read the applicable conformance evidence before changing a
  parity claim. A guessed response or an unrelated release is not evidence.
- Refuse unsupported semantics rather than accepting and ignoring them.
  Preserve deliberate stricter behavior that prevents data loss or unsafe
  input handling; document its measured difference from the reference.
- Distinguish execution or replay from checksum-only reads. Strict policy may
  refuse a body at execution while preserving its bytes for checksum parity.
  Read the existing command-specific contract before sharing a preflight.
- Keep native lint identities separate from Atlas check codes. Read
  `internal/lintnames` and `internal/atlascodes` only when that mapping changes;
  never import the code mapping into native or shared semantics.

## Resolve environment input once

Use `internal/envbool` for boolean `PTAH_*` variables. Absence, empty value,
invalid input, explicit false, and explicit true are distinct cases. Declare
the variable's classification and resolve it before the owning command's early
returns; a malformed value must not become a default.

Inspect `internal/cli/internal/cmdadapter` and
`internal/cli/internal/cmdflags` when an adapter forwards to a native command.
When the adapter resolves a flag's complete value, disable the target's
environment binding with `cmdflags.DisableEnvBinding`. Test both the scoped
refusal and a control where an unscoped run still receives the variable.
Preserve explicit CLI precedence and avoid blanket rejection of user-defined
`PTAH_*` names that HCL may read as input.

## Exercise the production boundary

Start with the command or exported adapter path that exhibited the defect.
Use helpers to prepare fixtures, not to bypass the preflight under test.
Choose the cases whose policy or input handling the change affects:

- Default and strict mode, including the capability that must remain available.
- Omitted, empty, malformed, and explicit environment/flag values, plus an
  explicit flag that must win over its environment value.
- A valid input accepted by the reference and an unsupported shape that must
  refuse before a database or filesystem effect.
- Output, error class, exit code, and durable result as separate assertions
  where those are distinct parts of the contract.
- Portable paths, quoting, or executable invocation when the fixture crosses
  platforms. Use repository helpers instead of OS-specific expected prose.

Follow [test policy](../../references/testing.md) for regression controls and
engine-level evidence. Keep provider contract tests separate from live plugin
tests. Use the validation policy to select local checks and the CI contours.

## Keep claims aligned

Update affected native and compatibility references, configuration docs, and
feature-matrix source rows when the behavior or parity verdict changes. State
which policy and reference evidence were tested. Do not claim full Atlas parity
from a focused fixture, or preserve an older Ptah alias without a current need.
