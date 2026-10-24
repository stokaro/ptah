# ADR 0030: ptah-compat resolves providers through the native pipeline

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0026](0026-official-providers-are-pinned-in-the-release-installed-explicitly-and-bundled-by-the-distribution.md)

## Context

`ptah-compat` is an adapter over Ptah capabilities, not a second product
(`AGENTS.md`). It keeps the Atlas CLI's flag spelling and output, and takes new
behavior through `PTAH_*` variables, never through flags. `PTAH_ATLAS_STRICT_COMPAT=1`
selects a community-edition-only policy that refuses what the pinned binary lacks.

What the code does today:

- Provider selection is a native-command feature. `providercommand.Configure`
  (`internal/cli/internal/providercommand/provider.go`) wraps a command's `RunE`,
  adds `--provider-config` and `--provider-engine` with their environment binding
  disabled, loads the manifest, picks the engine from `--db-url`, `--dialect` or
  `--dev-url`, starts the provider, and puts these on the command's
  context: a runtime (`engineruntime.With`), native connection bindings
  (`dbschema.WithNativeConnections`) and the dev-database owner
  (`devdocker.WithExternal`). It is applied to `schema apply`, `schema plan`,
  `schema inspect` and the `migrations` verbs (`internal/cli/schema/schema.go`,
  `internal/cli/migrations/provider.go`). No file under `internal/cli/atlas`
  uses it.
- Without a selection, `engineruntime.Resolve` returns `builtin.Bundled()`, the
  embedded engines. `servedEmbedded` in `providercommand` is the one place that
  decides an official engine still runs embedded; ADR 0026 says the flip means
  returning false there for YDB.
- A selection is exclusive. `engine.New(host.Provider())` builds a runtime with the
  provider's targets only, and `WithNativeConnections` refuses every URL scheme
  that is not registered, "including schemes for bundled drivers". A command that
  names a YDB URL and a SQLite dev URL, or compares YDB with PostgreSQL, cannot
  run under a selected provider today.
- The compat commands connect in their own code (`dbschema.ConnectToDatabase`,
  `devdocker.Resolve` in `migrate_diff.go`, `migrate_down.go`, `migrate_lint.go`)
  and read the dialect from the URL they resolved, possibly from `atlas.hcl`
  `env` blocks, late in the command. A selector that is read from flags before
  `RunE`, as `providercommand` does, cannot know it.
- YDB shows up in compat in three places: `script.go` (`rowCountsUnreported`,
  because `ydb-go-sdk` reports a row count it did not measure),
  `table_rebuild.go` (the `PTAH_ALLOW_TABLE_REBUILD` variable, whose help text names
  YDB), and the strict connect gate (`compat_strict_connect_gate.go`), which
  refuses a dialect the pinned binary has no driver for, such as YDB. Behind them,
  everything that makes `ydb://` work is the embedded YDB engine that the compat
  binary links through `engine/builtin`.
- Several shared packages hold YDB behavior that compat reaches: `tablerebuild`,
  `secretrotation` (`ydbsecret.WithRotations`), `migratetimeout`, `devsnapshot`.

The brief has four requirements: the same resolution as native (discovery,
configuration, lifecycle), Atlas CLI compatibility intact with no new flags, the
strict rules respected, and no separate built-in YDB implementation left in
`ptah-compat` after the flip.

## Decision

### One resolver, two thin callers

Extract the body of `providercommand.prepare` into a package of its own,
`internal/providerselect` (name for the implementation to confirm). It owns:

- loading the manifest and the selection order of ADR 0026 (manifest entry,
  bundled provider, user store, `provider_missing`);
- starting a provider and building the provider's runtime, native connection
  bindings and dev-database owner;
- the lifecycle: one process per provider per command invocation, closed when the
  invocation ends by return, error, panic or cancellation, with the process-death
  guarantees the SDK already has (the parent-death watch, bounded close).

`providercommand.Configure` becomes a caller that reads the two flags and passes
the result. The compat adapter becomes the second caller. This is a `GENERAL
CAPABILITY` in the sense of `AGENTS.md`: the semantics sit in a shared package
below the CLI layer, and native consumes it. The compat part is a `COMPATIBILITY
ADAPTER` that interprets the Atlas contract only where it chooses the manifest and
the moment to start.

### Start on first need

Compat decides the engine late, so the resolver is lazy. The adapter installs, on
the context of every compat command (the same recursive installation the strict
connect gate uses), a `providerselect.Lazy` value. `dbschema.ConnectToDatabase` and
`devdocker.Resolve` consult it when the URL's scheme or the `docker://` engine is
served by an official provider and not by a built-in engine. The first consult
resolves and starts that provider; later ones reuse it. A command that never
names a provider-served engine starts no process. The closing hook runs when the
compat command returns, including on interrupt, which `migrate apply` already
handles (`integration/atlas_migrate_apply_interrupt_unix_e2e_test.go`).

Native commands keep the eager path through their flags, and may later take the
lazy value too, so there is one lifecycle. The two paths must pass the same tests.

### Configuration without a flag

Compat adds no flag. It selects the manifest with the variable
`PTAH_PROVIDER_CONFIG`, read only by the compat surface, naming one manifest file.
When it is unset, selection starts at the bundled provider (rule 2 of ADR 0026).
Most users never set it. It exists for the same cases the native `--provider-config`
exists for: a provider build that is not the pinned one, and an offline host.
There is no `atlas.hcl` key for it. The precedent is `PTAH_ALLOW_EXTERNAL_SCHEMA`
and `PTAH_ALLOW_TABLE_REBUILD`: the compat surface takes no flag the pinned binary
lacks, so it reads a variable.

Engine selection needs no variable. It comes from the connection URL the command
resolved, which is how native commands choose it when `--provider-engine` is
absent (`targetSelector`).

The compat tree has no `providers` command. Compat adds no command that the pinned
binary lacks. The missing-provider diagnostic tells the user to run
`ptah providers install ydb`, and the distribution ships both binaries.

### Composition replaces exclusivity for default selection

A default selection (rules 2 and 3, and a manifest entry reached without an
explicit engine flag) adds the provider's targets to the built-in runtime and
registers the provider's URL schemes beside the built-in drivers. A command may
then name a YDB URL and a PostgreSQL URL, or a YDB URL and a SQLite dev URL, as it
does with the embedded engine today. An explicit native `--provider-engine`
selection keeps the exclusive behavior ADR 0021 states, because the operator asked
for exactly that provider.

This is an amendment, and it names the text it changes:

| Text | Change |
| ---- | ------ |
| ADR 0021: "An explicit native selection owns its connection scope. Unregistered schemes are refused even when a bundled driver exists." | Kept for an explicit selection. A default selection is additive. |
| ADR 0021: "No working-directory, PATH, environment, or implicit project-file discovery authorizes a binary. The operator supplies the manifest path explicitly." | Narrow exception: `PTAH_PROVIDER_CONFIG` identifies an explicit manifest file, as `--provider-config` does. It authorizes nothing by itself. The manifest entry (absolute executable path, SHA-256, provider ID, engines) and the checks of ADR 0026 authorize the binary: pinned digests, identity and integrity verification before every start, protocol and feature checks. Nothing is searched. No implicit fallback exists: a named manifest that fails to load, or an entry that fails verification, ends the search. |

`dbschema.WithNativeConnections` gains an additive mode. The engine runtime gains
a way to extend the built-in runtime with a provider's targets. Both keep the
rule that a registration conflict is an error and never a precedence rule.

### Strict compatibility

Strict mode keeps its meaning: the pinned community binary has no YDB driver, and
`ValidateDialect` refuses `ydb://` before any connection. The lazy resolver sits
behind that gate, so strict mode never starts a provider. In addition:

- `PTAH_PROVIDER_CONFIG` is registered as a gated variable. A strict run that sets
  it is refused by name, like the other known feature toggles, and the strict
  registry test (`internal/atlascompatpolicy`) lists it.
- No arbitrary `PTAH_*` name is refused by prefix.
- The default surface keeps YDB reachable. Compatibility never removes a
  capability, and the default surface does not become CE-only to simplify this
  ADR. The community binary has no YDB driver, so no Atlas behavior is being
  reproduced for the YDB path. The compat command shape (flags, exit codes, output
  layout) is reused, and the provider diagnostic is the one place that differs,
  because nothing in the pinned binary corresponds to it.

### Output and exit codes

A `provider_missing`, `provider_corrupt` or `provider_incompatible` failure surfaces
through the compat command's normal error path, with the same exit code a failed
connection gives, and the same machine-readable diagnostic fields as native
(`internal/providerdiagnostic`). It prints no URL and no credential.

### The built-in YDB implementation goes

The flip in ADR 0026 removes YDB from `engine/builtin` and `internal/builtindialects`,
and the packages that serve only YDB. For `ptah-compat` that is satisfied when:

- `go list -deps ./cmd/ptah-compat ./cmd/ptah ./cmd/ptah-ls` lists no YDB package.
  The package set is every non-test package that `git ls-files` names with `ydb` in
  its path, plus `internal/yqlparse` and `internal/syntaxext/yql`, and a guard
  holds that set above a floor.
- The compat-side YDB conditionals move behind provider facts. `rowCountsUnreported`
  becomes a session capability the provider reports (a present `false` for
  `execute.row-counts`), and the compat script runner reads it. A provider
  that does not report it leaves the count unknown, which is the existing
  "unreported" behavior.
- Shared YDB behavior reached from compat is moved by its own decision or listed
  as blocking the flip: table rebuild policy input, secret rotation, migrate
  timeout, and dev snapshot realm handling. Each needs a provider-declared
  planning or session option, and none is solved here. The owner has to decide
  whether ADR 0024 or a new record carries them (open questions).

## Alternatives considered

**Give compat the native flags.** `--provider-config` and `--provider-engine` on
compat commands. It copies the native shape exactly. It adds flags the pinned
binary lacks, which the `cli-surface` conformance tier forbids. Rejected.

**Make the native binary the only YDB surface.** `ptah-compat` refuses `ydb://`
and users run `ptah`. It is the smallest change, and it removes YDB from the
Atlas-compatible migration path, which `AGENTS.md` rules out: compatibility never
removes a capability. Rejected.

**Wrap each compat command in `providercommand.Configure`.** It reuses the code,
but the engine is read from flags before `RunE`, and compat resolves URLs from
`atlas.hcl` inside the command. Commands would start a provider for URLs that
turn out to be SQLite, or miss one. The lazy path avoids both. Rejected.

**A separate provider system for compat.** A second manifest format, a second
store and a second process manager. It is the "second plugin system" the brief
rules out. Rejected.

**Bundle only; no manifest for compat (rejected by the owner).** Compat uses rules 2 and 3 only, and
anyone who needs another build uses native `ptah`. It avoids the environment-variable
question. The cost is that an Atlas-shaped pipeline cannot take a hotfixed
provider, and an offline host cannot select a verified executable by path. It
stays the fallback if the owner rejects the variable.

## Consequences

- Provider lifecycle is written once. A fix to process cleanup reaches both
  surfaces.
- Mixed-engine commands work with a default selection. That requires the additive
  runtime, which is new code in `engine` and `dbschema`.
- The compat tree stays free of provider flags. The help output and the flag
  inventory do not change, and the surface test proves it.
- A user who has only `ptah-compat` and no bundled provider (a source build) gets
  a diagnostic that names a command the compat tree does not have.
- Strict mode gains one registered variable.

## Acceptance criteria

These are for the future implementation. None holds today.

- A compat command that names `ydb://` starts the bundled provider on first
  connect, and the process exits when the command returns, on error, on panic and
  on interrupt. A command that names only SQLite starts none (process count read
  from the provider host in a test).
- `migrate diff --dev-url docker://ydb/...`, `migrate lint` and an `atlas.hcl`
  `docker` block for YDB provision through the provider's `ProvisionDevDatabase`
  and release the lease at the end. The test reads the container list before and
  after.
- A compat command with a YDB URL and a PostgreSQL URL runs under a default
  selection. A native command with `--provider-engine ydb` and a PostgreSQL URL is
  refused, as ADR 0021 says.
- `PTAH_PROVIDER_CONFIG` selects the manifest; a wrong digest is refused with no
  fallback; a disabled entry is not replaced by the bundled provider. The same
  cases pass through the native flags with the same diagnostics.
- With `PTAH_ATLAS_STRICT_COMPAT=1`, `ydb://` is refused in the pinned binary's
  words before any provider starts, and a set `PTAH_PROVIDER_CONFIG` is refused by
  name. A control shows the variable still reaches an unscoped default run, so the
  refusal is not the variable being dropped (the two-test shape `AGENTS.md`
  requires).
- The `cli-surface` conformance tier shows no flag added to the compat tree. The
  feature-matrix row and the compat documentation describe the variable.
- `go list -deps` for the three binaries contains no YDB package, held by a guard
  with a floor on the checked package count, and `ptah-compat` passes its existing
  YDB integration tests with a provider and no embedded engine.
- Four YDB behaviors in shared CLI packages are tracked here because they block
  the flip and belong to no other record: the table-rebuild policy input
  (`tablerebuild`, `PTAH_ALLOW_TABLE_REBUILD`), secret rotation (`secretrotation`),
  the migrate timeout (`migratetimeout`) and the dev snapshot realm (`devsnapshot`).
  Each moves behind a provider-declared planning or session option, or into the
  plugin, with a test through the shipped binary. The flip does not happen while
  `go list -deps` of any binary still reaches `ydbsecret` or another YDB package.
- The pull request that adds the lazy resolver says `GENERAL CAPABILITY`
  (`internal/providerselect`, consumed by native commands) and the compat hook says
  `COMPATIBILITY ADAPTER`, naming the Atlas flag and output contract it preserves.

## Decided questions

- `PTAH_PROVIDER_CONFIG` is accepted with the exception in the amendment table.
- The missing-provider message names `ptah providers install <engine>` only.
- Default selection is additive; an explicit `--provider-engine` stays exclusive.
