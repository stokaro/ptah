# ADR 0026: Official providers are pinned in the release, installed explicitly, and bundled by the distribution

Status: Superseded by [ADR 0031](0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md)

## Context

What the code does today:

- A provider is selected from a manifest the operator writes by hand
  (`internal/providerconfig`, format version 2). An entry names an absolute
  executable path, its SHA-256, a provider ID and the engines it claims. The host
  hashes the file before it starts the process, and the SDK transport hashes it
  again at exec time. After startup the engines the provider declares must match
  the manifest.
- `--provider-config` and `--provider-engine` choose the installation. A disabled
  installation never selects another one. An official engine with no installation
  gives a `provider_missing` diagnostic, and a manifest entry cannot claim an
  official engine name for another provider ID.
- The catalog (`internal/providercatalog`) names the official engines (duckdb,
  spanner, ydb) with their provider ID, repository and setup URLs. `BundledByDefault`
  is set for ydb. The catalog holds no version and no digest.
- Nothing downloads, caches or installs a provider. ADR 0021 states that no
  command downloads, enables, replaces or updates one.
- The manifest has no version field. Provider identity at run time is the provider
  ID plus the engines the process declares.
- Each YDB plugin release publishes one archive per platform
  (`ptah-plugin-ydb_<version>_<os>_<arch>.tar.gz`, `.zip` on Windows),
  `checksums.txt` and `release-lines.json`. The host archives, the Docker image,
  the Homebrew cask, `install.sh`, `install.ps1` and the GitHub Action install only
  `ptah`, `ptah-ls` and `ptah-compat`. The action's `install.sh` does not verify
  checksums.

Issue #4204 asks for a clean break: separate provider repositories, a neutral SDK,
no provider-owned code in the main application, and no fallback to an embedded
implementation. ADR 0024 orders the work as one facet per pull request and says the
host code is deleted after the default flips. ADR 0021 says ordinary commands keep
their embedded implementations until extraction. Read as a product behavior, those
two statements describe a period in which an old and a new YDB engine are both
selectable. This ADR rules that out (see "Sequencing").

## Decision

### Target state

YDB is served by an external provider. The full official distribution contains it.
If the selected provider is absent, corrupt or incompatible, the command fails with
a diagnostic. No other engine, and no embedded YDB implementation, takes its place.

A build from source or `go install` can lack the provider binary. That user runs
`ptah providers install ydb` once. DuckDB and Spanner are not in the distribution
and are installed the same way.

Which engine serves `ydb://` is a product decision made in the Ptah release, not a
runtime probe. The presence of a file in a cache directory never changes the
architecture that runs; it is only the precondition for running the provider that
was already chosen.

### The release pins tested provider versions

A Ptah release pins one tested version of each official provider. For each
platform the pin holds the archive name, the archive SHA-256 and the SHA-256 of
the executable inside it. The pin is a generated Go file in the host sources. The
Ptah version, the provider version, the SDK version and the protocol version are
four separate numbers; the pin relates the first two.

`ptah providers install <engine>` installs the pinned version. It takes no version
argument. Nothing resolves "latest", refreshes the catalog, looks up releases
during normal work, or picks the highest installed version.

Provider hotfixes do not have to wait for a Ptah release. A user can obtain and
verify any provider build, then select it through the manifest with its absolute
path and digest. Installing arbitrary versions by command is a separate future
decision.

### Selection order

Selection starts after the manifest passes the validation it gets today. The first
rule that matches decides; later rules are not consulted.

| Order | Candidate | Source of trust | Result when the candidate is found but fails |
| ----- | --------- | --------------- | ------------------------------------------- |
| 1 | The installation the manifest selects for the engine | Executable path and digest the operator wrote | Refuse. No other candidate is tried. |
| 2 | The bundled official provider | Identity, version and digests pinned in this Ptah release | Refuse. |
| 3 | The official provider in the user store, same identity, version and digest as the pin | The same pin | Refuse. |
| 4 | None | | `provider_missing` with the install instruction for the engine |

- A disabled, missing, corrupt or incompatible installation that the manifest
  selected is never replaced by rule 2 or 3.
- Absence and corruption are different states with different diagnostics. A
  candidate that exists and fails verification ends the search; it is not skipped.
- A cached copy with another version is ignored by rules 2 and 3. Several old
  versions in the store create no ambiguity, because the pin names one directory.
- Nothing is discovered through `PATH`, the working directory or a project file. A
  project setting cannot replace a trusted executable or digest.
- A manifest entry for an official engine must still pass the ownership check, so
  an arbitrary provider cannot take an official identity by being selected. The
  manifest holds no version. The operator's digest is the version, and the claim
  to be a particular provider rests on the operator's decision. This ADR adds no
  stronger identity proof for overrides.

The same rules apply to every consumer of the shared application pipeline: `ptah`,
`ptah-compat` and `ptah-ls` where they use providers.

### Authorization to run code

A provider process starts only on one of three grounds:

1. The user installed a distribution that contains a specific pinned provider.
2. The user ran `ptah providers install` for the pinned official provider.
3. The user selected an installation through the manifest.

Recognizing an engine name or a URL scheme authorizes no download, no search for a
binary and no unverified code.

Behavior of the commands:

- `providers install` is the only command that downloads. It verifies and
  publishes the pinned provider. It does not run the downloaded executable to
  learn anything about it, and it does not edit the manifest.
- `providers list` keeps reading and hashing without execution.
- `providers check` starts only the one provider it was asked to check.
- Every other command never downloads and never updates.

Changes to ADR 0021, which stays as written:

| ADR 0021 provision | Proposed here |
| ------------------ | ------------- |
| "No command downloads, enables, replaces, or updates a provider" | Replaced: one command, `providers install`, downloads one pinned official artifact. No command enables, replaces or updates. |
| Automatic catalog downloads are rejected | Kept. |
| Ordinary commands keep embedded implementations until extraction | Replaced for YDB: no embedded implementation is selectable once YDB is served by a provider. |
| Recognition grants no execution permission | Kept. |
| Manifest version 2 and its validation | Kept. |
| `providers list` does not execute | Kept. |
| `providers check` starts one provider | Kept. |
| No `PATH`, working directory or project-file discovery | Kept. |
| Disabled installations are never replaced | Kept and extended to all selected installations. |
| Catalog holds no implementation | Kept. It gains version and digest data and nothing else. |

### Trust model

Four distinct checks, each with its own basis:

1. **Trust in the installed Ptah and its pin.** The user trusts the Ptah release
   they installed, and with it the pinned digests it carries.
2. **Archive against the pinned archive digest.** `providers install` compares the
   downloaded archive with the digest in the pin before it extracts anything.
3. **Executable against the pinned executable digest.** Install checks the
   extracted executable. The host and the SDK check it again before every start.
4. **Identity and protocol at startup.** The process must declare the expected
   provider ID and engines, and a compatible protocol and required features. A
   provider that does not is refused before any database connection opens.

A SHA-256 digest is not proof of authorship. A pinned digest binds the downloaded
bytes to a catalog the user already trusts. `checksums.txt` fetched with the
archive is not part of the trust basis: it detects corruption, not a hostile
channel.

A mirror or a base-URL override changes where the archive is fetched from. It does
not change expected digests, identity or any trust rule.

The pin is generated from the plugin release and committed to the release sources,
so a build is reproducible and does not contact GitHub or choose a latest version.
The generator checks what the compiler cannot: digest format, that the archive
and the executable match, no duplicate entries, and the platform set. The compiler
checks only structure and types, not hashes, URLs or compatibility.

Verifying signature bundles is not part of this decision and not required.

### One artifact format, two delivery paths

Bundling and `providers install` deliver the same artifact: the pinned plugin
archive's executable. Both lead to the same `Installation` model and the same
`Verify`, `Start` and ownership checks. There is no second lifecycle, trust policy
or registry for bundled files.

- **Bundled location.** A `providers/<provider-id>/<version>/` directory beside the
  `ptah` executable inside the installed layout, or the equivalent path a package
  format defines and documents. The host does not copy a bundled executable into
  the user store.
- **User store.** `os.UserCacheDir()/ptah/providers/<provider-id>/<version>/<os>_<arch>/`,
  writable only by the user. The cache is not guaranteed to persist. Clearing it
  produces the missing-provider diagnostic, never a hidden download or another
  engine.
- **Owners differ.** A bundled provider may sit in a root-owned or administrator-owned
  location. The shared aim is that no one but the owner of the location can
  replace the bytes between verification and start.

The installer must:

- extract without letting an archive path escape the staging directory;
- check the archive and the executable against the pin;
- publish an installation only when it is fully verified;
- behave correctly when it runs twice or in parallel, so an interrupted or
  concurrent install leaves no runnable partial executable.

The ADR requires this atomic result, not a particular mechanism. A single rename
is not assumed to be portable across filesystems and Windows.

### Distribution and offline use

| Channel | Requirement |
| ------- | ----------- |
| Release archives | The full archive contains the pinned YDB provider for the platforms the pin covers. |
| Docker image | The image contains it at the bundled location. |
| Homebrew | The cask installs it at the bundled location. |
| `install.sh`, `install.ps1` | Verify each artifact against its checksum before unpacking. |
| GitHub Action | Its `install.sh` verifies before unpacking. Today it does not, so this is mandatory implementation work. |
| All channels | None ships a different selection or trust model. |

A platform without a pinned artifact gets an explicit diagnostic that names the
platform. No bundled artifact means no promise of support.

Three offline cases:

- With a provider already installed or bundled, normal work needs no network.
- An internal mirror serves `providers install` through the base-URL override.
  The pinned digests still decide what is accepted.
- A fully offline host receives a verified executable by other means and selects it
  with a manifest entry (absolute path, digest). No extra command is needed.

### Sequencing

Landing the change in several pull requests is an engineering order. It is not a
supported mode. No release offers both an embedded YDB engine and the provider, no
flag opts out of the provider, and the embedded YDB code is deleted in the same
release that makes the provider the engine for `ydb://`. ADR 0024's order of
facets stays; only its remark that the host planner, comparer and renderer remain
reachable until the default flips no longer describes a user-facing state.

## Alternatives considered

**Download on first use.** The user types a URL and the provider appears. This
turns recognition into authorization to download and execute, and every command
into a network operation that CI and air-gapped hosts cannot accept. Rejected.

**Embed provider bytes in the host and extract them to run.** This needs no
download, and it keeps the process boundary, because the provider still runs as a
separate process. The cost is a host binary that grows with every embedded
provider and a provider that can only be released with the host. It also does not
need to cover every provider. Rejected for now because the distribution already
bundles files beside the host at lower cost. It is not ruled out as a packaging
detail.

**Take digests from a `checksums.txt` fetched at run time.** The listing and the
archive come from the same channel, so the listing detects corruption and adds no
independent trust. Rejected as the trust basis.

**Manifest only.** The operator writes an entry for every engine. This keeps ADR
0021 unchanged and works for third-party providers. It is inconvenient out of the
box, which is a cost, not proof that a default is impossible. Kept as selection
rule 1 and as the path for custom builds and offline hosts.

## Consequences

- A Ptah release carries tested provider versions. Hotfixes reach users through
  the next release or through an operator-selected manifest entry.
- Release tooling grows: a pin generator, a step that fetches and verifies plugin
  archives at release time, and changes to every distribution channel.
- The GitHub Action installer needs checksum verification before it can unpack a
  provider.
- Source builds and `go install` users must run `providers install ydb` before
  using YDB, and get a diagnostic with that instruction when they do not.
- A user who clears the cache sees `provider_missing`, not a download.
- The user-store layout and the bundled layout are part of the contract between
  installer, packages and host.

## Acceptance criteria

These are for the future implementation. None of them is claimed to hold today.

- An official install uses the bundled YDB provider without a download.
- A source build or `go install` without a provider prints the install
  instruction.
- DuckDB and Spanner are absent until the user installs them.
- An explicit manifest entry has priority over a bundled or stored provider.
- A wrong explicit selection causes no fallback.
- Another cached version does not change the default selection.
- A corrupted selected artifact is refused.
- An incompatible provider is rejected before any database work.
- A mirror cannot change a trusted digest.
- A fully prepared installation works offline.
- `providers list` and `providers install` do not start the provider to discover
  anything.
- An interrupted or parallel install leaves no runnable partial executable.
