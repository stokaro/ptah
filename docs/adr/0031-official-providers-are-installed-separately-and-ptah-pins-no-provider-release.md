# ADR 0031: Official providers are installed separately, and Ptah pins no provider release

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Supersedes: [ADR 0026](0026-official-providers-are-pinned-in-the-release-installed-explicitly-and-bundled-by-the-distribution.md)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0029](0029-column-tables-need-no-protocol-change-and-the-address-stays-provider-owned.md),
[ADR 0030](0030-ptah-compat-resolves-providers-through-the-native-pipeline.md)

## Context

What the code does today, under ADR 0026:

- `internal/providercatalog` names the official engines (duckdb, spanner, ydb)
  with their provider ID, repository URL and setup URL, and marks ydb
  `BundledByDefault`. The generated file `pins_gen.go` pins ydb to
  `stokaro/ptah-ydb-plugin` v0.1.1, with an archive digest and an executable
  digest for each of six platforms. `internal/cmd/providerpin` writes that file
  from a plugin release.
- `ptah providers install <engine>` takes no version. It downloads the pinned
  archive, compares it with the pinned digest, extracts the one executable,
  compares that with the pinned digest, and stores it under
  `os.UserCacheDir()/ptah/providers/<provider-id>/<version>/<os>_<arch>/`.
  `--download-base-url` (`PTAH_DOWNLOAD_BASE_URL`) replaces the download
  directory of the pinned tag.
- The release build runs `internal/cmd/providerbundle` as a GoReleaser `before`
  hook. The hook fetches the pinned archives and stages the executables. The
  host archives carry them as `providers/`, the Docker image copies them to
  `/usr/local/bin/providers`, and the Homebrew cask unpacks them beside `ptah`.
  `release.yml` checks the bundled executable in every archive and in both
  images. `install.sh`, `install.ps1` and the GitHub Action copy the
  `providers` directory beside the binaries.
- `providerconfig.Resolver.Select` tries the manifest entry, then the bundled
  provider, then the stored provider with the pinned version and digest, and
  ends with `provider_missing`.
- The host speaks one provider contract, `provider.Revision` in `provider-sdk`
  v0.2.0, which is 25. The handshake sends the revision and the features the
  host requires, and the SDK refuses a provider with another revision. The
  provider advertises its features. The host does not advertise its own.
- The pinned release, v0.1.1, is built against an SDK pseudo-version whose
  `Revision` is 24. A host built from this tree speaks 25, so it refuses the
  provider it bundles at startup with `provider_incompatible`.

The last point is the cost of the pin in practice. A pin binds each Ptah release
to one plugin build. A plugin fix reaches users only through a Ptah release or a
hand-written manifest entry, and a host change to the contract makes the pin
stale at once. The owner decided to deliver provider plugins separately from
Ptah.

## Decision

### Delivery

An executable provider plugin is installed separately from Ptah. YDB is the first
one, from `stokaro/ptah-ydb-plugin`.

- No Ptah release pins a provider version or a provider digest.
- No release archive, Docker image, Homebrew cask, `install.sh`, `install.ps1`
  or GitHub Action carries a provider.
- Ptah's own pipelines do not run against optional plugins. This is intentional.
  The contract is tested by the SDK and by each plugin's repository.

The host knows two facts about an official engine: that a provider exists for it,
and where that provider's releases live. Everything else comes from the release.

### The catalog

A catalog entry maps an engine to a provider identity and the provider identity
to a release source:

| Field | Meaning |
| --- | --- |
| Engine ID, aliases, URL schemes | Recognition, as ADR 0021 defines it. |
| Provider ID | The identity the process must declare, such as `github.com/stokaro/ptah-ydb-plugin`. |
| Repository URL | A GitHub repository. Its releases are the release source. |
| Setup URL | The documentation page for the engine. |

The catalog holds no version, no digest and no distribution flag. It stays data
only, and an entry grants no execution permission.

### Contract versions

A provider is compatible with the provider contract, not with particular Ptah
versions. The contract version has two parts, MAJOR.MINOR.

- **MAJOR** changes only on an incompatible change. The host and the provider
  must have the same major version. Today it is 25, the value of the SDK's
  `Revision`.
- **MINOR** grows by one with each compatible addition, and goes back to 0 when
  MAJOR changes. A host built on `provider-sdk` v0.2.0 speaks 25.0. The SDK work
  in progress makes it 25.1.
- A plugin declares the **minimum** contract it needs, a major and a minor
  version. The host starts it when `host.major == min.major` and
  `host.minor >= min.minor`.
- Anything beyond the minimum is negotiated as features, in both directions. The
  provider advertises the features it serves, and the host sends the features it
  requires. The host must also advertise the features it supports. The SDK does
  not carry that yet, and the SDK work that adds it is a requirement of this
  decision, not part of it. Until a host advertises a feature, a provider must
  not send a new enum value, and must not rely on a new request field, that the
  feature introduces.
- An additive SDK release (a new RPC, field or enum value) raises the minor
  version. It does not change the major version.
- Ptah releases do not move the contract. Ptah's own version number has no role in
  compatibility. A Ptah release that speaks 25.3 starts every release of a plugin
  whose minimum is 25.0, 25.1, 25.2 or 25.3.

The SDK README ([`provider-sdk/README.md`](../../provider-sdk/README.md)) states
the same rules for provider authors.

### The release manifest: `ptah-provider.json`

Each plugin release publishes one small JSON document as a release asset named
`ptah-provider.json`. Install reads it to choose a release and an artifact
without downloading archives. The YDB plugin adds it in
[stokaro/ptah-ydb-plugin#37](https://github.com/stokaro/ptah-ydb-plugin/issues/37).

```json
{
  "schema": 1,
  "provider_id": "github.com/stokaro/ptah-ydb-plugin",
  "version": "0.2.0",
  "min_contract": {"major": 25, "minor": 0},
  "engines": ["ydb"],
  "artifacts": [
    {
      "os": "linux",
      "arch": "amd64",
      "archive": "ptah-plugin-ydb_0.2.0_linux_amd64.tar.gz",
      "executable": "ptah-plugin-ydb"
    }
  ]
}
```

| Field | Type | Rule |
| --- | --- | --- |
| `schema` | integer | `1`. A reader refuses a value it does not know. |
| `provider_id` | string | Equals the catalog provider ID of the engine being installed. |
| `version` | string | A semantic version without a leading `v`. The release tag is `v` followed by this value. |
| `min_contract` | object | `major` and `minor`, both non-negative integers. `major` is at least 1. |
| `engines` | array of strings | Non-empty, no duplicates. Each name is an official engine ID that the catalog assigns to `provider_id`, and the engine being installed is one of them. |
| `artifacts` | array of objects | Non-empty. One entry per platform, and no platform twice. |
| `artifacts[].os`, `artifacts[].arch` | string | Go's `GOOS` and `GOARCH` spelling. |
| `artifacts[].archive` | string | A plain file name that is a release asset, ending in `.tar.gz` or `.zip`. |
| `artifacts[].executable` | string | A plain file name, the one top-level entry install extracts from the archive. |

A plain file name has no `/`, no `\`, no NUL, and is not `.` or `..`.

The document carries no digest. Digests come from the release's `checksums.txt`
only, so one file holds them and the two cannot disagree. A reader ignores a field
it does not know. A change that an older reader must not ignore raises `schema`.

### `ptah providers install <engine>`

The command installs the newest release of the engine's provider whose minimum
contract the host meets.

1. It lists the releases of the catalog repository through the GitHub REST API
   (`GET /repos/{owner}/{repo}/releases`).
2. It keeps published releases: not drafts, not prereleases, with a tag of the
   form `v<semantic version>`, and with a `ptah-provider.json` asset. A release
   without the asset predates the manifest or is not a provider release, and is
   skipped.
3. It reads the manifests in descending semantic-version order and takes the
   first whose `min_contract` the host meets.
4. A manifest that is malformed, or whose `provider_id`, `version` or `engines`
   contradict the catalog or the tag, ends the search with a refusal. It is not
   skipped, for the same reason a corrupt candidate is not skipped at selection.

`--version <v>` names one release, written with or without the leading `v`.
Install reads that release's manifest directly and does not call the API. It
refuses a release whose minimum contract the host does not meet, and the message
names both versions, for example: "the ydb provider 0.3.0 needs contract 26.0 or
a later 26.x; this Ptah speaks contract 25.0". A prerelease is installable only
this way.

After choosing, install takes the artifact for the running platform from the
manifest. A release without one is refused, and the message names the platform.
Install never runs the downloaded executable and never edits the provider
manifest.

Failures are diagnostics with their own codes. These cases must be told apart:

| Case | Diagnostic |
| --- | --- |
| The repository has no release with `ptah-provider.json` | `provider_no_release` |
| No release has a minimum contract the host meets | `provider_no_compatible_release`, naming the host contract and the newest releases with their minimums |
| `--version` names a release whose minimum the host does not meet | `provider_release_incompatible`, naming both versions |
| A release has no `ptah-provider.json`, `checksums.txt` or archive where the layout says | `provider_release_asset_missing`, naming the address |
| A manifest is malformed or contradicts the catalog | `provider_release_invalid` |
| The manifest lists no artifact for the platform | `provider_platform_unsupported` |
| `checksums.txt` has no entry for a file install read | `provider_checksum_missing` |
| The entry is empty, malformed or listed twice | `provider_checksum_invalid` |
| A file's SHA-256 differs from its entry | `provider_checksum_mismatch` |
| A mirror is set and `--version` is not | `provider_version_required` |

Install sends no credential to any server. Unauthenticated GitHub API requests
have an hourly limit per address. A host that reaches it passes `--version`, which
needs no API call.

### Integrity

- **Release checksums.** Install downloads `checksums.txt` from the chosen
  release. The file uses the `sha256sum` format, one `<hex digest>  <file name>`
  per line. `ptah-provider.json` and the archive must each have exactly one
  entry. Install compares the manifest bytes it read, and the archive before
  anything is extracted from it. A missing file, a missing entry, an empty or
  malformed digest, a duplicate entry and a mismatch each refuse the install.
- **Extraction.** Only the executable the manifest names is taken out, every
  entry path is checked, and nothing is published until the executable is fully
  written. These rules from ADR 0026 stay.
- **The install record.** After it publishes the executable, install computes
  the executable's SHA-256 and writes an install record (format below). The
  record is the install manifest of the user store.
- **Before every start.** The host reads the record, checks that it meets the
  record's `min_contract`, hashes the executable and compares the digest with the
  record. The SDK transport hashes the file again at exec time. The handshake
  then checks the provider ID, the engines, the contract major and the required
  features, before any database connection opens.

`checksums.txt` arrives through the same channel as the archive. It detects
corruption and truncation. It does not prove who published the release. The record
lives in a directory only the user can write. Whoever can write there can replace
the executable and the record together, so the record proves that the bytes did
not change after install, and nothing more. An operator who needs assurance about
the publisher verifies the artifact by other means, such as GitHub artifact
attestations, and selects it through a manifest entry with its absolute path and
digest. Signature verification is not part of this decision.

### The user store and the install record

The store stays at `os.UserCacheDir()/ptah/providers/`. An installation keeps its
path: `<store>/<provider-id>/<version>/<os>_<arch>/<executable>`, directories
0700. The record of one provider on one platform is
`<store>/<provider-id>/installed-<os>_<arch>.json`:

```json
{
  "schema": 1,
  "provider_id": "github.com/stokaro/ptah-ydb-plugin",
  "version": "0.2.0",
  "min_contract": {"major": 25, "minor": 0},
  "engines": ["ydb"],
  "executable": "0.2.0/linux_amd64/ptah-plugin-ydb",
  "sha256": "<hex digest of the executable>"
}
```

`executable` is relative to `<store>/<provider-id>/` and must stay below it. Install
writes the record through a temporary file and a rename in the same directory,
after the executable is in place. An interrupted install leaves the previous
record, which still names a complete installation. Two parallel installs each
write a complete record, and the later rename decides.

The record names the version selection uses. The most recent successful
`providers install` sets it, including a `--version` that chooses an older
release. Installing the version the record already names, with an executable that
matches the record, downloads no archive. Other versions in the store are not
read and are not removed.

### Selection order

Selection starts after the manifest passes the validation it gets today. The first
rule that matches decides, and later rules are not consulted.

| Order | Candidate | Source of trust | Result when the candidate exists but fails |
| --- | --- | --- | --- |
| 1 | The installation the manifest selects for the engine | Executable path and digest the operator wrote | Refuse. No other candidate is tried. |
| 2 | The user-store installation the install record names, for the engine's provider and the running platform | The digest install recorded from checksum-verified bytes | Refuse. |
| 3 | None | | `provider_missing` with the instruction `ptah providers install <engine>` |

- A candidate exists when its record exists. A record that cannot be read or
  parsed is refused (`provider_store_invalid`), not skipped.
- A record whose `min_contract` the host does not meet is refused
  (`provider_incompatible`) before the process starts, with the instruction to
  run `ptah providers install <engine>` again.
- A disabled, missing, corrupt or incompatible installation that the manifest
  selected is never replaced by rule 2.
- Nothing is discovered through `PATH`, the working directory or a project file.
- The engines and aliases the started process must declare come from the catalog
  entries of the record's engines, as they came from the catalog under ADR 0026.

The same rules apply to every consumer of the shared pipeline: `ptah`,
`ptah-compat` and `ptah-ls` where they use providers.

### Mirrors and offline use

`--download-base-url` and `PTAH_DOWNLOAD_BASE_URL` replace `https://github.com`
in every download address. A mirror serves the same release layout:

```text
<base>/<owner>/<repo>/releases/download/v<version>/ptah-provider.json
<base>/<owner>/<repo>/releases/download/v<version>/checksums.txt
<base>/<owner>/<repo>/releases/download/v<version>/<archive>
```

The layout keeps the owner and the repository, so one base serves every provider
and the environment variable means the same thing for every engine. A generic
proxy of `https://github.com` already has this layout.

A download mirror does not serve the releases API, so newest-compatible discovery
does not work through one. With a mirror, `--version` is required, and the command
refuses before it sends any request. The operator of an internal mirror decides
which releases it holds, so naming the version costs them nothing they do not
already know. The API address is `https://api.github.com` and is not configurable.

The checksum rules apply to a mirror unchanged. A mirror changes where the bytes
come from, not what is accepted.

Offline use needs no extra command:

- With a provider installed, normal work opens no network connection.
- A host behind a mirror runs `providers install <engine> --version <v>
  --download-base-url <base>`.
- A fully offline host receives a verified executable by other means and selects
  it with a manifest entry.

### Distribution

| Channel | What it ships |
| --- | --- |
| Release archives | `ptah`, `ptah-ls`, `ptah-compat`, `README.md`, `LICENSE`. No `providers` directory. |
| Docker image | The three binaries. An image that needs a provider runs `ptah providers install <engine>` in a build step of its own, as the user that later runs it, since the store is that user's cache directory. |
| Homebrew cask | The release archive, unchanged. |
| `install.sh`, `install.ps1` | The three binaries, after verifying the archive against `checksums.txt`. They copy no provider. |
| GitHub Action | The three binaries, after verifying the archive against `checksums.txt`. A workflow that needs a provider runs `ptah providers install <engine>` in a step. |

The action keeps the archive check ADR 0026 required. It protects the host
archive, and has nothing to do with bundling.

### Authorization to run code

A provider process starts on one of two grounds:

1. The user ran `ptah providers install` for that provider, and the install record
   names the executable.
2. The user selected an installation through the manifest.

The bundled-distribution ground of ADR 0026 is withdrawn. Recognizing an engine
name or a URL scheme still authorizes no download, no search for a binary and no
unverified code. `providers install` remains the only command that downloads.
`providers list` keeps reading and hashing without execution, and `providers
check` starts only the provider it was asked to check.

### Changes to earlier records

ADR 0026 is superseded as a whole. These provisions of it stay in force, restated
here so that a reader of this record does not need it:

- YDB is served by an external provider. When the selected provider is absent,
  corrupt or incompatible, the command fails with a diagnostic, and no other
  engine and no embedded YDB implementation takes its place.
- Which engine serves `ydb://` is a product decision made in the Ptah release, not
  a runtime probe. A file in the store is the precondition for running the
  provider that was chosen, never a switch between architectures.
- Landing the change in several pull requests is an engineering order, not a
  supported mode. No release offers both an embedded YDB engine and the provider
  for `ydb://`, no flag opts out of the provider, and the embedded YDB code is
  deleted in the release that makes the provider serve `ydb://`.
- The installer extracts safely, publishes only verified installations, and
  behaves correctly when it runs twice or in parallel.

Provisions of later records that name the bundle or the pin read as follows:

| Record and text | Reading under this record |
| --- | --- |
| ADR 0021: "No command downloads, enables, replaces, or updates a provider" | One command, `providers install`, downloads and replaces the store installation of one provider. No command enables or updates a provider by itself. |
| ADR 0027: "the operator installed a distribution, ran `providers install`, or wrote a manifest entry" | The operator ran `providers install` or wrote a manifest entry. |
| ADR 0029: "The host pins the plugin version in the release (ADR 0026), so the pin moves when step 6 is released" | No pin moves. The plugin release raises its `min_contract` if step 6 needs a newer contract. |
| ADR 0030: "When it is unset, selection starts at the bundled provider (rule 2 of ADR 0026)" | When it is unset, selection starts at the user-store installation (rule 2 here). |
| ADR 0030: "pinned digests, identity and integrity verification before every start" | The digest in the manifest entry or the install record, identity and integrity verification before every start. |
| ADR 0030: "the distribution ships both binaries" and the compat acceptance criterion "starts the bundled provider on first connect" | The distribution ships both binaries and no provider. A compat command starts the user-store provider on first connect. |

### What is deleted

- `internal/providercatalog/pins_gen.go`, the `Pin` and `Artifact` types and the
  platform list in `pin.go`, and the `BundledByDefault` field.
- `internal/providerpin` and `internal/cmd/providerpin`.
- `internal/providerbundle` and `internal/cmd/providerbundle`, with their tests,
  including the test that ties the GoReleaser platforms to the pin.
- The GoReleaser `before` hook that runs the bundler, the `providers` entry in the
  archive files, the staged directories in `dockers_v2.extra_files`, and the cask
  comment about the bundled directory.
- The Dockerfile copy to `/usr/local/bin/providers`.
- The bundle check in `.github/workflows/release.yml`, and the
  `/.release-providers/` entry in `.gitignore`.
- The `providers` copy in `install.sh` and `install.ps1`, the `providers` handling
  in the action's `install.sh`, and the gate and selftest cases that exist only for
  the bundle.
- The bundled selection rule: `BundledDir`, `BundledPath`, the bundled root of the
  resolver and the bundled origin of an installation.
- The diagnostic codes `provider_no_release_pinned` and
  `provider_executable_untrusted`, and the install meaning of
  `provider_archive_untrusted`, which `provider_checksum_mismatch` replaces.

This changes behavior. Ptah is pre-v1, so no compatibility is owed.

## Alternatives considered

**Keep the pin and the bundle (ADR 0026).** Every Ptah release carries one tested
plugin build, and an official distribution works without a download. The cost is
measured above: a plugin fix waits for a Ptah release, and the pin went stale
against the host's own contract. Rejected by the owner.

**Install the latest release.** One request, and the release page already marks
one release as latest. A latest release that needs a newer contract than the host
speaks would be installed and then refused at every start. Rejected.

**Declare compatible Ptah versions in the plugin release.** A field such as
`"ptah": ">=0.9"`. It ties plugins to the host's release numbering, which says
nothing about the interface between them, and every Ptah release would need every
plugin to widen its range. Rejected: compatibility is a property of the contract.

**Carry the digests in `ptah-provider.json`.** One file would hold everything
install reads. GoReleaser already writes `checksums.txt` for every asset, and two
files that both carry digests can disagree. Rejected.

**Publish an index of releases at a fixed address.** An index would make
discovery work through a mirror. It needs a publisher and an update step that
can fall behind the releases, and the releases API already answers what is
published. Rejected; a mirror takes `--version` instead.

**Accept a release with no `checksums.txt` entry, with a warning.** Fewer failed
installs for a careless release. It lets an unchecked archive reach the store.
Rejected: install is never looser than the release it reads.

## Consequences

- A plugin fix or a new plugin release reaches users without a Ptah release, as
  long as its minimum contract stays one the host meets.
- Every user of an official provider runs `ptah providers install <engine>` once,
  including users of the release archives and the Docker image. When YDB is
  served by its provider, that command is the precondition for `ydb://`, and the
  missing-provider diagnostic says so.
- The release pipeline and the installers shrink, and the Docker image no longer
  carries provider bytes.
- A plugin release must publish `ptah-provider.json` and list it in
  `checksums.txt`. A release without it cannot be installed by command. The
  existing YDB releases v0.1.0 and v0.1.1 are such releases.
- Discovery depends on the GitHub releases API, which has an unauthenticated rate
  limit. `--version` does not.
- Ptah's pipelines do not prove that a given plugin release works with a given
  Ptah build. The contract version and the feature negotiation are the promise,
  and the SDK and the plugin repositories test them.
- The record format, the store layout and the mirror layout are a contract
  between `providers install`, selection and mirror operators.
- Until the host advertises its features, a provider cannot use a host feature
  beyond its declared minimum.

## Acceptance criteria

These are for the implementation. None of them is claimed to hold today.

- No release archive, image or installer carries a provider, and no file in the
  tree pins a provider version or digest.
- `providers install` chooses the newest release whose minimum contract the host
  meets, among releases with different minimums, and skips releases without
  `ptah-provider.json`.
- `--version` installs a matching release and refuses a release the host cannot
  meet, naming both versions.
- A missing manifest asset, a missing `checksums.txt` entry, an empty digest, a
  mismatch and a platform without an artifact are each refused with their own
  diagnostic, and nothing is published.
- A mirror without `--version` is refused before any request.
- A stored installation is selected after the manifest and before
  `provider_missing`, its digest is checked before every start, and a changed
  executable is refused.
- The host's contract comparison is one function, so it reads the minor version
  when the SDK exposes one.
- `providers list` and `providers install` do not start a provider to discover
  anything.
