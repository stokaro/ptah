# ADR 0026: Official providers are pinned, installed explicitly, and bundled by the distribution

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

A provider is selected today from a manifest the operator writes by hand
(`internal/providerconfig`). The entry names an absolute executable path and its
SHA-256. The host verifies the digest before it starts the process, and the SDK
transport verifies it again at exec time. Nothing in the host downloads, caches
or installs a provider, and ADR 0021 forbids it: no command downloads, enables,
replaces or updates a provider.

The catalog (`internal/providercatalog`) already names the official engines,
duckdb, spanner and ydb, with their repository and setup URLs. It carries no
version and no digest. `BundledByDefault` is set for ydb and is read only to
word the "provider missing" message.

The YDB plugin is released: each release publishes one archive per platform,
`checksums.txt`, and `release-lines.json`. For ydb to become the default for
`ydb://` URLs, a user who types `--db-url ydb://...` has to get the plugin
without writing a manifest. For duckdb and spanner the same question arrives
later. No host release surface carries a plugin today: the archives, the Docker
image, the Homebrew cask, `install.sh`, `install.ps1` and the GitHub Action
install only `ptah`, `ptah-ls` and `ptah-compat`.

## Decision

**The catalog pins each official provider release.** For every official engine
the catalog records the plugin version, its repository, and for each supported
operating system and architecture the archive name, the archive SHA-256 and the
SHA-256 of the executable inside it. The data is a generated Go file, produced
from the plugin release's `checksums.txt` and archive by a small tool run when
the host is released. Go is preferred over embedded JSON because the compiler
checks it. Both digests are pinned, because the host and the SDK hash the
executable, not the archive. The host knows that an official plugin exists, where
it comes from and which bytes are expected. It knows nothing else about the
plugin's content.

**Installation is an explicit command.** `ptah providers install <engine>`
downloads the pinned archive, checks the archive digest before extracting, extracts
into a staging directory, checks the executable digest, and moves it into the
cache with one rename. Any mismatch fails closed and nothing falls back to the
built-in engine. No other command downloads. ADR 0021's rule is narrowed, not
removed: recognition still authorizes nothing, and a download happens only when
the operator runs the install command.

**The cache is per user.** The executable lives under
`os.UserCacheDir()/ptah/providers/<provider-id>/<version>/<os>_<arch>/`, in a
directory only the user can read. A flag overrides the location. The cached
absolute path becomes an `Installation` that goes through the same `Verify`,
`Start` and ownership checks as a manifest entry. Old versions are kept.

**An explicit selection always wins.** `--provider-config` and `--provider-engine`
behave as they do today. The catalog resolves an installation only when the
manifest has none for the engine. A manifest entry that claims an official engine
still overrides the catalog.

**The distribution bundles the plugin it makes the default.** For an engine whose
catalog entry is `BundledByDefault`, the release archive, the Docker image, the
Homebrew cask, `install.sh`, `install.ps1` and the GitHub Action place the plugin
next to `ptah` or at the cache path, and the host verifies it against the same
pinned digests. A build from source or `go install` carries no plugin and uses
`providers install`. YDB is bundled first.

**The default flips when the plugin is present.** For `ydb://` the host resolves
the bundled or installed plugin when one is present and verified. The built-in
engine stays reachable through an explicit opt-out for one release after the
flip, then the host code is deleted in the order ADR 0024 sets.

**Trust is the pinned digest.** The digests state integrity, as they do today,
not publisher authenticity. Verifying the plugin's signature bundle is a later,
optional step and is not part of this decision.

## Alternatives

**Download on first use.** The user types a URL and the plugin appears. It makes
recognition an execution authorization and turns every command into a network
operation, which CI and air-gapped hosts cannot accept. Rejected.

**Pin the digest at run time from a release listing.** The host would fetch
`checksums.txt` and trust it. The listing and the archive come from the same
publisher over the same channel, so it adds no check. Rejected.

**Embed the plugin in the host binary.** No download and no distribution change,
at the price of one binary that carries every plugin and cannot release them
separately. It also removes the process boundary the provider design is built on.
Rejected.

**Keep the manifest only.** The operator writes the entry for every engine. It
keeps ADR 0021 untouched and cannot make a plugin the default. Rejected for the
official engines, kept for third-party providers.

## Consequences

- The host release pins a plugin release. A plugin hotfix reaches users in the
  next host release, or through `providers install` once the catalog allows a
  version choice. Whether it should is a separate decision.
- The release tooling grows: a generator for the catalog data, a step that fetches
  and verifies the plugin archive at host release time, and changes to six
  distribution surfaces.
- `.github/actions/ptah/install.sh` does not verify checksums today. It has to be
  hardened before it unpacks a bundled plugin.
- Network access moves into one command with a base-URL override, so mirrors and
  air-gapped installs can use it.
- Windows needs the `.zip` archive and the `.exe` suffix handled, and a platform
  with no pinned archive fails with a message naming the platform.
- ADR 0021's text stays as written. This record narrows its download rule and says
  so.
