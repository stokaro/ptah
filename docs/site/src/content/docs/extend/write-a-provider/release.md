---
title: Package and release a provider
description: Publish provider releases that operators can verify and select, with the ptah-provider.json document, checksums and a minimum contract, and keep releases compatible across Ptah versions.
type: how-to
audience:
  - "go-developer"
  - "integration-developer"
readerQuestion: "How do I publish a provider release that Ptah can choose, verify and run?"
goal: "Publish a release with archives, ptah-provider.json and checksums.txt, and keep it compatible with the hosts it declares."
sourceOfTruth:
  - "internal/providerrelease"
  - "internal/providerinstall"
  - "internal/providerarchive"
  - "internal/providercatalog"
  - "internal/providerconfig"
  - "provider-sdk/provider/contract.go"
  - "github:stokaro/ptah-ydb-plugin"
generated: false
searchAliases:
  - "ptah-provider.json"
  - "provider release"
  - "min_contract"
overlaps:
  - /extend/executable-providers/
disposition: keep
---

Publish provider builds that an operator can verify, select and upgrade
without guessing which Ptah runs them. You need a provider that passes its
tests (see [Test a provider](../testing/)) and a repository whose releases
operators can download. The layout below is the one `ptah providers install`
reads, and the
[YDB provider](https://github.com/stokaro/ptah-ydb-plugin) publishes it with
GoReleaser.

## How operators run a release

Ptah runs a provider executable only when it can verify the exact bytes. An
operator reaches a release on one of two paths:

- **A manifest entry.** The operator downloads and verifies a build, then names
  its absolute path, its SHA-256 digest, the provider ID and the engines in a
  `providers.json` file, as the tutorial does. This works for every provider.
- **`ptah providers install <engine>`.** The command installs an official
  provider: one whose engine has an entry in Ptah's catalog. The entry maps the
  engine to the provider ID and to the GitHub repository whose releases the
  command reads.

A third-party engine name needs no catalog entry and no Ptah release, and a
manifest is how operators select it. A catalog entry is a change to Ptah
itself, so a provider that should install by command is proposed in an issue.
Either way, Ptah checks the digest before every start, and the provider ID, the
engines, the contract and the required features after it. The
[operator page](../../executable-providers/#which-provider-serves-an-engine)
says how Ptah picks between a manifest entry and an installation.

## Name and build the artifacts

A release carries one archive per platform, `ptah-provider.json` and
`checksums.txt`:

```text
<project>_<version>_<os>_<arch>.tar.gz   (.zip on Windows)
ptah-provider.json
checksums.txt
```

- The release tag is `v` followed by the version, as in `v0.3.0`, and the
  version is a semantic version without build metadata.
- The executable is a top-level file of its archive, listed once. Ptah refuses
  an archive with an absolute path, a `..` element, a backslash or a drive
  letter in any entry name, and extracts nothing but the executable.
- Build each executable without cgo where the driver allows it, so one
  archive runs on every machine of its platform. The YDB provider builds
  linux, darwin and windows on amd64 and arm64.
- Set the version the provider reports in `Describe` at link time. The
  tutorial's provider reads it from `backend.version`:

```bash
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-X example.com/ptah-tutorial-provider/backend.version=0.1.0" \
  -o ptah-tutorial-provider .
```

## Write ptah-provider.json

`ptah-provider.json` lets a host choose a release before it downloads an
archive. It names the provider, the version, the oldest contract the release
needs, its engines and an archive for each platform:

```json
{
  "schema": 1,
  "provider_id": "example.com/ptah-tutorial-provider",
  "version": "0.1.0",
  "min_contract": {"major": 25, "minor": 0},
  "engines": ["tutorial"],
  "artifacts": [
    {"os": "linux", "arch": "amd64", "archive": "ptah-tutorial-provider_0.1.0_linux_amd64.tar.gz", "executable": "ptah-tutorial-provider"},
    {"os": "windows", "arch": "amd64", "archive": "ptah-tutorial-provider_0.1.0_windows_amd64.zip", "executable": "ptah-tutorial-provider.exe"}
  ]
}
```

Ptah reads `schema` 1 and refuses another. It also refuses a document larger
than 64 KiB, a `version` with a leading `v`, an empty `engines` or `artifacts`
list, a platform listed twice, an archive name that is not a plain `.tar.gz`
or `.zip` file name, and an executable name with a directory part. For an
official provider, the document must name the catalog's provider ID, its
version must match the tag, and each engine must be an official engine of
that provider. The document carries no digest.

Generate the document from the provider's own description, so the minimum it
publishes is the minimum `Describe` declares. `provider.MinimumContract`
returns that minimum, and the YDB provider's `cmd/provider-manifest` program
writes the document with it in a GoReleaser `before` hook.

## Publish checksums.txt

`checksums.txt` lists the SHA-256 digest of every archive and of
`ptah-provider.json`, one `<hex digest>  <file name>` line each, in the format
`sha256sum` writes. `ptah providers install` downloads it first and checks
`ptah-provider.json` and the archive against it before it reads anything
inside them. A missing file, a missing or empty entry, a file listed twice and
a different digest each refuse the install.

Ptah verifies digests and nothing else: it reads no signature, and a digest
proves that the bytes did not change, not who built them. An operator who
selects a build with a manifest computes its digest from bytes they trust. A
signature you publish beside a release serves that verification, outside Ptah.

With GoReleaser, the `checksum` section writes `checksums.txt` in this format,
and `extra_files` adds `ptah-provider.json` to it and to the release. The
YDB provider's `.goreleaser.yaml` is a complete configuration. A pull request
can run `goreleaser release --snapshot --clean` and check the snapshot's
`checksums.txt` before a tag publishes anything.

## How install chooses a release

Without `--version`, `ptah providers install` lists the repository's published
releases, newest first, and installs the newest one whose minimum contract the
host meets. It skips drafts, prereleases and releases without
`ptah-provider.json`, and a malformed document stops the command instead of
being skipped. With `--version`, it installs that release or refuses it,
naming both contract versions. The command extracts the executable into the
user's cache directory and records its digest, and every later start checks
the executable against that record. The
[install procedure](../../executable-providers/#install-an-official-provider)
gives the operator's view, including mirrors.

## Keep releases compatible

A provider release is compatible with a contract, not with particular Ptah
versions:

- **Contract.** Ptah runs a release when the majors are equal and its minor is
  at least the release's `min_contract.minor`. Keep the minimum at the first
  minor of the major unless the provider cannot work with an older host at
  all, and put each later addition behind its feature. Raise the minimum only
  in a release that needs it, because it drops every older host.
- **Features.** A provider built from a newer SDK still works with an older
  host, for everything both list. Test with no host features and with
  `provider.TwoWayFeatures()`, as [Test a provider](../testing/#drive-the-provider-through-its-process)
  shows.
- **Models.** A captured claim names a model by the provider ID, the kind, the
  representation, the schema version and the definition digest, so documents
  an earlier release wrote keep reading while the definition stays the same.
  When a definition changes without changing what a claim means, declare the
  old identity as a predecessor under `models.predecessors` and
  `models.own-predecessors`. When the meaning changes, use a new kind or schema
  version, and documents that name the old one are refused.
- **Saved plans.** A saved plan binds the executable's digest, the provider
  version and the semantic identity. A new build needs a new plan.
- **Engine names.** An engine name and its aliases are what operators type and
  write in manifests. Renaming one breaks every manifest and URL that uses it.

Pin the SDK by its tag, `provider-sdk/vX.Y.Z`, never a pseudo-version, and
commit no `replace` directive or workspace file, so a release builds from
published modules. The SDK README keeps the table of SDK releases and the
contract each one speaks; see
[Versions (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#versions).

## Verify a release

Point a manifest at a build downloaded from the release, on a machine that
did not build it, and check it:

```bash
ptah providers check --provider-config providers.json --provider-engine tutorial --json
```

The report says `"status": "compatible"`, names the version, and lists the
features the provider negotiated with this Ptah. For an engine with a catalog
entry, run `ptah providers install` with `--version` instead, and
`ptah providers check` finds the installation without `--provider-config`.

## Documentation for an official provider

An official provider keeps its reader pages in its repository's `docs/`
folder. Ptah's site imports them at the release its declaration names and
publishes them under `extend/providers/<engine>/`, and the provider's pull
requests run the same checks through Ptah's reusable `provider-docs.yml`
workflow. The [YDB provider's pages](../../providers/ydb/) are published this
way.

## Next steps

- To find the README section and the decision record behind a feature, use
  the [Provider SDK reference](../sdk-reference/).
- To see what operators do with a release, read
  [Test an executable provider](../../executable-providers/).
