# Release Process

Ptah releases are produced by GoReleaser from annotated version tags.

## Prerequisites

- `HOMEBREW_TAP_TOKEN` repository secret with permission to push to
  `stokaro/homebrew-ptah`.
- GitHub Actions package permissions enabled for publishing
  `ghcr.io/stokaro/ptah`.
- GoReleaser `v2.15.4`. The GitHub Actions workflow pins this version because
  issue #174 requires a Homebrew formula install command
  (`brew install stokaro/ptah/ptah`), while newer GoReleaser releases treat
  formula publishing as deprecated in favor of casks.
- The release workflow must pass on the release commit before tagging.

## Cut A Release

1. Start from a clean `master` checkout.
2. Verify the release candidate:

   ```bash
   go test ./...
   golangci-lint run ./...
   goreleaser check
   goreleaser release --snapshot --clean --skip=sign,docker
   ```

3. Create and push an annotated semver tag:

   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

4. Wait for the `Release` workflow to finish.
5. Verify the GitHub Release contains archives for Linux, macOS, and Windows,
   `checksums.txt`, SBOM artifacts, and cosign signature/certificate files.
6. Verify the container images exist:

   ```bash
   docker pull ghcr.io/stokaro/ptah:v0.1.0
   docker pull ghcr.io/stokaro/ptah:latest
   ```

7. Verify Homebrew install:

   ```bash
   brew update
   brew install stokaro/ptah/ptah
   ptah version
   ptah-ls --version
   ptah-compat migrate --help
   ```

## Local Snapshot

Use a snapshot release to validate packaging without publishing:

```bash
goreleaser release --snapshot --clean --skip=sign,docker
./dist/ptah_darwin_arm64*/ptah version
./dist/ptah_darwin_arm64*/ptah-ls --version
./dist/ptah_darwin_arm64*/ptah-compat migrate --help
ln -sf "$(pwd)"/dist/ptah_darwin_arm64*/ptah-compat /tmp/atlas
/tmp/atlas schema inspect --help
```

Snapshot releases should print snapshot version metadata, the current commit,
the build date, Go version, and platform for the shipped binaries.

If `syft` is not installed locally, skip SBOM generation for the local packaging
smoke test only:

```bash
goreleaser release --snapshot --clean --skip=sign,docker,sbom
```

The GitHub Actions release workflow installs `syft` and `cosign`; do not skip
SBOM generation, checksum signing, Docker publishing, or Homebrew publishing for
real tag releases.
