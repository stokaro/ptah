# Release Process

Ptah releases are produced by GoReleaser from annotated version tags.

## When A Release Is Owed

A capability that only exists on `master` is a capability nobody downstream can
ship against. `stokaro/ptah-operator` is the reader that makes this concrete: it
names the Ptah build it executes by digest-pinned image and by `ptahVersion`,
and it supports exactly the build its lifecycle matrix exercised. Pinning a
development commit is what it does while no tag carries what it needs, and its
own record says so as a limitation rather than as support.

So a contract another repository executes against -- a machine-readable document
it parses, a layer it reads out of an artifact, a verb it drives -- is finished
when a release carries it, not when it merges. Cutting the tag is the last step
of that work, and whoever needs it should say which contracts they are waiting
on rather than which commit they pinned: a commit answers what runs today, and a
version is what a user can install.

Nothing here schedules a release. What this section settles is that "it is on
master" is not an answer to "can the operator ship it", and that a downstream
waiting on one is a reason to cut it rather than a note on its own backlog
(stokaro/ptah#3344).

## Prerequisites

- For Homebrew publishing: the `stokaro/homebrew-ptah` tap repository and a
  credential that can push to it. `Contents: Read and write` on that repository
  is the whole of what GoReleaser needs: it reads the default branch, reads the
  cask path for its SHA, and writes the file.

  The release mints that credential from the GitHub App behind the
  `PUBLISH_APP_ID` variable and the `PUBLISH_APP_KEY` secret, narrowed to
  `homebrew-ptah` alone. `HOMEBREW_TAP_TOKEN`, a fine-grained token with the
  same single permission, is the fallback where the app pair is absent.

  Both are required, and the release workflow's first step refuses a run with
  neither before anything is built. `skip_upload` is `false`, so a release
  cannot finish green with the Homebrew channel missing.

  An **expired** fine-grained token fails the same run later, at the cask
  push, because a presence check cannot tell a live token from a dead one. That
  is the failure the app removes: its token is minted per run and lives an hour,
  and the release job is capped at 40 minutes so the token cannot expire while
  the job still holds it. `scripts/check-app-token-lifetime.sh` holds that
  margin for every job that mints one.
- GitHub Actions package permissions enabled for publishing
  `ghcr.io/stokaro/ptah`.
- No signing key or secret. The `release` job signs the checksum file and the
  SBOMs, and the `Sign the release images` job signs the image index, each
  with its own GitHub OIDC token. `id-token: write` on those two jobs grants
  it, and on no pull-request job, so there is no key to rotate or leak.
- `DOCKER_HUB_USER` and `DOCKER_HUB_TOKEN` as repository secrets, for the
  Docker Hub mirror. The account is a secret rather than the repository owner's
  name because the two are different things that look alike: `stokaro` on
  Docker Hub is an organization, and a `dckr_pat_` token belongs to a person
  who is a member of it. The token needs write access to `stokaro/ptah`;
  delete is not used.
- GoReleaser publishes Homebrew as a cask, into `Casks/ptah.rb`. `brew install
  stokaro/ptah/ptah`, the command issue #174 requires, reaches it only while the
  tap holds no `Formula/ptah.rb`: Homebrew tries the formula loader first, and a
  formula an earlier release generated answers the command with its own version.
  `tap_migrations.json` is not the answer -- both loaders stop when the migrated
  name equals the name they were given, so a tap cannot redirect a token to
  itself. The generated formula is removed from the tap once, and
  `Formula/ptah-edge.rb`, which is hand-written, stays.
- The release workflow must pass on the release commit before tagging.
- The npm organization or user scope `@stokaro` must exist before publishing
  `@stokaro/ptah`. The publisher needs permission to create seven public
  packages: the CLI package and one binary package for each supported host.
  npm access is not needed to build or verify the packages.

## npm CLI Distribution

`npm/scripts/stage.mjs` verifies the checksums of the six GoReleaser archives
and packages their `ptah`, `ptah-compat`, and `ptah-ls` binaries. The small
`@stokaro/ptah` package selects the matching platform package at runtime.
Installation runs no lifecycle script and downloads nothing outside npm.

The first npm release needs an interactive npm login because a trusted
publisher cannot be configured until the packages exist. Cut a new Ptah
release from the commit that contains this packaging code. Download that
release's six platform archives and `checksums.txt`, verify the checksum
signature as in the workflow, then run:

```bash
node npm/scripts/stage.mjs --version X.Y.Z --assets ./release-assets --output ./npm-staged
node npm/scripts/publish.mjs --staged ./npm-staged
```

The publisher checks a published version's tarball integrity before skipping
it, so the command can resume after a partial upload. A prerelease version is
published under the `next` dist-tag, so `latest` stays on the newest release.
`--dry-run` after the directory runs every check and uploads nothing; the
release check runs it on the snapshot packages, whose version is a prerelease. It publishes the six
binary packages before the CLI package. Do not reuse a Ptah version tag to
repair a different npm tarball.

After the first publish, configure npm trusted publishing for each of the
seven packages: GitHub repository `stokaro/ptah`, workflow `release.yml`, and
permission to run `npm publish`. Then set the repository variable
`PTAH_NPM_PUBLISH=true`. Later release tags publish from signed GoReleaser
assets through the `Publish npm CLI packages` job. Until the variable is set,
that job stays skipped and npm is not a release channel.

## Cut A Release

1. Start from a clean `master` checkout.
2. Verify the release candidate:

   ```bash
   go test ./...
   make lint-golangci
   goreleaser check
   goreleaser release --snapshot --clean --skip=sign,docker
   ```

3. Create and push an annotated semver tag. Never move or delete a pushed tag;
   if a release run fails, fix the cause and cut the next patch version:

   ```bash
   git tag -a v0.1.2 -m "Release v0.1.2"
   git push origin v0.1.2
   ```

   A tag with a prerelease part, such as `v0.2.0-rc.1`, publishes a release
   candidate under its own version and moves nothing that names the newest
   release. GitHub marks it a prerelease, the images carry the version tag
   and not `latest`, the Homebrew cask is not uploaded, and npm takes it
   under the `next` dist-tag. The documentation site builds no version for
   it.

4. Wait for the `Release` workflow to finish. Its changelog runs from the
   nearest earlier `v*` tag, which the workflow passes to GoReleaser as
   `GORELEASER_PREVIOUS_TAG`. `provider-sdk/v*` and `sqlsyntax/v*` tags also sit
   on `master`, and GoReleaser would otherwise start the changelog at the newest
   of them.
5. Verify the GitHub Release contains archives for Linux, macOS, and Windows,
   the source archive, `checksums.txt`, SBOM artifacts, and one
   `<artifact>.sigstore.json` cosign bundle per checksum and SBOM file. Verify a
   bundle:

   ```bash
   cosign verify-blob --bundle checksums.txt.sigstore.json checksums.txt \
     --certificate-identity "https://github.com/stokaro/ptah/.github/workflows/release.yml@refs/tags/v0.1.2" \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```

   No archive or image carries a database provider. Each provider publishes its
   own releases, and users install one with `ptah providers install <engine>`
   ([ADR 0031](adr/0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md)).

6. Verify the container images contain `ptah`, `ptah-compat`, and `ptah-ls`
   in `/usr/local/bin` for both Linux architectures. The PR release check
   builds the snapshot images and runs every binary on amd64 and arm64.
   It also checks that `/usr/share/licenses/ptah/LICENSE` matches the source
   license, so downstream images can copy it alongside the binaries.
   GHCR is the address the documentation
   gives a reader, because Docker Hub rate-limits anonymous pulls and GHCR does
   not; Docker Hub carries the same images as a mirror:

   ```bash
   docker pull ghcr.io/stokaro/ptah:0.1.2
   docker pull ghcr.io/stokaro/ptah:latest

   docker pull docker.io/stokaro/ptah:0.1.2
   docker pull docker.io/stokaro/ptah:latest
   ```

   The release workflow already asserts this, anonymously and by digest, in its
   `Both registries carry this release` step. A tag that reached one registry
   and not the other fails the release rather than leaving a reader unable to
   tell which host is behind. Given the digest GoReleaser recorded, the step
   also requires both version tags to name it:

   ```bash
   scripts/check-release-image-mirror.sh 0.1.2
   scripts/check-release-image-mirror.sh 0.1.2 sha256:<digest>
   ```

7. Verify the image signatures. The `release` job reads the index digest
   GoReleaser pushed and passes it on. The `Sign the release images` job then
   runs the mirror check above, signs the index once per registry, by digest,
   with keyless cosign, and verifies each registry anonymously with the
   identity and issuer the
   [install page](site/src/content/docs/start/install-options.mdx) documents.
   The mirror check and the verification each read a host again for up to a
   minute while it is behind. Any of the steps failing fails the release, and
   `Publish npm CLI packages` waits for the signing job. The signing runs in
   the workflow rather than in GoReleaser's `docker_signs`, which would sign
   before the mirror check and once per tag. The same verification by hand,
   given the digest the version tag names:

   ```bash
   DIGEST="$(docker buildx imagetools inspect ghcr.io/stokaro/ptah:0.1.2 --format '{{.Manifest.Digest}}')"
   scripts/check-release-image-signatures.sh verify v0.1.2 "$DIGEST"
   ```

   The images, the GitHub Release and the Homebrew formula are public before
   they are signed. GoReleaser publishes all of them in the `release` job, and
   the signing job starts after it. Until the signing job passes, `cosign
   verify` reports `no signatures found` for the new digest, which is the
   right answer for an image nothing has signed yet. That is normally a few
   minutes. When the signing job fails, the window lasts until a rerun of it
   passes. Announce the release only after the signing job has passed.

   When `Sign the release images` fails, use **Re-run failed jobs**. It runs
   the signing job again with the digest the `release` job published, under
   the same identity, because that identity names `release.yml` and the tag.
   Signing a digest that already has a signature adds a second one beside it.
   `cosign sign` does not retry, so a transient Fulcio or Rekor error fails the
   signing job, and **Re-run failed jobs** is the recovery for that too. The
   rerun repeats the mirror check. If the version tag names another digest by
   then, for example after a full rerun or a manual push, the rerun refuses
   there. That is intended, because the signature would cover a digest no
   reader resolves from the tag. Follow step 3 and cut the next patch version.
   Never use **Re-run all jobs** on a published release. It runs GoReleaser
   again, which builds and pushes a new index. The attestations buildx
   attaches record the build, so the new index has a new digest. The version
   tag and `latest` move to it, and only the new digest is signed. The first
   digest stays in both registries without a signature, so anyone who pinned
   it during the window cannot verify it. A failure in the `release` job
   after GoReleaser pushed the images has the same effect when it is rerun,
   so for that failure follow step 3 and cut the next patch version.

   Snapshots are never pushed or signed, and no pull-request job holds
   `id-token: write`. The signature identifies the publisher of the digest. It
   is separate from the SBOM files and from build provenance, and it makes no
   SLSA claim.

8. Verify the versioned documentation. The tag's `Docs` run deploys it under a
   commit of its own — Pages keys a deployment by a commit the repository holds,
   and the tag's own commit is the one `master` already deployed — and the
   deploy job waits for the public site to serve that build before it finishes.
   Where the deploy job skips instead, because a newer deploy is already
   served, the run fails unless the served site lists the tag. So a green run
   is the answer. These two commands are what it checked:

   ```bash
   curl -s https://docs.ptah.run/versions.json
   curl -o /dev/null -w '%{http_code}\n' https://docs.ptah.run/v0.1.2/
   ```

   The site serves edge and every release that carries a docs site. Each
   release keeps its version-picker entry and its published URLs. A failed
   historical build fails the deployment, and the deployment guard refuses
   a candidate that removes a served version.
   Each older release page carries a warning with its version and a link to
   the latest release. The warning is rendered into the published HTML and
   works without JavaScript, including releases below the UI overlay's floor.
   `latest` serves the newest release at `/latest/`. Versioned pages use its
   matching page as their canonical URL. A page absent from `latest` has no
   canonical, and its archive warning links to the latest documentation home.

9. Verify the Homebrew install:

   ```bash
   brew update
   brew install stokaro/ptah/ptah
   ptah version
   ptah-ls --version
   ptah-compat migrate --help
   ```

10. When `PTAH_NPM_PUBLISH=true`, verify all three npm commands from a fresh
    project. Pin the tag's version rather than using npm's `latest` tag:

    ```bash
    npx @stokaro/ptah@X.Y.Z version
    npx --package=@stokaro/ptah@X.Y.Z ptah-compat version
    npx --package=@stokaro/ptah@X.Y.Z ptah-ls --version
    ```

## The Tap Holds A Cask And A Formula

`Casks/ptah.rb` is generated. GoReleaser renders it from the `homebrew_casks:`
section of `.goreleaser.yaml` and pushes it on every version tag, so it always
names the newest release, and `brew install stokaro/ptah/ptah` reaches it
because no formula of that token stands in front of it.

`Formula/ptah-edge.rb` is written by hand and lives only in the tap. It is a
head-only formula that compiles the tip of `master` on the user's machine, which
is the one thing a generated artifact cannot be: GoReleaser has no field for a
Homebrew `head`, and a release run overwrites what it generates whole.

GoReleaser writes that one path and no other, so the hand-written file survives
a release -- and nothing regenerates it either. **A change to how the release
binaries are built belongs in both places.** The edge formula repeats the
release build's `CGO_ENABLED=0`, its `-trimpath`, its `buildinfo` ldflags, and
its list of three binaries; when any of those move in `.goreleaser.yaml` and not
in the formula, edge quietly stops being the edge of what ships.

That coupling is checked, and the check lives in the tap because the file it
guards does: `scripts/check-edge-matches-release.sh` there reads this
repository's `.goreleaser.yaml` and requires the formula to still repeat it. It
reads the `builds:` section for those flags, so it is unaffected by which
package section renders the release. The
change that breaks it is made here, where nothing can see that file, so the
check runs on a daily schedule as well as on every push to the tap -- a drift
introduced by a change on this side surfaces within a day rather than at the
next install.

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
