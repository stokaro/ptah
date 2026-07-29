---
title: OCI registry artifacts
description: Publish, pin, pull, and consume Ptah migrations and desired schemas through an OCI registry.
---

Ptah can store migration directories and desired schemas as OCI 1.1 artifacts
in GHCR, ECR, GAR, Harbor, Docker Hub, a local `registry:2`, or another
OCI-compliant registry. Authentication comes from the Docker credential store;
there is no Ptah account or Ptah-specific registry token.

This is a native Ptah workflow. It does not implement Atlas Cloud or the
`atlas://` scheme.

## Choose a reference

```text
oci://registry.example/team/repository
oci://registry.example/team/repository:tag
oci://registry.example/team/repository@sha256:<64-lowercase-hex-characters>
```

An unqualified reference resolves to `:latest`. Tags are movable pointers. A
manifest digest is immutable and is the appropriate pin for a reproducible
deployment:

```text
oci://ghcr.io/acme/app-migrations:stable
oci://ghcr.io/acme/app-migrations@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

Every push writes `latest`, a collision-resistant generated UTC version tag in
`vYYYYMMDDhhmmss-<random-base32>` form, the positional reference tag when
present, and each repeatable `--tag`. `--version` overrides the generated
version tag.

:::caution[Tags are not immutable]
Ptah rejects a generated or explicit version tag that already resolves to
different content, and generated tags include cryptographic randomness.
This is a client-side preflight: concurrent writers reusing an explicit
`--version` still require registry-side immutable-tag policy. Positional,
`--tag`, and `latest` aliases remain movable. Only the returned
`@sha256:...` digest is mechanically immutable.
:::

If a later tag update fails after earlier tags moved, Ptah reports the manifest
digest, completed tags, and failed tag.

Ptah also rejects references that contain both a tag and digest, embedded
credentials, query strings, fragments, escaped path separators, surrounding
whitespace, or uppercase repository components.

## Authenticate securely

Log in with the registry's normal Docker flow:

```bash
docker login ghcr.io
```

Ptah honors `DOCKER_CONFIG`, Docker's default `config.json`, `credsStore`, and
per-registry `credHelpers`. Do not include credentials in the `oci://`
reference.

HTTPS is the default. `--plain-http` disables transport encryption and is only
for an explicitly trusted local registry used for development or tests. Do not
use it with GHCR, ECR, GAR, Harbor, Docker Hub, or a production registry.
This applies to `ptah oci referrers` as well as push, pull, and direct-consumer
commands.

Registry operations and Docker credential-helper lookups have a two-minute
default deadline, with shorter dial, TLS-handshake, and response-header
deadlines.

### Identity, integrity, and authenticity

An OCI digest identifies exact bytes, not the publisher. `ptah.sum` and
`atlas.sum` detect changes inside a migration directory, but they are not
signatures. Ptah does not currently verify artifact signatures.

Use registry access controls, trusted writers, digest allowlists, and a
registry-native signature policy when publisher authenticity is required.
Deployment reports are deliberately redacted. `plan.json` contains schema
fingerprints, dialect and capability metadata, object names, risk assessments,
and generated SQL statements, so protect it like schema and migration content.

## Publish migrations

Hash and verify the directory before publishing:

```bash
ptah migrations hash --dir ./migrations

ptah migrations push \
  oci://ghcr.io/acme/app-migrations \
  --migrations-dir ./migrations \
  --dir-format ptah \
  --verify-sum \
  --tag stable
```

The migration artifact contains only the inputs Ptah captures for a migration
run:

- SQL files;
- `ptah.sum` or `atlas.sum`;
- `.ptah-lint.yaml`.

Unrelated files are excluded. `--dir-format` accepts `auto`, `ptah`, or `atlas`.
With `--verify-sum`, a missing or mismatched integrity file fails before the
registry upload.

The command prints the manifest digest. Promote that exact artifact by digest:

```bash
MIGRATIONS=oci://ghcr.io/acme/app-migrations@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

ptah migrations status \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --json

ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --verify-sum
```

`ptah migrations down` accepts the same direct source:

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --target 5 \
  --confirm
```

`up`, `status`, and `down` pull into an immutable in-memory filesystem and use
the ordinary Ptah migration engine. An explicit `--dir-format` must match the
artifact metadata. `up --verify-sum` verifies the pulled artifact before
opening the database.

`ptah migrations lint --dir oci://...` also reads an OCI migration artifact
directly. See [Attach lint and plan reports](#attach-lint-and-plan-reports) for
the optional referrer output.

## Reconstruct a migration directory

```bash
ptah migrations pull \
  "$MIGRATIONS" \
  --out ./pulled-migrations
```

The destination path must be absent. Ptah stages the captured files and
renames them into place so a failed pull does not leave a partial directory.
Any existing file, symlink, or directory, including an empty directory, is
rejected without being overwritten.

## Understand deployment reporting

After an OCI-backed `migrations up` applies pending migrations successfully,
Ptah attempts to attach a `deployment.json` referrer to the exact migration
manifest digest that was pulled.

The report records:

- a generated deployment ID;
- the migration artifact digest;
- the database dialect;
- before and after versions;
- versions actually added to committed revision state;
- UTC start and finish timestamps;
- a `succeeded` outcome.

The report schema has no fields for database URLs, hostnames, environment
values, local paths, SQL text, or credentials.

No report is attempted for a local migration directory, a dry run, a no-op run,
a failed migration, or a failed final status check. Report attachment is
best-effort: a registry failure prints a warning but does not turn a successful
migration into a failed command.

Use `--skip-report` when the registry is read-only or another system owns the
deployment record:

```bash
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --verify-sum \
  --skip-report
```

## Attach lint and plan reports

Lint directly from an OCI migration artifact:

```bash
ptah migrations lint \
  --dir "$MIGRATIONS" \
  --dialect postgres \
  --format json
```

Add `--attach` to publish the canonical `lint.json` report as a referrer of the
exact migration manifest digest:

```bash
ptah migrations lint \
  --dir "$MIGRATIONS" \
  --dialect postgres \
  --format json \
  --attach
```

`--attach` requires an OCI `--dir`. `--git-base` is not supported with an OCI
lint source because the artifact has no local Git history. Ptah publishes the
report before applying the normal lint failure threshold, so findings can be
stored even when lint then returns its negative-result code.

Attach a canonical `plan.json` report to an OCI desired-schema artifact:

```bash
SCHEMA=oci://ghcr.io/acme/app-schema@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

ptah migrations plan \
  --schema-file "$SCHEMA" \
  --db-url "$DATABASE_URL" \
  --report json \
  --attach
```

Plan attachment requires exactly one OCI `--schema-file`. Combining that source
with a local schema file, Go root, or external schema command is rejected
because Ptah cannot choose an unambiguous subject. Planning without `--attach`
can still include OCI schema artifacts in a composite desired schema.

The plan records the desired artifact digest, a SHA-256 fingerprint of the
complete live schema used as current state, the dialect, selected schemas,
effective capabilities, and statement assessments.

Lint and plan attachments are not best-effort. If publication fails, the
command fails.

### Referrer discovery guarantees

With a native OCI Referrers API, the registry provides the concurrent index.
Without it, ORAS writes the standard referrers tag-schema index and Ptah also
writes a unique content-derived tag for every attachment. `ptah oci referrers`
merges and validates both sources, so concurrent Ptah processes do not lose
attachments through one shared fallback tag.

The content-derived tags are a Ptah extension. A different client that reads
only the standard tag-schema index can miss an entry after concurrent
cross-process publication. Use a registry with the native Referrers API for
complete cross-client audit discovery.

## List referrer metadata

List every direct referrer attached to an artifact:

```bash
ptah oci referrers "$MIGRATIONS"
```

Filter by Ptah report type and select JSON output when automation needs stable
fields:

```bash
ptah oci referrers "$MIGRATIONS" --type lint --format json
ptah oci referrers "$SCHEMA" --type plan --format json
```

`--type` accepts `all`, `lint`, `plan`, or `deployment`, and `--format` accepts
`text` or `json`. Defaults are `all` and `text`. The result contains direct
referrer descriptor metadata. Text output contains digest, artifact type, media
type, and size; JSON output also contains annotations when present. Ptah does
not currently expose a command to download or interpret the report payload.

The subject follows the ordinary reference rules. An unqualified subject
resolves to `:latest`, a tag resolves to its current manifest, and a digest
selects an immutable subject. Use a digest to inspect attachments for the exact
artifact used by a deployment or analysis run. The command uses Docker
credentials and HTTPS by default; `--plain-http` is only for an explicitly
trusted local registry.

## Publish a desired schema

Publish Go annotations:

```bash
ptah schema push \
  oci://ghcr.io/acme/app-schema \
  --root-dir ./models \
  --tag stable
```

Or publish one or more YAML, HCL, or SQL sources:

```bash
ptah schema push \
  oci://ghcr.io/acme/app-schema \
  --schema-file ./schema.sql \
  --dialect postgres
```

Ptah merges the selected sources and renders one canonical `schema.hcl` layer.
Schema publication fails closed:

- managed reference data is rejected because it cannot be represented without
  loss;
- role passwords are rejected so credentials cannot be published in an
  artifact;
- any lossy HCL renderer diagnostic rejects publication;
- the generated HCL must parse and render back to identical canonical bytes.

Ptah never pushes a partial schema artifact.

Pulling validates the artifact and creates a new canonical HCL file:

```bash
ptah schema pull \
  oci://ghcr.io/acme/app-schema:stable \
  --out ./schema.hcl
```

The output file must not already exist.

## Compare and check drift

Use an OCI schema through the repeatable `--schema-file` option:

```bash
SCHEMA=oci://ghcr.io/acme/app-schema@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

ptah schema compare \
  --db-url "$DATABASE_URL" \
  --schema-file "$SCHEMA"

ptah schema drift \
  --db-url "$DATABASE_URL" \
  --schema-file "$SCHEMA" \
  --format json
```

OCI schema inputs can be combined with other repeatable `--schema-file` and
`--root-dir` inputs through Ptah's composite schema loader.

## Map Atlas concepts to OCI

| Atlas Registry | Ptah OCI |
| --- | --- |
| `atlas://app` | `oci://registry.example/team/app` |
| Unpinned source resolves to latest | Unqualified `oci://` resolves to `:latest` |
| Atlas login and `ATLAS_TOKEN` | `docker login`, `DOCKER_CONFIG`, and credential helpers |
| Registry version | Generated `vYYYYMMDDhhmmss-<random-base32>` tag or explicit `--version` |
| Movable tag | Reference tag or repeatable `--tag` |
| Immutable content pin | OCI manifest `@sha256:...` digest |
| Migration directory plus `atlas.sum` | Per-file OCI layers, including `atlas.sum` or `ptah.sum` |
| Desired schema | Canonical, lossless `schema.hcl` artifact |
| Deployment audit | Best-effort deployment-report referrer after native `migrations up` |
| Lint results and migration plans | `migrations lint --attach` and `migrations plan --attach`; list direct descriptor metadata with `ptah oci referrers` |
| Cloud UI, promotion, policy, and accounts | Not provided; use registry-native controls |

The native workflow does not make the Atlas-compatible `migrate push` or
`schema push` implemented commands. Those paths remain Atlas community-edition
boundary stubs in the `ptah-compat` binary, and the Atlas-compatible apply
command does not gain native OCI transport flags.

## Publish to GHCR from GitHub Actions

Grant the job `packages: write`. The Docker login action writes credentials that
Ptah reads from Docker's configuration:

```yaml
name: Publish migrations

on:
  push:
    branches: [main]

permissions:
  contents: read
  packages: write

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7

      - uses: actions/setup-go@v6
        with:
          go-version: "1.26.x"

      - name: Log in to GHCR
        uses: docker/login-action@v4
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Install Ptah
        run: go install github.com/stokaro/ptah/cmd/ptah@latest

      - name: Publish migration artifact
        shell: bash
        run: |
          ref="oci://ghcr.io/${GITHUB_REPOSITORY,,}-migrations"
          ptah migrations push "$ref" \
            --migrations-dir ./migrations \
            --verify-sum \
            --tag "$GITHUB_SHA"
```

Pin the Ptah install to a release or commit in production CI. Existing
organization packages may also need the repository's Actions workflow granted
write access in the package settings. GitHub documents the current token and
package permission model in [Publishing and installing a package with GitHub
Actions](https://docs.github.com/en/packages/managing-github-packages-using-github-actions-workflows/publishing-and-installing-a-package-with-github-actions).

## Next steps

- Verifying and gating what you publish and apply:
  [Integrity and safety](../../versioned/integrity-and-safety/).
- The complete verb and flag inventory for the OCI commands:
  [Native commands](../../reference/native-commands/).
- Which registry-backed features are supported where:
  [Capabilities](../../reference/capabilities/).
