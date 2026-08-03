# OCI Registry Artifacts

Ptah can publish migration directories and desired schemas as OCI 1.1
artifacts in a registry that accepts standard OCI manifests. The native Ptah
commands use the Docker credential store and do not require a Ptah account,
Atlas account, or Ptah-specific token.

This is a native Ptah workflow. It does not implement the Atlas Cloud API or
the `atlas://` scheme, and it does not change the Atlas-compatible command
boundary of the `ptah-compat` binary.

## Supported Workflows

| Workflow | Native command |
| --- | --- |
| Publish a migration artifact | `ptah migrations push <oci-reference>` |
| Reconstruct a migration directory | `ptah migrations pull <oci-reference> --out <dir>` |
| Apply directly from OCI | `ptah migrations up --migrations-dir <oci-reference>` |
| Inspect or roll back against OCI migrations | `ptah migrations status` / `down --migrations-dir <oci-reference>` |
| Lint and optionally attach the report | `ptah migrations lint --dir <oci-reference> [--attach]` |
| Publish a desired-schema artifact | `ptah schema push <oci-reference>` |
| Write a canonical schema file | `ptah schema pull <oci-reference> --out schema.hcl` |
| Compare or check drift from OCI | `ptah schema compare` / `drift --schema-file <oci-reference>` |
| Plan and optionally attach the report | `ptah migrations plan --schema-file <oci-reference> [--attach]` |
| Attach a deployment report | Best-effort after a successful OCI-backed `migrations up` |
| List direct referrer metadata | `ptah oci referrers <oci-reference>` |

Native lint and plan commands can publish referrer artifacts with `--attach`.
`ptah oci referrers` lists direct referrer metadata, with filters for Ptah lint,
plan, and deployment reports. Ptah does not currently expose a command to pull
or consume a referrer's payload.

## OCI Reference Syntax

Ptah accepts these forms:

```text
oci://registry.example/team/repository
oci://registry.example/team/repository:tag
oci://registry.example/team/repository@sha256:<64-lowercase-hex-characters>
```

An unqualified reference resolves to `:latest`:

```text
oci://ghcr.io/acme/app-migrations
oci://ghcr.io/acme/app-migrations:latest
```

Those references are equivalent when pulling.

Tags are movable registry pointers. A digest selects an immutable manifest and
is the only mechanically immutable deployment pin:

```text
oci://ghcr.io/acme/app-migrations:v20260728153000
oci://ghcr.io/acme/app-migrations@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

A sum file carries different weight depending on which of those you applied. A
sum verifies a directory against the sum stored beside it; for an OCI artifact
that sum travels inside the artifact. Anyone who can push to the repository can
rewrite the migrations, rehash them, and repoint a tag, and the verification
still passes. So a sum over a tag-resolved artifact proves internal
consistency, not that the bytes are the reviewed ones. `ptah migrations up`
says so out loud: when a sum verifies over a tag-resolved artifact it prints
the digest the tag resolved to and the `@sha256:` reference that pins it. A
digest reference already names exact bytes and gets no such line.

Every push applies:

- `latest`;
- a collision-resistant version tag generated as
  `vYYYYMMDDhhmmss-<random-base32>` in UTC, or the value supplied with
  `--version`;
- the tag in the positional reference, when one was supplied;
- every repeatable `--tag` value.

Duplicate tags are collapsed. Before moving any tag, Ptah rejects a generated
or explicit version tag that already resolves to different content. Generated
tags include cryptographic randomness to avoid same-second collisions.
Positional, `--tag`, and `latest` aliases remain movable.

The write-once check is a client-side preflight, so two concurrent writers can
still race when an explicit `--version` is reused. Configure immutable-tag
policy in the registry for version-tag prefixes when concurrent publishers are
possible. Use the returned `Digest:` value for a hard pin. Pushing to an
`@sha256:` reference is rejected, as is a reference containing both a tag and
a digest. If a later tag update fails after earlier tags moved, Ptah reports
the manifest digest, completed tags, and failed tag instead of presenting the
operation as having no externally visible effect.

References cannot contain embedded credentials, query strings, fragments,
escaped path separators, surrounding whitespace, or uppercase repository
components.

## Authentication And Transport Security

Ptah reads Docker's credential configuration. Run the login flow for the
registry before using Ptah:

```bash
docker login ghcr.io
ptah migrations pull \
  oci://ghcr.io/acme/app-migrations:stable \
  --out ./migrations
```

The client honors:

- `DOCKER_CONFIG`;
- the default Docker `config.json`;
- a configured `credsStore`;
- per-registry `credHelpers`.

Do not put a username, password, or token in an `oci://` reference. Ptah rejects
embedded user information.

HTTPS is the default. `--plain-http` disables transport encryption and must be
limited to an explicitly trusted local registry, such as a disposable
`localhost` registry used by tests. Do not use it for GHCR, ECR, GAR, Harbor,
Docker Hub, or a production registry. The flag is available on all listed
OCI-aware commands, including lint, plan, and `oci referrers`.

Registry operations and Docker credential-helper lookups have a two-minute
default deadline, with shorter dial, TLS-handshake, and response-header
deadlines.

### Identity, integrity, and authenticity

An OCI `@sha256:...` reference identifies exact bytes. It does not identify who
published those bytes. Likewise, `ptah.sum` and `atlas.sum` detect changes
inside a migration directory; they are not signatures and do not establish a
publisher identity. Ptah does not currently verify artifact signatures.

Protect production repositories with registry access controls, limit push
permission to trusted automation, and promote digest allowlists through the
deployment system. Use a registry-native signature policy when publisher
authenticity is required.

Treat attached reports according to their contents. Deployment reports are
deliberately redacted. A `plan.json` report includes schema fingerprints,
dialect and capability metadata, object names, risk assessments, and generated
SQL statements, so its repository may require the same confidentiality as the
schema and migrations.

## Publish And Consume Migrations

Create and verify the integrity file before publishing:

```bash
ptah migrations hash --dir ./migrations

ptah migrations push \
  oci://ghcr.io/acme/app-migrations \
  --migrations-dir ./migrations \
  --dir-format ptah \
  --verify-sum \
  --tag stable
```

`migrations push` captures the files Ptah uses for a migration run:

- SQL files;
- `ptah.sum` or `atlas.sum`;
- `.ptah-lint.yaml`.

Unrelated files in the source directory are not included. The artifact records
the migration directory format and stores each captured file as its own OCI
layer. `--dir-format` accepts `auto`, `ptah`, or `atlas`.

`--verify-sum` is opt-in. When enabled, the push fails before registry upload if
the required `ptah.sum` or `atlas.sum` is missing, changed, or otherwise
invalid. On `push` the directory being verified is the local one the author
controls, so the check has the full weight of a reviewed working tree behind
it. On `up` the same flag verifies a pulled artifact against the sum shipped
inside it; see the tag-versus-digest note above for what that does and does not
establish.

The command prints the canonical reference, manifest digest, version tag, and
all applied tags. Pin the digest when promoting the exact bytes to another
environment:

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

The same direct source works with rollback:

```bash
ptah migrations down \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --target 5 \
  --confirm
```

For `up`, `status`, and `down`, the artifact is pulled into an immutable
in-memory filesystem and passed to the existing migration engine. An explicit
`--dir-format` must match the format recorded in the artifact. The
`up --verify-sum` gate verifies the pulled files against the sum that travelled
with them, before opening the database. Applied to a digest reference, that
gate covers exactly the bytes named on the command line. Applied to a tag, it
covers whatever the tag resolved to at pull time, and `up` prints that digest
alongside the `@sha256:` reference to pin it.

To reconstruct the captured directory:

```bash
ptah migrations pull \
  "$MIGRATIONS" \
  --out ./pulled-migrations
```

The destination path must be absent. Ptah stages the files and renames them
into place so a failed pull does not leave a partial directory. Any existing
file, symlink, or directory, including an empty directory, is rejected without
being overwritten.

## Deployment Reports

After a successful OCI-backed `ptah migrations up`, Ptah attempts to attach a
`deployment.json` artifact as a referrer of the exact migration manifest digest
that was pulled. A report is attempted only when:

- the source was an OCI migration artifact;
- pending migrations were applied;
- the run was not a dry run;
- migration execution and final status inspection succeeded;
- `--skip-report` was not set.

The report records a generated deployment ID, the immutable migration artifact
digest, database dialect, before and after versions, the versions actually
added to committed revision state, UTC timestamps, and a `succeeded` outcome.
Its schema deliberately has no fields for database URLs, hostnames, environment
values, local paths, SQL text, or credentials. Failed migrations do not publish
a report.

Reporting is best-effort. If the migration succeeds but the report cannot be
attached, Ptah prints a warning and the migration command still succeeds. Use
`--skip-report` when the registry is read-only, referrer writes are prohibited,
or deployment reporting is intentionally handled elsewhere:

```bash
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir "$MIGRATIONS" \
  --verify-sum \
  --skip-report
```

## Attach Lint And Plan Reports

Lint directly from an OCI migration artifact:

```bash
ptah migrations lint \
  --dir "$MIGRATIONS" \
  --dialect postgres \
  --format json
```

Add `--attach` to publish the canonical `lint.json` report as a referrer of the
exact migration manifest digest resolved by the command:

```bash
ptah migrations lint \
  --dir "$MIGRATIONS" \
  --dialect postgres \
  --format json \
  --attach
```

`--attach` requires an OCI `--dir`. OCI lint sources do not support
`--git-base`, because the registry artifact has no local Git history.
The attachment is published before the command applies its normal lint failure
threshold, so a report with findings can be stored even when lint subsequently
exits with its negative-result code.

Plan a migration from exactly one OCI desired-schema artifact and attach the
canonical `plan.json` report to that schema digest:

```bash
SCHEMA=oci://ghcr.io/acme/app-schema@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

ptah migrations plan \
  --schema-file "$SCHEMA" \
  --db-url "$DATABASE_URL" \
  --report json \
  --attach
```

For plan attachment, the desired state must be exactly one OCI
`--schema-file`; combining it with local files, Go roots, or external schema
commands makes the attachment subject ambiguous and is rejected. Planning
without `--attach` can still use OCI schema artifacts in a composite desired
schema.

The attached plan is bound to the desired artifact digest and a SHA-256
fingerprint of the complete live schema used as current state. It also records
the dialect, selected schemas, effective capability set, and statement
assessments. Consumers can therefore detect that a plan was produced for a
different desired artifact or current database state.

Lint and plan attachments are explicit command outputs, not best-effort audit
metadata. If publication fails, the lint or plan command fails. Use
`--plain-http` only when the OCI source is in an explicitly trusted local
registry.

### Referrer discovery guarantees

Ptah publishes ordinary OCI subject manifests. When the registry implements the
OCI Referrers API, that API is the authoritative concurrent index. When it does
not, ORAS writes the standard referrers tag-schema index and Ptah also writes a
unique content-derived tag for each attachment. `ptah oci referrers` merges and
validates both sources, so concurrent Ptah processes cannot lose attachments by
overwriting one shared fallback index.

The content-derived tags are a Ptah discovery extension. Other OCI clients that
read only the standard tag-schema index may miss an entry after concurrent
cross-process publication. Use a registry with the native Referrers API when
complete cross-client audit discovery is required.

## List Referrer Metadata

List every direct referrer attached to a migration or schema artifact:

```bash
ptah oci referrers "$MIGRATIONS"
```

Filter to one Ptah report type and request machine-readable output:

```bash
ptah oci referrers "$MIGRATIONS" --type lint --format json
ptah oci referrers "$SCHEMA" --type plan --format json
```

`--type` accepts `all`, `lint`, `plan`, or `deployment`; `all` is the
default. `--format` accepts `text` or `json`; `text` is the default. The command
lists direct referrer descriptors. Text output includes digest, artifact type,
media type, and size; JSON output also includes annotations when present. It
does not download or interpret report payloads.

The subject uses the same `oci://` rules as every other OCI command. An
unqualified subject resolves to `:latest`, a tag resolves to its current
manifest, and a digest selects an immutable subject. Use a digest when the
listing must refer to the exact artifact used by a deployment, lint run, or
plan. Authentication comes from Docker's credential configuration, HTTPS is
the default, and `--plain-http` is only for an explicitly trusted local
registry.

## Publish And Consume Schemas

Publish from Go annotations:

```bash
ptah schema push \
  oci://ghcr.io/acme/app-schema \
  --root-dir ./models \
  --tag stable
```

Publish from one or more YAML, HCL, or SQL files:

```bash
ptah schema push \
  oci://ghcr.io/acme/app-schema \
  --schema-file ./schema.sql \
  --dialect postgres
```

Ptah resolves and merges the selected desired-schema sources, then renders
exactly one canonical `schema.hcl` layer. Publication fails closed if the schema
cannot be represented without loss. In particular:

- managed reference data is rejected;
- role passwords are rejected so credentials cannot be published in an
  artifact;
- any lossy renderer diagnostic rejects the artifact;
- the generated HCL must parse and render back to identical canonical bytes.

No partial or best-effort schema artifact is pushed.

Pulling validates the artifact type, layer media type, expected single
`schema.hcl` layout, and HCL syntax before creating the output:

```bash
ptah schema pull \
  oci://ghcr.io/acme/app-schema:stable \
  --out ./schema.hcl
```

The output file must not already exist.

Use the OCI schema directly for live comparison or drift detection through the
repeatable `--schema-file` input:

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

OCI schema inputs can participate in the same composite desired-schema loading
path as repeatable local `--schema-file` and `--root-dir` inputs.

## Atlas-To-OCI Concept Mapping

| Atlas Registry concept | Ptah OCI equivalent |
| --- | --- |
| `atlas://app` | `oci://registry.example/team/app` |
| Unpinned source resolves to latest | An unqualified `oci://` reference resolves to `:latest` |
| Atlas login and `ATLAS_TOKEN` | `docker login`, `DOCKER_CONFIG`, and Docker credential helpers |
| Registry version | Generated `vYYYYMMDDhhmmss-<random-base32>` tag or explicit `--version` |
| Movable tag | Reference tag or repeatable `--tag` |
| Immutable version/content pin | OCI manifest `@sha256:...` digest |
| Migration directory plus `atlas.sum` | Per-file OCI layers, including `atlas.sum` or `ptah.sum` |
| Desired schema in Atlas Registry | Canonical, lossless `schema.hcl` OCI artifact |
| Deployment audit record | Best-effort deployment-report referrer after native `migrations up` |
| Stored lint results or migration plans | `migrations lint --attach` and `migrations plan --attach`; list direct descriptor metadata with `ptah oci referrers` |
| Atlas Cloud UI, promotion, policy, and account model | Not provided; use registry-native access control and tooling |

The native OCI commands do not make the Atlas-compatible `migrate push` or
`schema push` implemented commands. Those paths continue to mirror the Atlas
community-edition unsupported boundary, and the Atlas-compatible
`migrate apply` does not gain native OCI transport flags.

## GitHub Actions And GHCR

The job needs `packages: write`. Docker login writes credentials that Ptah reads
through the Docker credential store:

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
        run: go install go.5x5.cz/ptah/cmd/ptah@latest

      - name: Publish migration artifact
        shell: bash
        run: |
          ref="oci://ghcr.io/${GITHUB_REPOSITORY,,}-migrations"
          ptah migrations push "$ref" \
            --migrations-dir ./migrations \
            --verify-sum \
            --tag "$GITHUB_SHA"
```

Replace `@latest` with a release tag or commit in production CI so the Ptah
binary itself is pinned. If the GHCR package already exists under an
organization, grant the repository's Actions workflow write access in the
package settings.

For GitHub's current token and package permission model, see
[Publishing and installing a package with GitHub
Actions](https://docs.github.com/en/packages/managing-github-packages-using-github-actions-workflows/publishing-and-installing-a-package-with-github-actions).

## Related Documentation

- [Native CLI command reference](./site/src/content/docs/reference/native-commands.md)
- [Dialect and cross-cutting capabilities](./capabilities.md)
- [CLI exit codes](./exit_codes.md)
- [Issue #664](https://github.com/stokaro/ptah/issues/664)
