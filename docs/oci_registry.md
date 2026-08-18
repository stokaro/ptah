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
| Verify artifact integrity without a database | `ptah migrations validate --dir <oci-reference>` |
| Publish a desired-schema artifact | `ptah schema push <oci-reference>` |
| Write a canonical schema file | `ptah schema pull <oci-reference> --out schema.hcl` |
| Compare or check drift from OCI | `ptah schema compare` / `drift --schema-file <oci-reference>` |
| Render, export, or inspect an OCI schema | `ptah schema render` / `export` / `inspect --schema-file <oci-reference>` |
| Plan or apply a schema from OCI | `ptah schema plan` / `apply --schema-file <oci-reference>` |
| Generate a migration toward an OCI schema | `ptah migrations generate --schema-file <oci-reference>` |
| Plan and optionally attach the report | `ptah migrations plan --schema-file <oci-reference> [--attach]` |
| Attach a deployment report | Best-effort after a successful OCI-backed `migrations up` |
| List direct referrer metadata | `ptah oci referrers <oci-reference>` |
| Read an attached report's payload | `ptah oci fetch <oci-reference> [--type ...] [--digest ...]` |
| Pin a mutable tag to a digest | `ptah oci resolve <oci-reference>` |
| Read a manifest without downloading it | `ptah oci inspect <oci-reference>` |
| List the aliases a repository carries | `ptah oci tags <oci-reference>` |
| Promote without rebuilding | `ptah oci tag <oci-reference> <tag>...` |
| Promote into another repository | `ptah oci copy <source> <destination> [--recursive]` |
| Ask what the registry supports | `ptah oci capabilities <oci-reference>` |

Native lint and plan commands can publish referrer artifacts with `--attach`.
`ptah oci referrers` lists direct referrer metadata, with filters for Ptah lint,
plan, and deployment reports, and `ptah oci fetch` returns the payload behind a
descriptor — so a report Ptah published is a report Ptah can read back, without
ORAS or a raw registry call.

`fetch` never guesses which attachment was meant. One candidate is fetched;
several are refused with their digests printed, so a pipeline that would have
silently taken "the latest" fails while its author is watching. Narrow with
`--type`, or name one exactly with `--digest`. The same rule governs the files
inside the chosen referrer: one is written, several require `--file`.

`resolve` prints the pinned reference a tag currently names, which is the first
half of the operation that makes a deployment reproducible — record the digest
once, then pass it to every later step instead of resolving the tag again at
each one:

```bash
DIGEST=$(ptah oci resolve "oci://ghcr.io/acme/app-migrations:latest")
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir "$DIGEST" --verify-sum
```

`inspect` reads the manifest and stops there: artifact type, subject,
annotations, and each file layer's name, media type, size, and digest, with no
file downloaded. It also reports how each referrer was discovered, which is the
one fact the other verbs cannot show. See
[Referrer discovery guarantees](#referrer-discovery-guarantees) for what the
`durable-tag` value means for clients other than Ptah.

## Promotion

`build -> staging -> production` does not need a rebuild, and should not have
one. A push creates an immutable artifact **and** moves aliases in the same
operation, so promoting through it re-derives content that was already
reviewed: what reaches production is an artifact equal to the reviewed one
rather than the same one.

Moving the alias keeps the digest identical by construction:

```bash
DIGEST=$(ptah oci resolve "oci://ghcr.io/acme/app-migrations:latest")
ptah oci tag "$DIGEST" staging
# after the staging soak, the same bytes are promoted again
ptah oci tag "oci://ghcr.io/acme/app-migrations:staging" production
```

Aliases move one at a time. If a later one fails, the ones already applied are
named, because an operator told only that the command failed still has to go
and find out which environment now points at the new build.

Crossing a repository boundary is `copy`:

```bash
ptah oci copy --recursive \
  "oci://ghcr.io/acme/app-migrations:v20260728153000" \
  "oci://registry.internal/acme/app-migrations:production"
```

`--recursive` carries the artifact's referrers with it. Without it the copy
arrives with its lint results, plans, deployment reports, and signatures left
behind in the source repository — the promotion succeeds and silently loses the
evidence it was promoted on. A digest destination is refused: a digest names
content that already exists, so there is nothing for a copy to create there.

## Reaching a private registry

A registry with its own certificate authority needs one setting, and without it
every OCI verb answers with a TLS error that names a certificate rather than a
configuration:

| Variable | What it carries |
| --- | --- |
| `PTAH_OCI_CA_FILE` | A PEM bundle trusted **in addition to** the system roots |
| `PTAH_OCI_CLIENT_CERT` | The certificate presented for mutual TLS |
| `PTAH_OCI_CLIENT_KEY` | Its private key |

The authority is added to the system roots rather than replacing them: naming
an internal authority means "trust this as well", and replacing the pool would
break every other registry the same run talks to, as a failure that names no
cause. The mutual-TLS pair must be complete — setting one without the other is
refused, because half a credential authenticates nothing and the run would
otherwise fail later at the registry with an error about authorization rather
than about configuration.

There is no password setting and there will not be one. A credential passed on
a command line lands in shell history and in the process list of every user on
the machine. Credentials come from the Docker credential store, which already
exists on any machine that can pull an image.

## What a published artifact records about itself

A digest says two artifacts are the same bytes. It does not say which commit
they came from, which pipeline ran, or which Ptah wrote them — the questions
asked when a deployment is being explained rather than performed. Every push
records what the run knows:

| Annotation | Source |
| --- | --- |
| `org.opencontainers.image.source` | `PTAH_OCI_SOURCE`, else the GitHub server and repository |
| `org.opencontainers.image.revision` | `PTAH_OCI_REVISION`, else `GITHUB_SHA` |
| `io.stokaro.ptah.build-run` | `PTAH_OCI_BUILD_RUN`, else `GITHUB_RUN_ID` |
| `io.stokaro.ptah.version` | The publishing Ptah's own version |

Read them back with `ptah oci inspect`. A value the command was given always
wins over one inferred from the environment: the caller knows what it is
publishing, and the environment only knows what ran.

## Choosing how referrers are published

Two mechanisms make an attachment discoverable and they do not have the same
reach. The referrers index defined by the distribution specification is what
every conformant client reads. Ptah's content-derived durable tag is readable
by Ptah and by anyone who knows the naming rule, which in practice means Ptah.

`PTAH_OCI_REFERRER_POLICY` decides which one an attachment gets:

| Value | What it does |
| --- | --- |
| `auto` (default) | Asks the registry and uses the index where it exists, the durable tag where it does not |
| `api` | Uses the index and does not write the durable tag; a registry without the index fails the publish |
| `required-api` | Asks first, so a registry without the index fails **before** anything is written |
| `tag` | Writes the durable tag alone, for a registry whose index is present but wrong |

It is an environment variable rather than a flag because the decision belongs to
the pipeline rather than to one command. Every verb that attaches something —
`migrations lint --attach`, `migrations plan --attach`, the deployment report
after `migrations up` — has to make the same choice, and a flag that only some
of them carry is a guarantee with holes in it. Export it once:

```bash
export PTAH_OCI_REFERRER_POLICY=required-api
ptah migrations lint --dir "$MIGRATIONS" --attach
```

`required-api` is the setting an audit trail needs, and the difference between
it and `api` is when the refusal happens. Both demand the index; only
`required-api` asks before the artifact exists, so a pipeline that must not
publish an undiscoverable attachment never publishes one and has nothing to
clean up.

The index policies deliberately do **not** also write the durable tag. A tag
written beside an index the operator demanded would make a failed guarantee
read as a satisfied one on the next listing, which is the failure this policy
exists to prevent.

## Asking the registry what it supports

```bash
ptah oci capabilities "oci://ghcr.io/acme/app-migrations:latest"
```

Ptah's own discovery is robust whatever the registry does, because it publishes
referrers two ways and merges them on read. Cross-client discovery is the part
that depends on the registry, and this is how to find out rather than assume:
the question is put with the client pinned to the referrers API, so a success
cannot have come from the tag-schema fallback. A refusal naming the API as
unsupported is the registry saying no; a failure to ask is reported as an error
rather than folded into a no.

## OCI Reference Syntax

Ptah accepts these forms:

```text
oci://registry.example/team/repository
oci://registry.example/team/repository:tag
oci://registry.example/team/repository@sha256:<64-lowercase-hex-characters>
oci://registry.example/team/repository:tag@sha256:<64-lowercase-hex-characters>
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

A reference may carry a tag and a digest together, which is what a promotion
pipeline emits — "the artifact we call `:release`, resolved to these bytes":

```text
oci://ghcr.io/acme/app-migrations:release@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
```

The digest decides which bytes are fetched, exactly as with every other OCI
client when the two disagree. The tag is carried for readability and is echoed
back in the canonical form; it never selects content and never softens the pin.
Because the reference is digest-pinned, pushing to it is rejected exactly as
pushing to a bare `@sha256:` reference.

The pin is enforced, not merely recorded. Ptah asks the registry for the
manifest by digest and verifies the bytes it receives against that digest
before anything is read out of the artifact, so a repointed tag cannot smuggle
other content in behind a reference that names one. A digest the registry does
not serve fails; it never falls back to the tag written beside it.

`migrations up`, `migrations status`, `migrations down`, `migrations pull`, and
`lint` accept every form above. `migrations hash` does not accept any `oci://`
reference: it writes the integrity file back into the directory it hashed, and
a registry artifact is immutable content addressed by its own digest. Hash the
local directory and publish the result.

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
possible. Use the returned `Digest:` value for a hard pin. Pushing to any
reference carrying an `@sha256:` digest is rejected, including the
`:tag@sha256:` form. If a later tag update fails after earlier tags moved, Ptah reports
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
Docker Hub, or a production registry. The flag is available on every command
that resolves an `oci://` source, including lint, plan, `schema inspect`, and
`oci referrers`. That is not maintained by hand: a test walks the built command
tree and requires each command whose `--schema-file` reaches the OCI loader to
register `--plain-http`, then drives it against a registry, because a flag that
parses and is dropped would satisfy a registration check while leaving the
command exactly as unreachable as before.

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
`up --verify-sum` gate verifies the pulled files against the sum that traveled
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
