# ADR 0023: Engine knowledge moves with the provider

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

Extracting YDB into `stokaro/ptah-ydb-plugin` has to leave the host with no YDB
knowledge. The SDK covers inspection, execution, history, locks, offline
comparison and planning, and offline validation. The host
still holds these areas of its YDB implementation, and none has an owner on the
provider side:

- YQL lint rules, 9 files and 2.5k lines in `dialect/ydb/ydblint`.
- Dev-database provisioning and cleanup: `internal/embedydb`,
  `internal/devdocker/ydbdocker`, `internal/devclean/ydbreplay`, about 4k lines.
- Release lines and capability probes: `internal/capabilityprobe`,
  `internal/builtindialects/ydb.go`, and the dialect under
  `engine/builtin/internal/dialects/ydb`, about 3.4k lines.
- Authentication. The host connects through the SDK's environment authenticator,
  which reads service-account keys and instance metadata, and refuses an
  environment that would connect anonymously without saying so. The provider
  accepts anonymous, URL user and password, and URL token.

A host that keeps any of these still knows which YDB images exist, which rules
apply to YQL, or which server versions are supported. Counting non-test Go files
whose path names YDB (`git ls-files`) gives 407 files and 66.9k lines in 65
host packages, against 88 files and 18.7k lines in the provider.

## Decision

Each area belongs to the provider. The host calls a service and renders what it
returns.

**Lint.** The SDK gets a lint service. A provider declares its rule catalog and
returns findings for a statement batch, with source positions. The host keeps
the shared pieces: output formats, SARIF, severity thresholds, and
`--fail-on`. A rule identifier stays unique across the catalog the host merges.
`internal/lintcatalog` keeps its convention check over the merged set.

**Dev database.** The SDK gets a provisioning service: create a dev database for
a target, report its address, and remove it. The provider owns images, ports,
health checks, and cleanup of what it created. The host passes the operator's
request (`docker://ydb/<release>`, `--dev-url`) and never names an image. A
provider that cannot provision answers with a typed refusal that the host
reports as an installation diagnostic.

**Release lines and probes.** The provider declares its release lines, support
levels, and probes in its manifest. The host reads the manifest to build the
capability matrix. `internal/capabilityprobe/cells.go` keeps the lines of the
engines that stay in process and gains an entry per installed provider, read
from the manifest, not from a literal. The rule that a line may claim a level
only if something runs against it applies to provider lines unchanged.

**Authentication.** The provider uses the SDK's environment authenticator and
keeps the host's refusal of a silent anonymous fallback: an environment that
would connect anonymously fails unless the URL asks for anonymous access. The
host passes the URL and the process environment through the session and reads no
YDB credential itself. A credential never appears in a diagnostic.

## Alternatives considered

**Lint stays in the host as shared SQL lint.** It is the cheaper path, and the
lexer and rule tables are already generic in shape. It leaves a YQL rule set in
the host, and every provider after YDB would ask for the same exception. The
service costs one SDK addition before v1.

**The host provisions dev databases from catalog data.** The catalog would carry
image names and ports. The host would then know YDB's runtime layout, and the
catalog stops being data-only recognition (ADR 0021).

**Probes and lines declared in the host.** The matrix stays in one file. The
host then lists YDB release lines it cannot run on its own, and a provider
release that adds a line needs a host release.

**Explicit authentication methods only.** It narrows the provider to what the URL
can carry. Operators who authenticate with service-account keys or instance
metadata lose a working setup.

## Consequences

The SDK grows two services, lint and provisioning, and a manifest section for
release lines, before the v1 freeze. The conformance suite has to cover them.

The coverage of the host's own YQL lint corpus becomes a provider test. The
fixtures move with the rules.

The capability matrix reads installed providers, so a standalone host without
the YDB provider shows no YDB cells. The missing-provider diagnostic from
ADR 0021 covers that case.

Environment authentication makes the provider's behavior depend on the process
environment. The session records which method was selected, without the
credential, so a failure is diagnosable.
