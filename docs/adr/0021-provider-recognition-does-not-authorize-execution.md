# ADR 0021: Provider recognition does not authorize execution

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

The experimental native provider workflow starts independently built executables.
Its initial manifest identifies executable bytes and the expected provider, but
engine ownership is available only after startup. Listing installations or
selecting one engine would otherwise require executing every configured entry.
An unknown URL also cannot distinguish a missing official provider from an
unrecognized engine.

The independent fixture proves inspection, planning, saved-plan application,
and process cleanup. It does not establish YDB, DuckDB, or Spanner object support.
The stable protocol and official extraction remain incomplete.

## Decision

Keep official recognition in a data-only application package. Catalog records
contain names, aliases, URL schemes, provider identity, repository and setup URLs,
and distribution policy. They contain no parser, capability, renderer, or database
implementation. A catalog entry grants no execution permission.

Require explicit engine declarations in experimental manifest version 2. Validate
all declarations before selection, including disabled and unselected entries.
Duplicate names are errors rather than precedence rules. Official names resolve
to their canonical engine and designated provider. Third-party names need no host
release. After startup, negotiated declarations must match the manifest exactly.

Select one installation and start only that process. The operator supplies the
manifest path explicitly. No working-directory, PATH, environment, or implicit
project-file discovery authorizes a binary. The absolute executable path and
SHA-256 digest select reviewed bytes; publisher verification remains an operator
responsibility. No command downloads, enables, replaces, or updates a provider.

Separate integrity listing from protocol negotiation. `providers list` reads and
hashes files without execution. `providers check` starts one selected executable,
checks its identity and protocol, and closes it without opening a database.
Machine-readable diagnostics distinguish absence, disabled installations, missing
files, integrity failures, startup failures, and protocol incompatibility.

An explicit native selection owns its connection scope. Unregistered schemes
are refused even when a bundled driver exists. This covers addresses loaded from
project configuration after provider selection. Ordinary commands without an
explicit selection retain their embedded implementations until extraction.

## Alternatives considered

Starting every configured executable would reveal engines without extending the
manifest. It makes listing execute code and lets an unused broken installation
prevent unrelated work. Explicit declarations avoid both effects at the cost of
checking the declaration against negotiation.

First-match precedence would allow overlapping engine names. It makes manifest
order change the executable that receives a database address. Refusing conflicts
makes selection independent of order and prevents official alias impersonation.

Automatic catalog downloads would reduce installation steps. It would also make
recognition an execution authorization and require a release provenance policy
that the experimental runtime does not implement. Installation remains explicit.

Keeping manifest version 1 would require eager discovery or a second reader with
different selection semantics. This changes behavior; pre-v1, so no compatibility
is owed. The experimental manifest uses only version 2.

## Consequences

Installation metadata can be inspected safely without starting a provider.
The catalog and structured diagnostics remain reusable outside the native CLI.
Transport compatibility remains separate from database capability and authorization.

Project metadata, saved artifacts, Atlas adapters, and language-server workflows
still need their applicable selection integration under #4204. The native
experimental surface requires explicit selection for metadata-only targets.
The SDK still needs engine-owned objects, facets, offline parsing, transactions,
and history before official extraction can satisfy the full issue.

The official repository identities are reserved by the catalog; they are not
proof of published release artifacts. Distribution policy is not an installed
file inventory. Stable protocol version 1 must wait for independent qualification
of YDB, DuckDB, and Spanner.
