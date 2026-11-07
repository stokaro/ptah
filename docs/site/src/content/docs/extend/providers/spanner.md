---
title: Configure the Spanner provider
description: Install, select and verify the optional Spanner executable provider, diagnose an absent or unusable installation, and find where the provider documents what Spanner does in Ptah.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I install, configure and verify the Spanner executable provider?"
goal: "Install the Spanner provider and verify that Ptah selects it."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/providerinstall
  - internal/connector/pgconnect
  - internal/cli/providers
  - stokaro/ptah#4204
generated: false
searchAliases:
  - "Spanner"
  - "Cloud Spanner"
  - "Spanner support"
  - "PGAdapter"
overlaps: []
disposition: keep
---

Ptah serves Spanner's PostgreSQL interface only through this provider. The
`ptah`, `ptah-compat` and `ptah-ls` binaries hold no Spanner implementation:
the provider reads a live database, plans table changes, renders and runs the
statements, keeps the migration history and the migration lock, lints
migrations, writes declared rows and empties dev databases. The provider is
optional: no Ptah installation channel carries it. Use this page to install
it, to select a reviewed build with a manifest, and to correct a selection that
reports a missing or unusable installation. What Spanner does in Ptah is the
provider's to document; see
[Where the provider documents Spanner](#where-the-provider-documents-spanner).

## Support level

Support is best effort. The provider writes PostgreSQL-dialect statements
only, and renders no GoogleSQL. Its live tests run the Cloud Spanner emulator
behind PGAdapter, which is evidence about the PostgreSQL interface, not about
the managed service. No test runs against a managed instance, so review the
statements a plan writes before you apply them to a managed database.

## Before you start

The provider talks to PGAdapter, which speaks the PostgreSQL protocol in front
of a Spanner database. PGAdapter is a separate component, and so is the
database: installing the provider installs no PGAdapter, creates no Spanner
instance or database, and downloads nothing when it starts. Run PGAdapter
against your instance, or the emulator image for a local database, before a
command connects. [Using the provider with Ptah](usage/) shows both.

## Check availability

The official provider ID is `github.com/stokaro/ptah-spanner-plugin`. Its designated repository
is [stokaro/ptah-spanner-plugin](https://github.com/stokaro/ptah-spanner-plugin).
The catalog recognizes `spanner`, `cloudspanner`, `google-spanner`, and `google_spanner` as engine names.

Install the newest release this Ptah can run with
`ptah providers install spanner`, or a chosen one with `--version`. A command
that names a `spanner://`, `cloudspanner://` or `google-spanner://` URL or the
`spanner` dialect then selects the installed copy without a flag or a
manifest; a manifest entry, described below, overrides it. The
[install procedure](../../executable-providers/#install-an-official-provider)
covers how a release is chosen, the checks, and where the file is stored.
Remaining work on official providers is tracked in
[issue #4204](https://github.com/stokaro/ptah/issues/4204).

List the catalog without executing providers:

```console
ptah providers list --json
```

The Spanner entry reports its provider ID, repository and setup link. An empty
`installations` array means this invocation selected no manifest.

## Declare a trusted installation

When you have a compatible Spanner artifact, for example the executable that
`ptah providers install spanner` printed, verify its publisher and checksum
before authorizing it. Create an explicit manifest using the
[executable-provider procedure](../../executable-providers/#select-the-executable).
Set `provider_id` to `github.com/stokaro/ptah-spanner-plugin` and declare the canonical engine
`spanner`. Copy the artifact's actual aliases into `engines[].aliases`.

## Verify the installation

Given the reviewed manifest at `providers.json`, run:

```console
ptah providers check --provider-config providers.json \
  --provider-engine spanner --json
```

A successful check reports `"status": "compatible"`. Read the
[verification contract](../../executable-providers/#check-the-installation)
before treating that result as database capability evidence.

## Resolve a refusal

With no manifest entry and no installed provider, an explicit selection reports
`provider_missing`, engine ID `spanner`, the command
`ptah providers install spanner`, and this setup page, before it connects to a
database. An installed executable that does not match the digest its install
record holds is refused with `provider_untrusted`, and no other copy is tried.
An installed release whose minimum contract this Ptah does not meet is refused
with `provider_incompatible`; run `ptah providers install spanner` again. A
disabled entry reports `provider_disabled`. Missing executable bytes, checksum
mismatches, startup failures, and incompatible protocols have distinct codes.

A `postgres://` URL can reach PGAdapter too. When the server's version banner
names Spanner, the connection is refused with `provider_engine_identified`
before anything is read, and the pool is closed. The diagnostic carries the
engine ID, the provider ID, the repository and this setup page, and tells you
to connect with a `spanner://` URL, which selects the provider. Ptah never
inspects, plans or writes a server it identified as Spanner through the
PostgreSQL engine. A typed `--server-version` whose banner names Spanner is
refused the same way, as naming a server the dialect does not describe. The
emulator's banner reads `PostgreSQL 14.1` and names no product, so a
`postgres://` URL to the emulator cannot be identified: name the engine with a
`spanner://` URL.

Use the [failure reference](../../executable-providers/#resolve-failures) to
correct the reported installation state, then repeat the check above.

## Use the selected provider

A native command that names a Spanner URL or the `spanner` dialect selects the
installed provider. Pass `--provider-config providers.json` to select a manifest
entry instead. `--provider-engine spanner` asks for the executable provider
explicitly, and the command then refuses every URL scheme the provider does not
serve. An absent or failed installation is reported; nothing else serves Spanner
in its place, and a failed provider never falls back to PostgreSQL.

`ptah-compat` selects the installed provider in the same way, or the entry of
the manifest that `PTAH_PROVIDER_CONFIG` names; see
[Use a provider from ptah-compat](../../executable-providers/#use-a-provider-from-ptah-compat).
The community Atlas binary has no Spanner driver, so Spanner on `ptah-compat`
is a Ptah extension. `PTAH_ATLAS_STRICT_COMPAT=1` refuses a Spanner URL in
the community binary's words, `sql/sqlclient: unknown driver "spanner"`.
The language server starts no provider: it completes the directives the
provider claims from a copy recorded from a provider build.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations.

## Where the provider documents Spanner

What Spanner does in Ptah is the provider's to document: which statements a
plan writes, which changes it plans in place and which it refuses, how a
migration runs, and what is not supported yet. The pages below live in the
provider's repository and are published here with the release of the provider
this documentation describes. They start from these questions:

| Question | Provider page |
| --- | --- |
| How do a `spanner://` URL, a dev database and `ptah-compat` reach Spanner? | [Using the Spanner provider with Ptah](usage/) |
| Which column types, keys, indexes, constraints, row deletion policies, views and sequences does a schema declare? | [Declaring Spanner objects](declarations/) |
| How does a migration run, record a failure, hold the lock and lint? | [Migrations on Spanner](migrations/) |
| Which objects and changes does the provider refuse, and where does the emulator differ? | [What the Spanner provider refuses](limitations/) |

The Go annotation, YAML and HCL attributes Spanner adds to a directive or key
every engine has, such as the row deletion policy's
`platform.spanner.row_deletion_column` and
`platform.spanner.row_deletion_interval`, are listed with that directive in
the [Go annotation reference](../../../reference/go-annotations/), the
[YAML schema reference](../../../reference/yaml-schema/) and the
[HCL schema reference](../../../reference/hcl-schema/). Ptah's
[support matrix](../../../databases/support-matrix/) lists the engines Ptah
builds in; Spanner's release line and capabilities are the provider's to
declare, and its release publishes them in `release-lines.json`. The provider's
[README](https://github.com/stokaro/ptah-spanner-plugin#readme) holds the rest
of its reference.
