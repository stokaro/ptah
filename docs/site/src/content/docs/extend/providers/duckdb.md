---
title: Configure the DuckDB provider
description: Install, select and verify the DuckDB executable provider, diagnose an absent or unusable installation, and find where the provider documents what DuckDB does in Ptah.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I install, configure and verify the DuckDB executable provider?"
goal: "Install the DuckDB provider and verify that Ptah selects it."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/providerinstall
  - internal/cli/providers
  - stokaro/ptah#5667
generated: false
overlaps: []
disposition: keep
---

Ptah serves DuckDB only through this optional provider, which embeds DuckDB in
its own executable: Ptah itself carries no DuckDB code and no cgo. The provider
reads a database file or an in-memory database, compares and plans, applies
changes in a transaction, keeps the migration history, and provisions dev
databases. Use this page to install it, to select a reviewed build with a
manifest, and to correct a selection that reports a missing or unusable
installation. What DuckDB does in Ptah is the provider's to document; see
[Where the provider documents DuckDB](#where-the-provider-documents-duckdb).

## Check availability

The official provider ID is `github.com/stokaro/ptah-duckdb-plugin`. Its designated repository
is [stokaro/ptah-duckdb-plugin](https://github.com/stokaro/ptah-duckdb-plugin).
The catalog recognizes `duckdb` as engine names.

The provider is optional and installed separately from Ptah. Install the newest
release this Ptah can run with `ptah providers install duckdb`, or a chosen one
with `--version`. The
[install procedure](../../executable-providers/#install-an-official-provider)
covers how a release is chosen, the checks, and where the file is stored. The
first release needs provider contract 25.30, which states that the host starts
a provider in its own working directory, so a relative `duckdb://` path resolves
against the directory you run Ptah in. Remaining work is tracked in
[issue #5667](https://github.com/stokaro/ptah/issues/5667).

List the catalog without executing providers:

```console
ptah providers list --json
```

The DuckDB entry reports its provider ID, repository and setup link. An empty
`installations` array means this invocation selected no manifest.

## Declare a trusted installation

When you have a compatible DuckDB artifact, verify its publisher and checksum
before authorizing it. Create an explicit manifest using the
[executable-provider procedure](../../executable-providers/#select-the-executable).
Set `provider_id` to `github.com/stokaro/ptah-duckdb-plugin` and declare the canonical engine
`duckdb`. Copy the artifact's actual aliases into `engines[].aliases`.



## Verify the installation

Check the copy that `ptah providers install duckdb` stored. It needs no manifest:

```console
ptah providers check --provider-engine duckdb --json
```

It checks the executable against the SHA-256 in the install record, starts
it, and reports `"status": "compatible"`. With nothing installed it reports
`provider_missing`.

To check a manifest entry instead, given the reviewed manifest at
`providers.json`, run:

```console
ptah providers check --provider-config providers.json \
  --provider-engine duckdb --json
```

A successful check reports `"status": "compatible"`. Read the
[verification contract](../../executable-providers/#check-the-installation)
before treating that result as database capability evidence.

## Resolve a refusal

With no configured installation, an explicit selection reports
`provider_missing`, engine ID `duckdb`, and this setup page. A disabled entry
reports `provider_disabled`. Missing executable bytes, checksum mismatches,
startup failures, and incompatible protocols have distinct codes.

Use the [failure reference](../../executable-providers/#resolve-failures) to
correct the reported installation state, then repeat the check above.

## Use the selected provider

Pass `--provider-config providers.json` to a native command that names a
database or a dialect, and add `--provider-engine duckdb` when the command names
no `duckdb://` URL. A `duckdb://` URL or the `duckdb` dialect needs neither flag
once `ptah providers install duckdb` has installed a release. An absent or
failed installation cannot select an embedded implementation.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations. `ptah-compat` reads the
manifest from `PTAH_PROVIDER_CONFIG`, and the language server selects no
provider.

## Where the provider documents DuckDB

What DuckDB does in Ptah is the provider's to document: which statements a plan
renders, which changes DuckDB cannot make and the provider refuses, and what is
not supported yet. The pages below live in the provider's repository and are
published here with the release of the provider this documentation describes.
They start from these questions:

| Question | Provider page |
| --- | --- |
| How do a `duckdb://` address, one writer at a time and a dev database work? | [Using the DuckDB provider with Ptah](usage/) |
| Which declarations describe DuckDB objects? | [Declaring DuckDB objects](declarations/) |
| How is a DuckDB migration recorded and run? | [Migrations on DuckDB](migrations/) |
| What does the provider refuse, and why? | [What the DuckDB provider refuses](limitations/) |

The provider's [README](https://github.com/stokaro/ptah-duckdb-plugin#readme)
holds the rest of its reference.
