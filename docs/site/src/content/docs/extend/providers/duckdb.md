---
title: Configure the DuckDB provider
description: Diagnose an absent or unusable DuckDB executable provider and verify an explicitly trusted installation.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I configure and verify the DuckDB executable provider?"
goal: "Verify an explicitly trusted DuckDB provider installation."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/cli/providers
  - stokaro/ptah#4204
generated: false
overlaps: []
disposition: keep
---

Use this page when an explicit DuckDB provider selection reports a missing or
unusable installation. You need a trusted executable artifact, its checksum, and
a host that supports the artifact's protocol.

## Check availability

The official provider ID is `github.com/stokaro/ptah-duckdb-plugin`. Its designated repository
is [stokaro/ptah-duckdb-plugin](https://github.com/stokaro/ptah-duckdb-plugin).
The catalog recognizes `duckdb` as engine names.

Official DuckDB executable artifacts remain under development in
[issue #4204](https://github.com/stokaro/ptah/issues/4204). The catalog reserves
this identity. Its repository publishes no release with `ptah-provider.json`,
so `ptah providers install duckdb` refuses with `provider_no_release`.

This provider is optional. Installing the main Ptah binary does not establish
a DuckDB provider installation.

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

Given the reviewed manifest at `providers.json`, run:

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
