---
title: Configure the Spanner provider
description: Diagnose an absent or unusable Spanner executable provider and verify an explicitly trusted installation.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I configure and verify the Spanner executable provider?"
goal: "Verify an explicitly trusted Spanner provider installation."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/cli/providers
  - stokaro/ptah#4204
generated: false
overlaps: []
disposition: keep
---

Use this page when an explicit Spanner provider selection reports a missing or
unusable installation. You need a trusted executable artifact, its checksum, and
a host that supports the artifact's protocol.

## Check availability

The official provider ID is `github.com/stokaro/ptah-spanner-plugin`. Its designated repository
is [stokaro/ptah-spanner-plugin](https://github.com/stokaro/ptah-spanner-plugin).
The catalog recognizes `spanner`, `cloudspanner`, `google-spanner`, and `google_spanner` as engine names.

Official Spanner executable artifacts remain under development in
[issue #4204](https://github.com/stokaro/ptah/issues/4204). The catalog reserves
this identity; it does not provide a released download.

This provider is optional. Select it explicitly for Spanner work. A PostgreSQL
URL does not select Spanner, and a failed provider never falls back to PostgreSQL.

List the catalog without executing providers:

```console
ptah providers list --json
```

The Spanner entry reports its provider ID, repository, setup link, and distribution
policy. An empty `installations` array means this invocation selected no manifest.

## Declare a trusted installation

When you have a compatible Spanner artifact, verify its publisher and checksum
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

With no configured installation, an explicit selection reports
`provider_missing`, engine ID `spanner`, and this setup page. A disabled entry
reports `provider_disabled`. Missing executable bytes, checksum mismatches,
startup failures, and incompatible protocols have distinct codes.

Use the [failure reference](../../executable-providers/#resolve-failures) to
correct the reported installation state, then repeat the check above.

## Use the selected provider

Pass `--provider-config providers.json --provider-engine spanner` to supported
native schema commands. This explicitly requires the executable provider;
an absent or failed installation cannot select an embedded implementation.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations. The catalog does not add
provider selection to Atlas-compatible commands or the language server.
