---
title: Configure the YDB provider
description: Diagnose an absent or unusable YDB executable provider and verify an explicitly trusted installation.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I configure and verify the YDB executable provider?"
goal: "Verify an explicitly trusted YDB provider installation."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/cli/providers
  - stokaro/ptah#4204
generated: false
overlaps: []
disposition: keep
---

Use this page when an explicit YDB provider selection reports a missing or
unusable installation. You need a trusted executable artifact, its checksum, and
a host that supports the artifact's protocol.

## Check availability

The official provider ID is `github.com/stokaro/ptah-ydb-plugin`. Its designated repository
is [stokaro/ptah-ydb-plugin](https://github.com/stokaro/ptah-ydb-plugin).
The catalog recognizes `ydb` and `ydbs` as engine names.

Official YDB executable artifacts remain under development in
[issue #4204](https://github.com/stokaro/ptah/issues/4204). The catalog reserves
this identity; it does not provide a released download.

The full distribution policy includes this provider. A standalone binary or
source build needs an explicitly installed provider. An incomplete bundle needs
the matching provider restored.

List the catalog without executing providers:

```console
ptah providers list --json
```

The YDB entry reports its provider ID, repository, setup link, and distribution
policy. An empty `installations` array means this invocation selected no manifest.

## Declare a trusted installation

When you have a compatible YDB artifact, verify its publisher and checksum
before authorizing it. Create an explicit manifest using the
[executable-provider procedure](../../executable-providers/#select-the-executable).
Set `provider_id` to `github.com/stokaro/ptah-ydb-plugin` and declare the canonical engine
`ydb`. Copy the artifact's actual aliases into `engines[].aliases`.



## Verify the installation

Given the reviewed manifest at `providers.json`, run:

```console
ptah providers check --provider-config providers.json \
  --provider-engine ydb --json
```

A successful check reports `"status": "compatible"`. Read the
[verification contract](../../executable-providers/#check-the-installation)
before treating that result as database capability evidence.

## Resolve a refusal

With no configured installation, an explicit selection reports
`provider_missing`, engine ID `ydb`, and this setup page. A disabled entry
reports `provider_disabled`. Missing executable bytes, checksum mismatches,
startup failures, and incompatible protocols have distinct codes.

Use the [failure reference](../../executable-providers/#resolve-failures) to
correct the reported installation state, then repeat the check above.

## Use the selected provider

Pass `--provider-config providers.json --provider-engine ydb` to supported
native schema commands. This explicitly requires the executable provider;
an absent or failed installation cannot select an embedded implementation.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations. The catalog does not add
provider selection to Atlas-compatible commands or the language server.
