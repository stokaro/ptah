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
  - internal/providerinstall
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

The provider is installed separately from Ptah, on every installation channel.
Install the newest release this Ptah can run with
`ptah providers install ydb`, or a chosen one with `--version`. A command that
asks for the provider with `--provider-engine ydb` then selects the installed
copy without a manifest; a manifest entry, described below, overrides it.
Without `--provider-engine ydb`, a `ydb://` address is served by the engine
built into `ptah` in this documentation version.
The [install procedure](../../executable-providers/#install-an-official-provider)
covers how a release is chosen, the checks, and where the file is stored.
Remaining work on official providers is tracked in
[issue #4204](https://github.com/stokaro/ptah/issues/4204).

The releases v0.1.0 and v0.1.1 publish no `ptah-provider.json`, so the command
cannot install them.

List the catalog without executing providers:

```console
ptah providers list --json
```

The YDB entry reports its provider ID, repository and setup link. An empty
`installations` array means this invocation selected no manifest.

## Declare a trusted installation

When you have a compatible YDB artifact, for example the executable that
`ptah providers install ydb` printed, verify its publisher and checksum
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

With no manifest entry and no installed provider, an explicit selection reports
`provider_missing`, engine ID `ydb`, the command `ptah providers install ydb`,
and this setup page. An installed executable that does not match the digest its
install record holds is refused with `provider_untrusted`, and no other copy is
tried. An installed release whose minimum contract this Ptah does not meet is
refused with `provider_incompatible`; run `ptah providers install ydb` again. A disabled entry
reports `provider_disabled`. Missing executable bytes, checksum mismatches,
startup failures, and incompatible protocols have distinct codes.

Use the [failure reference](../../executable-providers/#resolve-failures) to
correct the reported installation state, then repeat the check above.

## Use the selected provider

Pass `--provider-config providers.json --provider-engine ydb` to supported
native schema commands. This explicitly requires the executable provider;
an absent or failed installation cannot select an embedded implementation.
Without `--provider-config`, the flag selects the installed provider.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations. The catalog does not add
provider selection to Atlas-compatible commands or the language server.
