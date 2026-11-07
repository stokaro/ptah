---
title: Configure the YDB provider
description: Install, select and verify the YDB executable provider, diagnose an absent or unusable installation, and find where the provider documents what YDB does in Ptah.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I install, configure and verify the YDB executable provider?"
goal: "Install the YDB provider and verify that Ptah selects it."
sourceOfTruth:
  - internal/providercatalog
  - internal/providerconfig
  - internal/providerinstall
  - internal/cli/providers
  - stokaro/ptah#4204
generated: false
searchAliases:
  - "YDB"
  - "YDB support"
  - "YQL"
overlaps: []
disposition: keep
---

Ptah serves YDB only through this provider. The `ptah`, `ptah-compat` and
`ptah-ls` binaries hold no YDB implementation: the provider renders YQL,
compares and plans, reads a live database, applies changes, keeps the migration
history and the migration lock, lints YQL migrations, writes data and runs
embedding generations. Use this page to install it, to select a reviewed build
with a manifest, and to correct a selection that reports a missing or unusable
installation. What YDB does in Ptah is the provider's to document; see
[Where the provider documents YDB](#where-the-provider-documents-ydb).

## Check availability

The official provider ID is `github.com/stokaro/ptah-ydb-plugin`. Its designated repository
is [stokaro/ptah-ydb-plugin](https://github.com/stokaro/ptah-ydb-plugin).
The catalog recognizes `ydb` and `ydbs` as engine names.

The provider is the only implementation of YDB that Ptah runs, and it is
installed separately from Ptah, on every installation channel. Install the
newest release this Ptah can run with `ptah providers install ydb`, or a chosen
one with `--version`. A command that names a `ydb://` or `ydbs://` URL or the
`ydb` dialect then selects the installed copy without a flag or a manifest; a
manifest entry, described below, overrides it.
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

Check the copy that `ptah providers install ydb` stored. It needs no manifest:

```console
ptah providers check --provider-engine ydb --json
```

It checks the executable against the SHA-256 in the install record, starts
it, and reports `"status": "compatible"`. With nothing installed it reports
`provider_missing`.

To check a manifest entry instead, given the reviewed manifest at
`providers.json`, run:

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

A native command that names a YDB URL or the `ydb` dialect selects the
installed provider. Pass `--provider-config providers.json` to select a manifest
entry instead. `--provider-engine ydb` asks for the executable provider
explicitly, and the command then refuses every URL scheme the provider does not
serve. An absent or failed installation is reported; nothing else serves YDB in
its place.

`ptah-compat` selects the installed provider in the same way, or the entry of
the manifest that `PTAH_PROVIDER_CONFIG` names; see
[Use a provider from ptah-compat](../../executable-providers/#use-a-provider-from-ptah-compat).
`PTAH_ATLAS_STRICT_COMPAT=1` refuses a YDB URL in the community binary's words.
The language server starts no provider: it completes the directives the
provider claims from a copy recorded from a provider build.

The [native provider workflow](../../executable-providers/) describes inspection,
saved plans, application, and its protocol limitations.

## Where the provider documents YDB

What YDB does in Ptah is the provider's to document: which YQL a plan renders,
which changes it plans in place and which it refuses, the capabilities each
release line and feature flag gives, and what is not supported yet. The pages
below live in the provider's repository and are published here with the release
of the provider this documentation describes. They start from these questions:

| Question | Provider page |
| --- | --- |
| How do a `ydb://` URL, a dev database and `ptah-compat` reach YDB? | [Using the provider with Ptah](usage/) |
| Which directives and YAML keys declare YDB objects? | [Declaring YDB objects](declarations/) |
| How is a YDB migration split, locked and timed out? | [Migrations on YDB](migrations/) |
| How is an embedding generation indexed on YDB? | [Embedding generations on YDB](inference/) |

The Go annotation, YAML and HCL attributes YDB adds to a directive or key every
engine has are listed with that directive in the
[Go annotation reference](../../../reference/go-annotations/), the
[YAML schema reference](../../../reference/yaml-schema/) and the
[HCL schema reference](../../../reference/hcl-schema/). Ptah's
[support matrix](../../../databases/support-matrix/) lists the engines Ptah
builds in; YDB's release lines and capabilities are the provider's to declare,
and its release publishes them in `release-lines.json`. The provider's
[README](https://github.com/stokaro/ptah-ydb-plugin#readme) holds the rest of
its reference.
