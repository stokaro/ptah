---
title: Configure the Spanner provider
description: Install, select and verify the Spanner executable provider, diagnose an absent or unusable installation, and find where the provider documents what Spanner does in Ptah.
type: how-to
audience:
  - database-engineer
readerQuestion: "How do I install, configure and verify the Spanner executable provider?"
goal: "Install the Spanner provider and verify that Ptah selects it."
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

The Spanner provider serves Spanner's PostgreSQL interface, through PGAdapter,
as a separate executable: it reads a live database, compares and plans, renders
and applies changes, keeps the migration history and the migration lock, lints
migrations and writes declared rows. Its support level is best effort, measured
against the emulator. Use this page to install it, to select a reviewed build
with a manifest, and to correct a selection that reports a missing or unusable
installation. What Spanner does in Ptah is the provider's to document; see
[Where the provider documents Spanner](#where-the-provider-documents-spanner).

## Check availability

The official provider ID is `github.com/stokaro/ptah-spanner-plugin`. Its designated repository
is [stokaro/ptah-spanner-plugin](https://github.com/stokaro/ptah-spanner-plugin).
The catalog recognizes `spanner`, `cloudspanner`, `google-spanner`, and `google_spanner` as engine names.

The provider is optional and installed separately from Ptah. Install the newest
release this Ptah can run with `ptah providers install spanner`, or a chosen one
with `--version`. The
[install procedure](../../executable-providers/#install-an-official-provider)
covers how a release is chosen, the checks, and where the file is stored.
Remaining work on official providers is tracked in
[issue #4204](https://github.com/stokaro/ptah/issues/4204).

A PostgreSQL URL does not select Spanner, and a failed provider never falls back
to PostgreSQL. While a Ptah release still builds in a Spanner engine of its own,
the provider page
[On a Ptah with a built-in Spanner engine](usage/#on-a-ptah-with-a-built-in-spanner-engine)
says which commands reach the provider and which reach the built-in engine.

List the catalog without executing providers:

```console
ptah providers list --json
```

The Spanner entry reports its provider ID, repository and setup link. An empty
`installations` array means this invocation selected no manifest.

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
saved plans, application, and its protocol limitations.

## Where the provider documents Spanner

What Spanner does in Ptah is the provider's to document: which statements a plan
renders, which changes it plans and which it refuses, and what is not supported
yet. The pages below live in the provider's repository and are published here
with the release of the provider this documentation describes. They start from
these questions:

| Question | Provider page |
| --- | --- |
| How do a `spanner://` URL, a dev database and `ptah-compat` reach Spanner? | [Using the Spanner provider with Ptah](usage/) |
| Which declarations describe Spanner objects? | [Declaring Spanner objects](declarations/) |
| How is a Spanner migration split, recorded and locked? | [Migrations on Spanner](migrations/) |
| What does the provider refuse, and why? | [What the Spanner provider refuses](limitations/) |

The provider's [README](https://github.com/stokaro/ptah-spanner-plugin#readme)
holds the rest of its reference.
