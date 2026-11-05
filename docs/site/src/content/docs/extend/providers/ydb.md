---
title: Configure the YDB provider
description: Install, select and verify the YDB executable provider, diagnose an absent or unusable installation, and make a lock node strict.
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
overlaps: []
disposition: keep
---

Ptah serves YDB only through this provider. Use this page to install it, to
select a reviewed build with a manifest, and to correct a selection that reports
a missing or unusable installation.

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
[YDB](../../../databases/ydb/) lists where the provider documents what YDB does
in Ptah. Remaining work on official providers is tracked in
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

## Make a lock node strict

The provider takes the migration lock as a semaphore on the `ptah_locks`
coordination node at the database root. It creates a missing node with strict
read and attach consistency, and it refuses an existing node whose two settings
are not both strict. The refusal is a `NOT_ACQUIRED` lock receipt that holds
nothing, and its reason names the node and its two settings. The node is left
as it is.

YDB's CLI and YQL have no command for a coordination node, so the fix goes
through the coordination service. With
[ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk), alter the node in
place:

```go
err := driver.Coordination().AlterNode(ctx, path.Join(driver.Name(), "ptah_locks"), coordination.NodeConfig{
	ReadConsistencyMode:   coordination.ConsistencyModeStrict,
	AttachConsistencyMode: coordination.ConsistencyModeStrict,
})
```

`DropNode` on the same path removes the node instead, and the next lock creates
it strict. Drop it only while no run holds a lock there: dropping the node ends
every lock session on it. See
[Coordination locks](https://github.com/stokaro/ptah-ydb-plugin#coordination-locks)
in the provider's documentation.
