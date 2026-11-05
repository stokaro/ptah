---
title: YDB
description: YDB in Ptah - the engine the official YDB provider serves, how ydb:// URLs, the ydb dialect name and docker://ydb dev URLs reach it, what to install, which commands reach YDB, and where the provider documents YQL rendering, planning, release lines and limitations.
type: reference
audience:
  - "database-engineer"
readerQuestion: "How does Ptah reach YDB, and where is YDB behavior documented?"
goal: "Find how Ptah serves YDB and where the provider documents its behavior."
sourceOfTruth:
  - "internal/providercatalog"
  - "internal/providerselect"
  - "github:stokaro/ptah-ydb-plugin"
generated: false
searchAliases:
  - "YDB support"
  - "YQL"
overlaps: []
disposition: keep
---

Ptah reaches YDB through the official executable provider
[stokaro/ptah-ydb-plugin](https://github.com/stokaro/ptah-ydb-plugin), provider
ID `github.com/stokaro/ptah-ydb-plugin`. The `ptah`, `ptah-compat` and `ptah-ls`
binaries hold no YDB implementation: the provider renders YQL, compares and
plans, reads a live database, applies changes, keeps the migration history and
the migration lock, lints YQL migrations, writes data and runs embedding
generations. Ptah keeps the shared pipeline: the source formats, the comparison
and ordering of a plan, the migration policy and the command surfaces.

The engine name is `ydb`, with the alias `ydbs`. Ptah recognizes it without the
provider: a `ydb://` or `ydbs://` URL, `--dialect ydb` and a `docker://ydb/...`
or `docker+ydb://...` dev URL all name it.

## Install the provider

Install the newest provider release whose minimum provider contract this Ptah
meets:

```console
ptah providers install ydb
```

The command checks the archive against the release's `checksums.txt` and keeps
the executable in your user store. A chosen release takes `--version`.
[Configure the YDB provider](../../extend/providers/ydb/) describes the checks,
an explicit manifest and the refusals; the
[install procedure](../../extend/executable-providers/#install-an-official-provider)
covers every official provider.

## How a command selects it

- A native command that names a YDB URL or `--dialect ydb` selects the installed
  provider. It needs no provider flag.
- `--provider-config <manifest>` selects the entry of an explicit trusted
  manifest, which overrides the user store.
- `--provider-engine ydb` asks for an executable provider explicitly; the
  command then refuses every URL scheme the provider does not serve.
- `ptah-compat` selects the installed provider in the same way, or the entry of
  the manifest `PTAH_PROVIDER_CONFIG` names. See [ptah-compat](#ptah-compat).

[Which provider serves an engine](../../extend/executable-providers/#which-provider-serves-an-engine)
gives the order in which candidates are tried.

## Without the provider

With no installation and no manifest entry, a command that names YDB stops
before it connects or provisions a dev database. It reports `provider_missing` with the engine ID `ydb`, the provider ID, the
command `ptah providers install ydb` and the
[setup page](../../extend/providers/ydb/). An installed executable whose digest
differs from its install record, a release whose minimum contract this Ptah does
not meet, and a disabled manifest entry are refused with their own codes; see
[Resolve a refusal](../../extend/providers/ydb/#resolve-a-refusal).

## Connecting

`ydb://` connects over plaintext gRPC and `ydbs://` over TLS:

```text
ydb://localhost:2136/local
ydbs://ydb.example.com:2135/?database=/ru-central1/b1g/etn
```

The provider owns the grammar of the address: the database path, the credential
parameters, the balancer settings and the `monitoring` endpoint it reads the
cluster's feature flags from. See
[Connection addresses](https://github.com/stokaro/ptah-ydb-plugin#connection-addresses).

## Dev, shadow and scratch databases

SQL cannot create a YDB database, so a `--dev-url`, `--shadow-db` or scratch
database that names a YDB server the run does not own is a dev realm: a
directory the provider creates for the run in the database the URL names, and
removes when the run ends. `docker://ydb[/<tag>[/local]]` asks the provider to
start a local-ydb container for the run. See
[Dev realms](https://github.com/stokaro/ptah-ydb-plugin#dev-realms) and
[Dev databases](https://github.com/stokaro/ptah-ydb-plugin#dev-databases).

## ptah-compat

No Atlas edition has a YDB driver, so YDB on `ptah-compat` is a Ptah extension.
The default profile takes a `ydb://` or `ydbs://` URL on the `schema`, `migrate`
and `script` verbs and starts the provider when a connection, a dev database or
a planning runtime first needs it. [Use a provider from
ptah-compat](../../extend/executable-providers/#use-a-provider-from-ptah-compat)
describes the session.

`PTAH_ATLAS_STRICT_COMPAT=1` reproduces the community binary, which answers
`sql/sqlclient: unknown driver "ydb". See: https://atlasgo.io/url` for a
`ydb://` URL, with `ydbs` for a `ydbs://` URL. Strict mode refuses a YDB URL in
those words wherever it comes from, and refuses `PTAH_PROVIDER_CONFIG` by name.
See [Compatibility differences](../../atlas/retained-divergences/#a-ydb-database-url).

## Where the provider documents YDB

The provider's documentation is the reference for what YDB does in Ptah: which
YQL a plan renders, which changes it plans in place and which it refuses, the
capabilities each release line and each feature flag gives, and what is not
supported yet.

| Topic | Provider documentation |
| --- | --- |
| Releases, platforms and the minimum contract | [Releases](https://github.com/stokaro/ptah-ydb-plugin#releases) |
| Connection addresses and feature flags | [Connection addresses](https://github.com/stokaro/ptah-ydb-plugin#connection-addresses), [Feature flags](https://github.com/stokaro/ptah-ydb-plugin#feature-flags) |
| Dev realms, dev databases and their cleanup | [Dev realms](https://github.com/stokaro/ptah-ydb-plugin#dev-realms), [Dev databases](https://github.com/stokaro/ptah-ydb-plugin#dev-databases) |
| Execution, transactions and migration bodies | [Execution and transactions](https://github.com/stokaro/ptah-ydb-plugin#execution-and-transactions), [Migration splits](https://github.com/stokaro/ptah-ydb-plugin#migration-splits) |
| Migration history and the migration lock | [Migration history](https://github.com/stokaro/ptah-ydb-plugin#migration-history), [Coordination locks](https://github.com/stokaro/ptah-ydb-plugin#coordination-locks) |
| Views, comments, users, groups and permissions | [Common objects](https://github.com/stokaro/ptah-ydb-plugin#common-objects) |
| Row tables, keys, types and Serial columns | [Native row-table creation](https://github.com/stokaro/ptah-ydb-plugin#native-row-table-creation), [Serial columns](https://github.com/stokaro/ptah-ydb-plugin#serial-columns) |
| Column families, TTL, partitioning, read replicas and key bloom filter | [Native table alteration](https://github.com/stokaro/ptah-ydb-plugin#native-table-alteration) |
| Indexes, index partitioning, vector and full-text indexes | [Index alteration](https://github.com/stokaro/ptah-ydb-plugin#index-alteration), [Vector and full-text indexes](https://github.com/stokaro/ptah-ydb-plugin#vector-and-full-text-indexes) |
| Table rebuilds | [Native table rebuild](https://github.com/stokaro/ptah-ydb-plugin#native-table-rebuild) |
| Column tables | [Column tables](https://github.com/stokaro/ptah-ydb-plugin#column-tables) |
| Changefeeds | [Changefeeds](https://github.com/stokaro/ptah-ydb-plugin#changefeeds) |
| Topics | [Topics](https://github.com/stokaro/ptah-ydb-plugin#topics) |
| Coordination nodes | [Offline coordination declarations](https://github.com/stokaro/ptah-ydb-plugin#offline-coordination-declarations), [Native coordination planning and execution](https://github.com/stokaro/ptah-ydb-plugin#native-coordination-planning-and-execution) |
| Resource pools, classifiers and streaming queries | [Resource pools and classifiers](https://github.com/stokaro/ptah-ydb-plugin#resource-pools-and-classifiers), [Streaming queries](https://github.com/stokaro/ptah-ydb-plugin#streaming-queries) |
| Async replications and transfers | [Async replications and transfers](https://github.com/stokaro/ptah-ydb-plugin#async-replications-and-transfers) |
| Secrets | [Secrets](https://github.com/stokaro/ptah-ydb-plugin#secrets) |
| External data sources and external tables | [External data sources and external tables](https://github.com/stokaro/ptah-ydb-plugin#external-data-sources-and-external-tables) |
| Desired YQL schema files | [SQL schema reading](https://github.com/stokaro/ptah-ydb-plugin#sql-schema-reading) |
| Migration lint and `ptah sql lint` | [Migration lint](https://github.com/stokaro/ptah-ydb-plugin#migration-lint), [Statement facts for `ptah sql lint`](https://github.com/stokaro/ptah-ydb-plugin#statement-facts-for-ptah-sql-lint) |
| Seeds, declared rows and the query builder | [Rows, row writes and seeds](https://github.com/stokaro/ptah-ydb-plugin#rows-row-writes-and-seeds) |
| Embedding generations | [Embedding store](https://github.com/stokaro/ptah-ydb-plugin#embedding-store) |
| What the provider does not cover yet | [Extraction still required](https://github.com/stokaro/ptah-ydb-plugin#extraction-still-required) |

The Go annotation, YAML and HCL attributes that declare YDB objects are listed
in the [Go annotation reference](../../reference/go-annotations/), the
[YAML schema reference](../../reference/yaml-schema/) and the
[HCL schema reference](../../reference/hcl-schema/). The provider claims them,
and Ptah reads them only when a command selects the provider.

Ptah's [support matrix](../support-matrix/) and
[capability reference](../../reference/capabilities/) describe the engines
Ptah builds in. YDB's release lines and capabilities are the provider's to
declare; its release publishes them in `release-lines.json`.

## Next steps

- Install and verify the provider: [Configure the YDB provider](../../extend/providers/ydb/).
- How Ptah selects, starts and checks any executable provider: [Test an executable provider](../../extend/executable-providers/).
- URL forms for every engine: [Database URLs and dev databases](../../concepts/database-urls-and-dev-databases/).
