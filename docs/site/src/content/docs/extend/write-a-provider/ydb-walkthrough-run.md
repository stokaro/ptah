---
title: Run the YDB walkthrough
description: Declare a YDB table, its changefeed, a transfer and the transfer's target table, then compare, apply and compare again through the real YDB provider, with each step's owner named.
type: how-to
sourceMode: static-file-only
audience:
  - "go-developer"
  - "integration-developer"
  - "database-engineer"
readerQuestion: "How do I watch one YDB declaration go through Ptah and the YDB provider, from comparison to a plan with nothing left to do?"
goal: "Run the walkthrough example against a local YDB server and see which component does each step."
sourceOfTruth:
  - "internal/cli/compare"
  - "internal/cli/schema"
  - "stokaro/ptah#5807"
generated: false
searchAliases:
  - "YDB transfer example"
  - "YDB changefeed example"
overlaps:
  - /extend/write-a-provider/ydb-walkthrough/
disposition: keep
---

[Understanding the provider boundary](../ydb-walkthrough/) explains who reads,
compares, plans and runs each part of a YDB schema. Here you run the same
example: `orders`, its changefeed `orders/feed`, the transfer `ingest_orders`
and the table `orders_archive`. Each step says which component does the work.

The steps below ran against these versions; later releases may print more or
different detail.

| Component | Version |
| --- | --- |
| Ptah | v0.24.1 |
| YDB provider (`stokaro/ptah-ydb-plugin`) | v0.7.0 |
| YDB server | `ydbplatform/local-ydb:26.2.1.14` |

## Before you start

You need Docker and the `ptah` binary;
[Install Ptah](../../../start/install/) covers the binary. The example creates
its objects in a new directory `shop` of a throwaway server, and the last step
removes the server.

## Start a YDB server

```console
docker run -d --name ptah-walkthrough-ydb --hostname localhost \
  -e YDB_USE_IN_MEMORY_PDISKS=true -e YDB_GRPC_SERVICES=replication \
  -p 2136:2136 -p 8765:8765 ydbplatform/local-ydb:26.2.1.14
```

`YDB_GRPC_SERVICES=replication` adds the replication API, which local-ydb
leaves out by default and through which the provider reads a transfer. Wait
until `docker ps` reports the container as healthy.

## Install the provider

```console
ptah providers install ydb --version 0.7.0
```

Ptah downloads the release, checks the archive against the release's
`checksums.txt` and records the executable's digest. The provider runs only
when a command needs it, as a separate process; see [Using the YDB
provider](../../providers/ydb/usage/) for the URL forms and selection rules.

## Declare the objects

Save the declaration as `schema.yaml`:

```yaml
tables:
  orders:
    schema: shop
    columns:
      id: {type: Int64, primary: true, not_null: true}
      note: {type: Utf8}
    changefeeds:
      feed:
        mode: NEW_IMAGE
        format: JSON
        consumers:
          archiver: {}
  orders_archive:
    schema: shop
    columns:
      offset: {type: Uint64, primary: true, not_null: true}
      message: {type: Utf8}
transfers:
  ingest_orders:
    schema: shop
    source: shop/orders/feed
    target: shop/orders_archive
    consumer: archiver
    flush_interval: PT1S
    using: "($msg) -> { return [<| offset: $msg._offset, message: CAST($msg._data AS Utf8) |>]; }"
```

The tables are Ptah's common model. The changefeed and the transfer are YDB
declarations the provider decodes: [Declaring YDB
objects](../../providers/ydb/declarations/) lists their keys.

## Compare

```console
ptah schema compare --db-url ydb://localhost:2136/local \
  --schema-file schema.yaml --schemas shop
```

```text
Differences detected (2 categories):
  feature_changes (1): github.com/stokaro/ptah-ydb-plugin/transfer shop.ingest_orders
  tables_added (2): shop.orders, shop.orders_archive
```

Several components took part before that line was printed:

- Ptah Core loaded the YAML and sent the changefeed and the transfer to the
  provider to decode.
- The provider read the `shop` directory, which does not exist yet.
- Core compared the two tables itself.
- Core sent the transfer to its owner, the provider's `ydb-transfers`
  comparison, together with the declared changefeed it reads and the declared
  target table. The comparison answered with one change.

The changefeed is part of the `orders` table's facet, so it arrives with the
table addition rather than as a change of its own.

Ptah v0.24.1 prints the transfer's line with an internal address in place of
its name; later releases print the name, as above. The report then shows the
statements a plan would run, under "Reconciling SQL", without line breaks
between the provider's statements (stokaro/ptah#5812).

## Review and apply

```console
ptah schema apply --db-url ydb://localhost:2136/local \
  --schema-file schema.yaml --schemas shop --dry-run
```

```text
Planned schema changes:
CREATE TABLE `shop/orders` (
    `id` Int64 NOT NULL,
    `note` Utf8,
    PRIMARY KEY (`id`)
);
ALTER TABLE `shop/orders` ADD CHANGEFEED `feed` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON');
ALTER TOPIC `shop/orders/feed` ADD CONSUMER `archiver`;
CREATE TABLE `shop/orders_archive` (
    `offset` Uint64 NOT NULL,
    `message` Utf8,
    PRIMARY KEY (`offset`)
);
CREATE TRANSFER `shop/ingest_orders` FROM `shop/orders/feed` TO `shop/orders_archive` USING ($msg) -> { return [<| offset: $msg._offset, message: CAST($msg._data AS Utf8) |>]; } WITH (CONSUMER = 'archiver', FLUSH_INTERVAL = Interval('PT1S'));
```

The provider proposed the steps for the changefeed, its consumer and the
transfer, and named the objects the transfer needs, the changefeed among
them. Core put every step in order, and the provider rendered each as YQL;
`--dry-run` stops after printing them. Without it the same command asks for
approval; `--auto-approve` applies straight away:

```console
ptah schema apply --db-url ydb://localhost:2136/local \
  --schema-file schema.yaml --schemas shop --auto-approve
```

```text
Schema apply completed successfully.
```

On apply, the provider ran each statement in that order, and YDB created the
objects.

## Watch YDB run the transfer

Ptah has exited. Write a row to `orders` and read `orders_archive` a few
seconds later:

```console
docker exec ptah-walkthrough-ydb /ydb -e grpc://localhost:2136 -d /local \
  yql -s "UPSERT INTO \`shop/orders\` (id, note) VALUES (1, 'first order'u);"
docker exec ptah-walkthrough-ydb /ydb -e grpc://localhost:2136 -d /local \
  yql -s "SELECT offset, message FROM \`shop/orders_archive\`;"
```

```text
┌────────┬───────────────────────────────────────────────────────────────────────┐
│ offset │ message                                                               │
├────────┼───────────────────────────────────────────────────────────────────────┤
│ 0      │ "{\"update\":{},\"newImage\":{\"note\":\"first order\"},\"key\":[1]}" │
└────────┴───────────────────────────────────────────────────────────────────────┘
```

YDB wrote the change to the changefeed's topic, and its transfer process turned
the message into a row. This is the runtime data flow; neither Ptah nor the
provider took part in it.

## Compare again

```console
ptah schema compare --db-url ydb://localhost:2136/local \
  --schema-file schema.yaml --schemas shop
```

```text
No schema differences detected.
```

The provider read the directory again, and every comparison found the
database as declared. An empty result here means the comparisons ran and
agreed. It is not the same as a comparison that was skipped or refused,
which reports its reason.

## Clean up

```console
docker rm -f ptah-walkthrough-ydb
```

The server kept its data in memory, so removing the container removes every
object the example created. `ptah providers install` stored the provider
under your user cache directory, where later runs reuse it.

## What next

- [Understanding the provider boundary](../ydb-walkthrough/) for the layers,
  models and owners behind each step.
- [Using the YDB provider](../../providers/ydb/usage/) for connection URLs,
  dev databases and `ptah-compat`.
