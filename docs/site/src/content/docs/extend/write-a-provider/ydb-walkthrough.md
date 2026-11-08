---
title: "Understanding the provider boundary: a YDB walkthrough"
description: One YDB example followed from database objects through Ptah's models, the provider contract and the responsibilities of Ptah Core and the YDB provider.
type: concept
audience:
  - "go-developer"
  - "integration-developer"
  - "database-engineer"
readerQuestion: "Which parts of a schema belong to the database, which to Ptah, which exist only at the provider boundary, and who reads, compares, plans and runs each part?"
goal: "Tell a database object, a Ptah model, a contract message and a component's responsibility apart, using one YDB example from declaration to execution."
sourceOfTruth:
  - "provider-sdk/protocol/experimental/provider.proto"
  - "provider-sdk/README.md"
  - "internal/providerhost"
  - "docs/adr/0122-a-comparison-reads-the-current-state-of-another-comparisons-models.md"
  - "stokaro/ptah#5807"
generated: false
searchAliases:
  - "provider boundary"
  - "YDB walkthrough"
  - "Core and provider responsibilities"
  - "YDB transfer"
overlaps:
  - /extend/write-a-provider/architecture/
  - /extend/write-a-provider/models-and-planning/
disposition: keep
---

You know relational databases and schema migrations, and you meet a YDB
object Ptah cannot model itself, such as a transfer. You want to know who
reads its definition, who decides whether it can be created, and who runs it
once it exists. Four YDB objects below follow that path from the database to
execution, and each step names who understands a piece of information and
who only carries it.

[How a provider works](../architecture/) describes the process boundary and
[Engines, models and planning](../models-and-planning/) the model paths. This
page puts one running example through both. Code and decision records are
linked as further reading; nothing here requires them.

## The layers

Every term below belongs to exactly one layer. Most confusion about providers
comes from treating two layers as one.

| Layer | What belongs here | Not to be confused with |
| --- | --- | --- |
| The database (YDB) | Tables, topics, consumers, changefeeds, transfers, and how the server runs them | Anything Ptah runs itself |
| Ptah Core (the host) | Shared schema concepts, comparison policy, routing, global plan order, saved plans, migration history policy, output | A pipe that only forwards calls |
| The YDB provider | YDB representations, reading the catalog, normalizing, comparing and planning its own models, rendering and running statements | A second migration tool with a planner of its own |
| The contract | Neutral messages, identities, model declarations, service descriptions, ownership rules and features | YDB semantics, or a running process |
| The SDK | Go APIs, generated bindings, validators and runtime support for the contract | The contract itself; a provider need not be written in Go |

Some words name one component from different sides. *Host* and *Ptah Core*
are the same program: the `ptah` binary. A *provider* is a separate
executable that serves one or more *engines*; the YDB provider serves the
engine `ydb`. *Plugin* is the name of the repository and the release
(`stokaro/ptah-ydb-plugin`); the program it ships is the provider.

## The YDB objects in the example

YDB, like other databases, has more than tables. The example uses five kinds
of object. The topic analogies below help, but YDB's semantics are its own.

| Object | What it is |
| --- | --- |
| Row table | Rows and columns with a primary key, as in any relational database |
| Topic | A persistent, partitioned stream of messages, comparable to a Kafka topic |
| Consumer | A named reading position on a topic, registered on the topic itself, not the process that reads |
| Changefeed | A stream of a table's row changes, which YDB writes to a topic it keeps under the table |
| Transfer | A background process YDB runs that reads a topic and writes rows to a target table through a YQL lambda |

Two flows combine in the example. A changefeed publishes the changes of a
table to its topic, and a transfer turns the messages of a topic into rows:

```text
orders (row table)
   │  row changes, written by YDB
   ▼
orders/feed (changefeed of orders, with its topic)
   │  messages, read by YDB's transfer process
   ▼
ingest_orders (transfer)
   │  rows, written by YDB
   ▼
orders_archive (row table)
```

This is a runtime data flow inside YDB. Ptah takes no part in it: once a plan
has created the transfer, YDB runs it after Ptah exits. Ptah's part is to
declare, compare, plan and apply the four objects.

## One example, four names

The table shows how each object reaches Ptah. A *kind* is a namespaced name
for one provider model; the YDB provider's kinds all start with
`github.com/stokaro/ptah-ydb-plugin/`, shortened to `…/` here.

| Name | In YDB | In Ptah | Compared by |
| --- | --- | --- | --- |
| `orders` | Row table | A common table, with its YDB settings in a `…/row-table-properties` facet | Core for the table; `ydb-row-table-properties` for the facet |
| `orders/feed` | Changefeed of `orders` | One entry in the `…/table-changefeeds` facet of `orders` | `ydb-row-table-properties` |
| `ingest_orders` | Transfer | A provider object of kind `…/transfer` | `ydb-transfers` |
| `orders_archive` | Row table | A common table | Core |

Later sections rely on what the table implies:

- A table is not YDB's concept or Ptah's: the physical table lives in YDB, and
  Ptah represents it in its common model, which every engine shares. YDB's own
  settings for it travel as a *facet*: a provider value attached to an object
  that Core models.
- The changefeed is not an object of its own in Ptah. The
  `…/table-changefeeds` facet holds the complete set of one table's
  changefeeds, so one facet value of `orders` can describe several streams.
  The topic YDB keeps for a changefeed is part of that entry, while a topic
  created on its own is a separate object of kind `…/topic`, compared by
  `ydb-topics`.
- The names in the last column are comparison services the provider
  declares. Each model has exactly one owner. Core asks the owner and nobody
  else to compare it.

The kinds and owners are declared in the provider's `schema` and comparison
packages, such as
[`internal/replicationcompare` (latest development source)](https://github.com/stokaro/ptah-ydb-plugin/blob/master/internal/replicationcompare/compare.go)
for transfers and
[`internal/tablecompare` (latest development source)](https://github.com/stokaro/ptah-ydb-plugin/blob/master/internal/tablecompare/compare.go)
for table facets.

## Kind, model, value, identity, envelope

Take `ingest_orders`: each word below names a different thing about it, and
a different component understands each.

| Word | For `ingest_orders` | Who understands it |
| --- | --- | --- |
| Kind | `…/transfer`, the name of the model | Both sides, as a name |
| Model | The provider's declaration of that kind: its representation, version and JSON definition | The provider writes it; Core checks it is well formed |
| Value | The transfer's settings: source topic, target table, lambda, connection | The provider |
| Identity | The transfer's kind, directory and name, which say which object a value is about | Core, for matching and routing |
| Envelope | The message that carries a value: kind, version, encoding, owner and the payload bytes | Core carries it; only the provider reads the payload |

So Core can match `ingest_orders` in the declaration with `ingest_orders` in
the database, route both to `ydb-transfers` and keep the comparison's answer
in a saved plan without ever decoding what a transfer is.

A model has a *representation*. `DESIRED` is what a declaration states,
`OBSERVED` what a read found, `CHANGE` a difference a comparison reports and
`OPERATION` a step a plan carries out. They are different shapes on purpose:
a read can find settings that no declaration can spell, so an observed value
cannot always be turned into a desired one. Coverage says what a read
established about each kind; [Engines, models and
planning](../models-and-planning/#what-a-read-promises) defines it.

## Who does what

| Stage | Ptah Core | YDB provider | SDK and contract checks | YDB |
| --- | --- | --- | --- | --- |
| Load the declaration | Reads YAML, Go, HCL or SQL into the common model | Decodes its own extensions, such as a transfer | Validates the decoded values against the declared models | — |
| Read the database | Asks for a read of the selected scope | Reads the catalog and builds observed values and coverage | Checks identities, representations and coverage records | Answers catalog calls |
| Compare | Compares common objects; routes provider models to their owners | Compares the models it owns | Checks the request and the answer against the declarations | — |
| Check dependencies | Checks common relations | Refuses a change whose YDB dependencies do not hold | Structure only, not YDB meaning | — |
| Plan | Validates every contribution and orders the whole plan | Proposes steps and the dependencies between them | Checks the shape of steps and effects | — |
| Render and run | Shows, saves and applies the plan; keeps migration history | Renders YQL and runs it; provides history storage and locks | Checks execution outcomes | Runs the statements |
| Report | Prints plans, warnings and refusals | Gives reasons in the contract's terms | Redacts and checks reasons | — |

Several lines in the table are easy to misread.

- Core has a shared source frontend. The provider interprets only its own
  extensions; it does not parse every format from scratch.
- Not every comparison runs in the provider. Core compares the common tables,
  and the provider compares what it owns, including YDB's table settings.
- The provider proposes YDB steps and says which steps depend on which. Core
  orders the combined plan; the provider runs no global planner.
- SDK validation proves a message is well formed. It does not prove that a
  transfer's source will exist after the plan.
- Migration history policy belongs to Core. YDB-specific storage and locks
  belong to the provider: [How a provider works](../architecture/) has the
  details.

The contract's messages are a communication boundary. They are not a third
model of the database, and no third service owns their meaning.

## The workflow in order

These are logical stages. The contract methods named are real; a stage may
use several calls, and a workflow such as `schema plan` stops before
execution.

1. **Declaration.** Core loads the source. The provider decodes its
   extensions with `DecodeSource` and checks values with `NormalizeValues`.
2. **Selection.** Core starts the provider and calls `Describe`; the two
   agree on features. `Open` starts a session for the target database.
3. **Reading.** `Inspect` reads the selected scope. A read can span several
   catalog calls; it is not an atomic snapshot of the whole database, and it
   covers only the selected scope.
4. **Comparison.** Core compares common objects, then calls `CompareModels`
   once for each owner whose kinds are in play.
5. **Changes or refusal.** An owner returns changes, or refuses a change and
   says why.
6. **Planning.** `PlanFeatures` turns accepted changes into steps with
   dependencies. Core checks them and orders the whole plan.
7. **Review and execution.** Core prints or saves the plan. On apply,
   `Render` produces the statements and `Execute` runs them.
8. **Reinspection.** The next run reads again and finds nothing to change.

At every stage the questions are the same: who runs it, what does it
receive, what does it return, does it touch the database, and where does the
user see the result?

## When one comparison needs another's facts

`ydb-transfers` owns transfers, but whether `ingest_orders` can be created
depends on objects it does not own: its source `orders/feed` belongs to
`ydb-row-table-properties` and its target `orders_archive` to Core. A
comparison never reads another owner's objects by itself. It declares, in
`declared_kinds` and `declared_facet_kinds`, which other kinds it reads, and
Core fills in the request:

| Request field | The question it answers |
| --- | --- |
| `current` | What a read found of the models this comparison owns |
| `desired` | What the declaration states for those models |
| `declared_relations` | Which common relations are declared, such as the target table |
| `declared_objects` | Which objects of the declared other kinds the declaration names |
| `declared_facets` | The declared facets of those kinds, with their values and owners |
| `current_context` | What a read found of those kinds, with values and coverage |

Reading another owner's facts does not make the reader their owner, and it
does not allow a second comparison to report changes to them.

`current_context` exists because the declaration alone is not enough. A read
can record a changefeed it cannot fully describe, and the declaration then
does not name it. ADR 0122, a comparison reads the current state of another
comparison's models, records the decision. The evidence lives in the observed facet value as well
as in its coverage, because coverage names the facet's owner but does not list
the individual streams. [What a comparison may conclude](../ydb-incomplete-knowledge/)
works through that case and the other scenarios in which a fact is missing.

### The request in the walkthrough

The excerpt below is the request `ydb-transfers` received when `ptah schema
compare` ran the walkthrough's schema against an empty `shop` directory, with
Ptah v0.25.0 as the host. No command prints this request
(stokaro/ptah#5818); a build of the YDB provider at commit 24d982d5 that
wrote it to a file captured it.
It is reduced, and these simplifications are deliberate:

- An identity part such as `"schema": "shop"` is written as a plain string.
  The contract writes each part as `{"source": "shop", "normalized": "shop"}`.
- A `payload` is shown as the JSON it decodes to. The contract carries it as
  bytes, base64 in this JSON form.
- The value envelopes leave out the provider identifier, the model version,
  the encoding, the owner, the digests and the database context.
- The current context shows the coverage of topics only; the coverage of the
  tables' changefeeds has the same shape. A region shows only its directory,
  and the regions the read adds for tables and columns are left out.
- `targetFeatures`, `identifiers`, `parents`, `declaredColumns`, `exclusions`
  and `required` are left out.

```json title="CompareModelsRequest to ydb-transfers (excerpt)"
{
  "engine": "ydb",
  "comparisonId": "ydb-transfers",
  "databasePath": "/local",
  "capabilities": { "transfers": true },
  "kinds": ["github.com/stokaro/ptah-ydb-plugin/transfer"],
  "desired": {
    "records": [{
      "kind": "github.com/stokaro/ptah-ydb-plugin/transfer",
      "subject": { "schema": "shop", "name": "ingest_orders" },
      "value": {
        "value": {
          "payload": {
            "source": "shop/orders/feed",
            "target": "shop/orders_archive",
            "consumer": "archiver",
            "flushInterval": "PT1S",
            "lambda": "($msg) -> { return [<| offset: $msg._offset, message: CAST($msg._data AS Utf8) |>]; }"
          }
        },
        "requiredObjects": [
          { "kind": "github.com/stokaro/ptah-ydb-plugin/topic", "schema": "shop/orders", "name": "feed" }
        ]
      }
    }]
  },
  "current": { "records": [] },
  "declaredRelations": [
    { "kind": "table", "schema": "shop", "name": "orders" },
    { "kind": "table", "schema": "shop", "name": "orders_archive" }
  ],
  "declaredFacets": [{
    "kind": "github.com/stokaro/ptah-ydb-plugin/table-changefeeds",
    "subject": { "kind": "table", "schema": "shop", "name": "orders" },
    "value": {
      "value": {
        "payload": {
          "changefeeds": [{
            "name": "feed",
            "mode": "MODE_NEW_IMAGE",
            "format": "FORMAT_JSON",
            "topic": { "consumers": [{ "name": "archiver" }], "partitioningSettings": {} }
          }]
        }
      }
    }
  }],
  "currentContext": {
    "representation": "OBSERVED",
    "records": [],
    "coverage": {
      "kinds": [{
        "model": { "kind": "github.com/stokaro/ptah-ydb-plugin/topic", "representation": "OBSERVED" },
        "sources": [{
          "outside": {
            "state": "UNINSPECTED",
            "reason": "Knowledge is limited to returned subjects and fully listed directories"
          },
          "regions": [{ "scope": { "schema": "shop" }, "knowledge": { "state": "COMPLETE" } }]
        }]
      }]
    }
  }
}
```

Read in plain words, the request says this:

- `desired` holds the declared transfer as the provider normalized it.
  `requiredObjects` names what it reads: the topic `feed` under the table
  `shop/orders`, which is the changefeed's topic.
- `current` is empty: the read found no transfer in `shop`.
- `declaredRelations` lists both tables, so the target `orders_archive` is
  declared.
- `declaredFacets` carries the changefeed set that `orders` declares. It holds
  `feed`, so the source is declared too.
- `declaredObjects` is absent, because the schema declares no standalone
  topic.
- `currentContext` holds no records, because `shop` holds no topic or
  changefeed yet. Its coverage says the read covered `shop` completely and
  established nothing outside it.

From these facts `ydb-transfers` reports one new transfer, and the plan
creates it after the tables and the changefeed.

## Versions

Several version numbers describe one installation, and none implies another.

| Version | What it says |
| --- | --- |
| Ptah release | Which host behavior a user runs |
| Provider release | Which provider behavior a user runs, and which features it lists |
| SDK release | Which Go module a host or provider was built with |
| Contract major.minor | Which messages and features two peers can agree on |
| Model kind and version | Which payload shape a value has |
| YDB server version | Which objects and statements the server supports |

A contract feature reaches users only when a released host and a released
provider both list it. `models.current-context`, for example, is contract
25.40; a run uses it only where the host and the provider negotiate it.
`ptah providers check --provider-engine ydb --json` reports the provider's
version and features, and `ptah version` the host's.

## Further reading

- [Run the YDB walkthrough](../ydb-walkthrough-run/) to run this example
  against a local YDB server, step by step.
- [What a comparison may conclude](../ydb-incomplete-knowledge/) for the
  scenarios in which the read or the declaration lacks a fact.
- [How a provider works](../architecture/) and [Engines, models and
  planning](../models-and-planning/) for the rules this page illustrates.
- [Using the YDB provider](../../providers/ydb/usage/) and [Declaring YDB
  objects](../../providers/ydb/declarations/) for running the provider.
- The [SDK reference](../sdk-reference/) for every message and feature.
