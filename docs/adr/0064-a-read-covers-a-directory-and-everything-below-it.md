# ADR 0064: A read covers a directory and everything below it

Status: Proposed

Contract: 25.21

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0054](0054-a-filtered-read-carries-the-planning-context-it-does-not-compare.md)

## Context

A read reports what it knows about each native model as coverage: knowledge
outside every region, regions of a namespace, and single subjects. A region
names a database, a catalog, a schema and a path prefix. The path prefix
matches whole components, so a region covers every path below its prefix. The
schema matches one exact value.

The YDB provider spells a directory as the schema, `a/b/c`, one component. A
read with no `--schemas` walks every directory below the root, except
dot-prefixed directories and the dev realm directory `ptah_dev`, and claims
each directory it listed complete. No region covers a directory that does not
exist. A native object declared in a new directory therefore has an unknown
current state, and the comparison leaves it unplanned. A claim for the whole
database, a region with no schema, would also cover the directories the walk
leaves out and everything below them, which the read does not know.

Measured on local YDB 26.2.1.14 with a host built from Ptah master ba83f5966
and a YDB provider built from its open reverse-plan branches; neither changes
a read. The desired schema declares a table `seed` in `d` and a secret `pw` in
`d/fresh/nested`, where neither directory below `d` exists.

| Read | The provider path | The built-in YDB engine |
| --- | --- | --- |
| `migrations generate`, no `--schemas`, the target | `Warning: github.com/stokaro/ptah-ydb-plugin/secret d/fresh/nested.pw could not be compared between the database and the desired schema: The current secret or its absence was not established.` The up file creates only the table. | ``CREATE SECRET `d/fresh/nested/pw` WITH (value = $PTAH_SECRET_G60);`` |
| the same in a dev realm of the target (`dev_realm=<name>`) | the same warning | `CREATE SECRET` in the up file, `DROP SECRET` in the down file |

The provider's documentation records the same for `schema apply` with no
`--schemas` and a topic in `fresh/nested`: the warning, and exit 0.

A selection is outside this record. `--schemas d` selects `d` alone on the
provider, and the provider reports the same warning for the secret in
`d/fresh/nested`; the built-in engine plans the secret. Whether a selection
covers the directories below it is a separate decision.

## Decision

### A region may cover a schema and every schema below it

`schema.subtree-coverage` is a two-way feature: the provider and each engine
that sends such regions list it, and the host lists it in its host features.
Contract 25.21 adds three fields:

```proto
message IdentifierRules {
  // ...
  // The character that separates nested schemas in a normalized schema
  // spelling, or empty when schemas do not nest. Contract 25.21.
  string schema_separator = 5;
}
message ModelNamespace {
  // ...
  // With a schema, the region also covers every schema nested below it.
  // Contract 25.21, under schema.subtree-coverage.
  bool schema_subtree = 5;
}
message ComparisonNamespace {
  // ...
  bool schema_subtree = 9;
}
```

A region with `schema_subtree` covers a subject whose normalized schema is the
region's, or begins with the region's followed by the separator. The empty
schema, the database root, covers every schema of the database. The match
compares normalized spellings and never splits a component: it holds only
where the engine declares a separator that no component contains.

Regions combine as they do today. Unknown knowledge in a matching region
overrides a complete one, and a subject's own coverage overrides both. A read
that claims a subtree complete and leaves out a directory below it sends an
unknown subtree region for that directory.

### What the SDK enforces

Both transport boundaries refuse a region with `schema_subtree` unless the
feature is negotiated, unless the region names a schema, and unless the
engine's identifier rules declare a separator of one character. A filtered
read sends no subtree region: its regions must fit inside the namespaces its
selectors bind, and a binding names one exact schema. Absence and requested
defaults stay refused for every region, as before.

The SDK exports the match as one function. The host's coverage lookup and a
provider's comparison both call it, so the two cannot disagree about which
subjects a region covers.

### What the host does with a subtree region

`schemaext.NamespaceScope` carries the flag and the separator, and its match
follows the rule above. The adapter builds the native scope and the common
table and column scopes of a region with the flag, and spells an encoding
request's regions back through the same conversion. Nothing else changes:
the comparison reads knowledge from coverage as it does today.

### What the YDB provider sends

The provider lists the feature and declares `/` as its schema separator. A
read with no selector sends, for each native model, one complete subtree
region at the database root and one unknown subtree region for each directory
the walk leaves out: each dot-prefixed directory, at any depth, and the dev
realm directory. A filtered read sends the exact regions it sends today.

A native object declared in a new directory is then absent, and the plan
creates it. One declared below a directory the walk leaves out stays unknown,
and the comparison leaves it unplanned with the warning it prints today.

## Alternatives considered

**A region with no schema.** The provider could claim the whole database
without a contract change. The claim would cover the dot-prefixed directories
and the dev realm directory, which the read does not list. The provider's own
documentation already gives this reason. Rejected.

**The same region, and a refusal of declarations below the left-out
directories.** The comparison would never act on the false part of the claim.
Every other reader of the coverage would, and the provider would refuse
declarations the built-in engine accepts. Rejected.

**Directories as path components.** A YDB directory could be spelled as the
path of an identity, and `path_prefix` already covers a subtree. Every YDB
identity, every document and every schema-qualified name the host prints would
change, for one coverage gap. Rejected.

**The read proves absence for the paths a declaration names.** The host would
send the declared directories to an unfiltered read, and the provider would
prove each one empty from its parent's listing, as it does for a selected
directory today. A read would then depend on a desired schema that `db read`
and an export do not have, and the contract would need a field in the
inspection request instead of one in the region. Rejected.

## Consequences

- Contract 25.21 adds two namespace fields, one identifier rule and a two-way
  feature. It is numbered after contract 25.20, which ADR 0063 proposes for
  the tables a read leaves out.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Without the feature nothing changes: the provider sends exact regions, and
  a native object in a new directory stays unplanned with a warning.
- With it, an unfiltered read through the YDB provider plans a native object
  in a new directory, as the built-in engine does.
- A provider whose engine does not nest schemas declares no separator and
  cannot send a subtree region.

## Acceptance evidence

- SDK tests accept a negotiated subtree region and refuse one from an engine
  that does not list the feature, to a host that does not list it, without a
  schema, without a separator, and in a filtered read. Matching tests: the
  root subtree covers every schema; the subtree of `a` covers `a` and `a/b`
  and not `ab`; an unknown subtree overrides a complete one above it.
- Host tests: the scope match and its JSON form, and the adapter's conversion
  read back unchanged through an encoding request. Through the SDK fixture
  provider, an unfiltered read with a root subtree region plans a native object
  in a new namespace, and one below an unknown subtree stays unplanned.
- Through the YDB provider on 25.1.4.7 and 26.2.1.14: with no `--schemas`, on
  the target and in a dev realm, `migrations generate` writes `CREATE SECRET`
  for the secret in `fresh/nested` and `DROP SECRET` in the down file, as the
  built-in engine does, and `schema apply` creates a topic there; a secret
  declared below a dot-prefixed directory stays unplanned with the warning.
