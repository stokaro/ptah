---
title: Atlas schema commands
description: Inspect, diff, apply, plan, format, clean, and test schemas with the Atlas-style ptah-compat schema verbs.
---

You want Atlas-style declarative schema work — inspect a live database, diff
schema files, apply or plan desired-schema changes — through Ptah. This page
covers the `ptah-compat schema` verbs with runnable examples. Every invocation
on this page uses the separate `ptah-compat` drop-in binary; the install steps
plus the flag translation rules are on the
[Atlas compatibility overview](../overview/).

## Command behavior

| Atlas-compatible command | Ptah behavior |
| --- | --- |
| `ptah-compat schema inspect` | Inspects a live database, a local schema file, a migration directory, or an `env://` reference (non-database sources evaluated on the `--dev-url` dev database) and writes Atlas-shaped HCL, SQL, JSON, or custom-template output, including split/write file exports. |
| `ptah-compat schema apply` | Diffs a desired-state source (schema files, a database URL, a migration directory, or an `env://` reference) against a live database and applies the planned SQL after confirmation. |
| `ptah-compat schema plan` | Saves the declarative plan as a fingerprinted local plan file for a later `schema apply --plan`. |
| `ptah-compat schema diff` | Diffs two desired-state sources (schema files, database URLs, migration directories, or `env://` references) and prints migration SQL. |
| `ptah-compat schema fmt` | Formats local `.hcl` files using HCL canonical layout. |
| `ptah-compat schema clean` | Plans and applies destructive cleanup of user-owned schema objects. |
| `ptah-compat schema test [paths]` | Forwards to `ptah schema test` with Ptah-native YAML test cases. |
| `ptah-compat schema push` | Atlas CE boundary stub; the native `ptah schema push` to any OCI registry is the open replacement. |

Per-verb status detail — Atlas differences, waivers, and the inputs that fail
explicitly — is on [Atlas-compatible commands](../../reference/atlas-commands/).

## Inspect a schema source

`ptah-compat schema inspect` accepts a `--url` inspection source and writes
machine-oriented schema output without native Ptah status banners. The default
format is Atlas-compatible HCL. The source is a live database URL, a local
schema file (`.hcl`, `.yaml`, `.yml`, or `.sql`), a migration directory (a
directory containing `atlas.sum`), or an `env://` reference into the evaluated
`atlas.hcl` environment.

```bash
ptah-compat schema inspect --url "$DATABASE_URL" > schema.hcl
ptah-compat schema inspect --url "$DATABASE_URL" --format '{{ sql . }}' > schema.sql
ptah-compat schema inspect --url "$DATABASE_URL" --format '{{ json . }}' > schema.json
```

### Inspect output formats

The Atlas-compatible `--format` value is a Go-template body. The helper call,
not the helper's bare name, renders HCL, SQL, or JSON:

| Request | Standard output |
| --- | --- |
| no `--format` | rendered HCL |
| `--format '{{ hcl . }}'` | rendered HCL |
| `--format '{{ sql . }}'` | rendered SQL |
| `--format '{{ json . }}'` | rendered JSON |
| `--format hcl` | literal `hcl` (3 bytes, no line feed) |
| `--format sql` | literal `sql` (3 bytes, no line feed) |
| `--format json` | literal `json` (4 bytes, no line feed) |
| `--format ' sql '` | literal ` sql ` (hex `20 73 71 6c 20`, no line feed) |
| `--format ' json '` | literal ` json ` (hex `20 6a 73 6f 6e 20`, no line feed) |
| `--format ' hcl '` | literal ` hcl ` (hex `20 68 63 6c 20`, no line feed) |

The literal rows match Atlas CE v1.3.0 for empty and populated SQLite databases.
Native `ptah schema inspect --format hcl|sql|json` keeps its rendered
shorthands.

### HCL document framing

Single-document HCL output carries no Ptah generated-code marker. A nonempty
HCL document ends with exactly one line feed, whether you use the default
format, the `hcl` or `.MarshalHCL` template path, or
`--output`. An empty SQLite database therefore produces these exact visible
lines, followed by one line feed:

```hcl
schema "main" {
}
```

This is a framing match, not a claim that every populated schema is
byte-identical to Atlas. The modeled-block differences and retained
divergences described below still apply. Native `ptah schema inspect` keeps
Ptah's generated-code marker and its existing terminal blank line.

:::caution[A dev database executes SQL for real]
Whatever you pass as `--dev-url` runs the SQL Ptah is evaluating, so point it
at a disposable database. This matters most when the SQL came from a plan file
someone else wrote — see
[The replay is not a sandbox](#the-replay-is-not-a-sandbox).
:::

Non-database sources require `--dev-url`, mirroring Atlas dev-database
normalization: the dev database is reset destructively, the source is
materialized on it (schema files executed, migration directories replayed),
and the result is introspected. Inspecting a file without `--dev-url` fails
with Atlas's `--dev-url cannot be empty` message.

One kind of object is not materialized: a role the dev database's **server**
already has. The reset empties the dev database, and a role is not in it —
PostgreSQL roles belong to the server — so `CREATE ROLE` for one of them fails
at SQLSTATE 42710 no matter how clean the database is. Ptah leaves such a role
exactly as the server has it, never altering it, and names the skipped roles on
standard error. A role the server does not have is still created, so the same
document still materializes on a server that has never seen it. This is what
lets an inspected description be fed straight back in against a clean sibling
database on the same server.

```bash
ptah-compat schema inspect \
  --url file://schema.sql \
  --dev-url "$DEV_DATABASE_URL" > schema.hcl
```

### What a URL puts under inspection

A PostgreSQL-family URL that pins no `search_path` describes the whole
database — every non-system schema, with its tables — and one that pins a
schema describes exactly that schema. An empty database still describes the
schema it has rather than rendering an empty document, so a consumer walking
`.schemas` does not break on a database that is merely empty.

```bash
# Every schema of the database, `public` and `extra` alike.
ptah-compat schema inspect --url "postgres://…/app?sslmode=disable"

# Only `public`.
ptah-compat schema inspect --url "postgres://…/app?sslmode=disable&search_path=public"
```

Every object in the document names the schema that owns it, not the one the
connection happens to be on. That applies to the non-table kinds as much as to
tables: an `enum` block carries `schema = schema.<name>`, a `function`, `view`,
`materialized`, `domain`, `composite`, `range` and `sequence` block each carry
the attribute wherever the object is outside the document's default schema, and
a column declared against a type in another schema is written against that
schema's type. Applying such a document therefore rebuilds each object where it
was, and applying it back to the database it describes plans nothing.

#### A schema-limited run refuses a multi-schema HCL desired state

An HCL desired state that declares more than one top-level `schema` block is
refused when the run is limited to a single schema, and the message names the
file and every block it counted:

```text
cannot use HCL with more than 1 schema when dev-url is limited to schema "main":
2 top-level schema blocks are declared: "main" at schema.hcl:1, "other" at schema.hcl:4
```

A run is limited when the URL that decides its scope names one schema: any
SQLite URL, a PostgreSQL-family URL carrying `search_path=<one name>`, or a
MySQL-family URL naming a database. `--dev-url` is checked first and the target
`--url` second, so the message names the flag that limited the run. A
PostgreSQL-family URL with no `search_path` limits nothing, and the same
document loads there with both schemas described.

The refusal covers `schema inspect`, `schema apply`, `schema diff`,
`schema plan validate` and `migrate diff`, because a desired state the run
cannot reach in full is a desired state no plan can honor. Without it the extra
schemas were dropped and the run reported success:
`schema diff --from one.hcl --to two-schemas.hcl` answered
`Schemas are synced, no changes to be made`, and `migrate diff` wrote a
migration file covering half the document.

The count is of BLOCKS, not of distinct names. A document that opens
`schema "main"` twice — which is what a directory of per-table HCL files looks
like when every file repeats the schema block — is two blocks, and is refused
on a SQLite dev database for that reason. Declare the schema once, or give the
run a realm-scoped URL.

`--schema` / `-s` narrows inspection when the underlying database reader supports
schema scoping, and outranks the URL's scope: naming a schema that does not
exist renders an empty document rather than falling back to the connection's
own schema. `--format`
accepts Atlas-style Go templates with `.MarshalHCL`, `hcl`, `sql`, `json`,
`base64url`, `mermaid`, `split`, and `write`. Split-write exports are
supported for HCL and SQL output with the documented Atlas split strategies:
per object (the default: one file per object under per-type directories, with
a `main.sql` `atlas:import` entry point for SQL), `"schema"` (one file per
schema), and `"type"` (one file per object type), plus an optional
file-extension argument:

```bash
ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split | write "schema" }}'

ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ sql . | split "type" | write "schema" }}'

ptah-compat schema inspect \
  --url "$DATABASE_URL" \
  --format '{{ hcl . | split "schema" ".pg.hcl" | write "schema" }}'
```

Rendering plans the output files first, and one writer applies the plan:
duplicate output paths, paths that escape the output directory, collisions
between a planned file and a planned directory, and destinations that already
exist as directories fail explicitly before anything is written. Unsupported
split modes and unsafe extension arguments fail at render time. The pinned
Atlas CE binary rejects the `split`, `write`, and `hcl` template functions as
non-community features, so these exports are an open Ptah extension that
follows the documented Atlas behavior.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors, and removes matching resources from HCL,
SQL, JSON, and custom-template output. Exporter blocks remain an explicit gap.

A type selector sits on the final pattern segment, and `[type=schema]` may sit
on the leading one: `*[type=schema].*[type=table]` names every table in every
schema, and `app[type=schema].*[type=table]` narrows that to one schema. A type
selector on any other segment is refused before a database is contacted.

Ptah applies that literal meaning to every schema source. The pinned community
binary applies it to live PostgreSQL inspection but accepts it without filtering
tables in a SQLite file diff. Ptah keeps one source-independent selector meaning
instead of reporting that the selector succeeded while leaving its tables in
the plan. See the measured [comparison](../comparison/#leading-schema-type-selector).

A **field selector** — the `.field` suffix behind a type selector — names a
field to subtract while the object it belongs to stays. Ptah honors:

- `[type=extension].version`
- `.comment` on `table`, `view` and `materialized_view`
- `.*`, which names every field the selected types support

Any other field is refused, by name, before a database is contacted. See
[the comparison](../comparison/#exclude-field-selectors) for why Ptah refuses
these rather than accepting and ignoring them the way the pinned community
binary does.

A pattern matches an object under either spelling: its bare name, or its name
qualified by the schema that owns it. Introspection reports an object in the
connection's own schema with no schema of its own, so the connection default
supplies the qualified spelling — `--exclude public.users` and
`--exclude users` remove the same table on a PostgreSQL database URL, and both
subtract it from every side of a comparison, so an excluded object is neither
created nor dropped.

That rule covers every top-level object kind: tables, views, materialized
views, extensions, enums, functions, sequences, domains, composite types and
range types. Each carries the schema that owns it, so `--exclude public.mood`,
`--exclude app.fn_app` and `--exclude public.positive_int` remove exactly the
object they name — including from the DROP list a `schema diff` or
`schema apply` plans.

Two kinds have no owning schema and so match by bare name only. Roles are
cluster-scoped. Schemas are the top of the tree themselves, and a selector
naming one removes it together with everything in it — see
[Excluding a schema](#excluding-a-schema).

The kinds an `--exclude` subtracts are the same kinds an `--include` selects.
Sequences, domains, composite types and range types used to be selectable by
`--include` and invisible to `--exclude`, so `--exclude positive_int` left the
domain in the plan and `DROP DOMAIN` ran on an object the selector was written
to protect.

### Excluding a schema

A one-part selector also names a **schema**, and a schema that matches takes
its contents with it:

```console
$ ptah-compat schema inspect --url "$PG_URL" -s public -s app --exclude app
schema "public" {
  comment = "standard public schema"
}
...
```

`schema "app"` and every object in it — tables, enums, sequences, views, the
grants that ride them — leave together. That is what the pinned Atlas community
binary v1.3.0 does for the same selector in the same scope: with `--exclude app`
it plans drops for `public` only, and removing the selector from that run adds
exactly one line, `DROP SCHEMA "app" CASCADE;`. `--exclude app[type=schema]`
names the same schema explicitly.

The two-part spelling keeps meaning what it always meant. `--exclude 'app.*'`
removes the objects in `app` and **keeps** the schema itself, because `app` is
not a match for the glob `app.*`. Use the one-part form to protect the schema,
the two-part form to protect only its contents.

Schemas were the last kind that was read and rendered but never offered to the
patterns, which made `--exclude app` fail in both directions at once: `schema
apply` refused it as a selector that matched nothing, and with the permissive
opt-in set the plan still dropped `app`'s tables and enums.

## An `--exclude` selector that matches nothing

A selector that names no object protects no object, and the output cannot say
whether that was the schema or the selector. Ptah always says which:

- `schema inspect` and `schema diff` keep their exit status and write
  `Warning: the --exclude selection matched no objects: "<selector>".` to
  stderr. Neither verb changes anything, and stdout stays byte-identical so the
  CI idiom "does this selection differ?" keeps working.
- `schema apply` refuses with exit 1. It is the verb that carries the plan out,
  and a selector written to keep an object out of the plan that named nothing
  leaves the plan free to change it.

A selector counts as matched when it names an object in **any** state the
command filtered. Naming an object that exists on only one side of a comparison
is ordinary — that is what a CREATE or a DROP looks like — so only a selector
empty on every side is reported.

A selector is only ever called empty by a filter that asked it. That is why
this diagnostic is tied to the coverage above: an object kind that is read but
never offered to the patterns would make every selector naming one of its
objects look empty, and the refusal would then be a hard failure asserting
something false. The same rule covers children of a parent another selector
already removed — `--exclude users --exclude users.id` names two objects that
both exist, so it is accepted even though the column leaves with its table.

This is a deliberate divergence. The pinned Atlas community binary v1.3.0 exits
0 and prints no diagnostic for `--exclude nosuchobject`; matching is the floor
rather than the ceiling, and a silent scope failure is the one answer Ptah
declines to reproduce.

Reusing one exclude list across environments where some of the objects are
absent is a real workflow, so the permissive behavior stays reachable on this
same surface:

```console
$ PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 ptah-compat schema apply --url "$PG_URL" \
    --to file://schema.hcl --exclude nosuchobject
Warning: the --exclude selection matched no objects: "nosuchobject".
```

It is an environment variable and not a flag because the conformance
`cli-surface` tier asserts that `ptah-compat` registers exactly the flags the
pinned binary registers.

It reads like every other boolean `PTAH_*` variable: unset keeps the refusal, a
valid boolean is honored, and anything else fails `schema apply` before it reads
the database — including on a run whose selectors all match, so a typo in a
shared environment file stops the next run rather than the next unmatched
selector. See
[Boolean environment variables](../../reference/configuration/#boolean-environment-variables).

Accepting both spellings is looser than the pinned Atlas community binary,
deliberately. That binary reads a pattern relative to the URL scope: on a
database URL only `public.users` matches, and on a schema-bound URL
(`?search_path=public`) only `users` does. Ptah honors both in both scopes.
The extra matches only ever remove more objects from a plan, so the looser
rule cannot turn a protected object into a dropped one.

That looseness applies to objects, not to their children. A pattern names at
most an object and one of its children, and Ptah always filters inside a single
schema, so the schema slot is filled by the connection and the pattern is read
relative to it: `users` names the table, `users.name` names its column, and
`users.users_name_idx` names its index. A third part has nowhere left to go.
Ptah refuses it with the community binary's own message, which quotes the
pattern with the schema already prefixed:

```console
$ ptah-compat schema inspect --url "$PG_URL" --exclude public.users.name
Error: too many parts in pattern: "public.public.users.name"
```

The refusal is a usage error rather than a no-op on purpose. A pattern that
deep is almost always an attempt to name a table, and matching it against
columns would answer a question that was not asked by removing a column from a
table the user was trying to keep whole. A pattern too deep for any scope, such
as `*.*.*.*`, is refused before a database is contacted, so its message carries
no schema prefix.

Parts are counted on the pattern as written, selector text and field suffix
included:

- `*[type=extension].version` and `*[type=table].comment` are accepted.
- `public.*[type=extension].version` and `public.*[type=table].comment` are
  refused — the same arithmetic the community binary applies on a schema-bound
  URL, where it answers
  `too many parts in pattern: "public.public.*[type=table].comment"`.
- `*[type=schema].*[type=table]` is accepted in every scope: a leading
  `[type=schema]` segment fills the schema slot itself, so the connection's
  schema is not counted a second time.

Counting the resource glob instead would accept the refused spellings, and since
Ptah applies one depth rule to every scope that would mean exiting 0 where the
community binary exits 1.

### How an inspected column type is written

A column read out of a database carries no record of how anyone spelled it, so
inspection decides. A type the Atlas HCL schema models is written bare and
lower case, and every other type is written as a `sql("...")` call carrying the
server's own spelling:

```hcl
column "price"   { type = numeric(10,2) }
column "prices"  { type = sql("numeric(10,2)[]") }
column "kind"    { type = sql("cube") }
```

The split is not cosmetic. The pinned Atlas community binary refuses a type it
does not model in every spelling except the call — `type = USER_DEFINED` comes
back as `Unknown column.type`, and `type = "numeric(10,2)[]"` as
`set field "type": unexpected type string` — so a bare or quoted unmodeled type
produces a document that binary cannot read at all. Which names are modeled is
measured against it per dialect by the Atlas CE Oracle job rather than copied
from a table, and the list errs short: wrapping a type that did not need it
round-trips, while leaving one bare that did need it does not.

**Arrays always take the call.** No array is one HCL type expression, whatever
its element type is, so `text[]`, `numeric(10,2)[]` and
`timestamp(3) with time zone[]` are all written as `sql(...)`. PostgreSQL drops
an array's declared dimensions itself — `varchar(100)[10][]` is stored and
reported as `character varying(100)[]` — so the inspected spelling is the
server's, not the author's.

**A domain is named, not resolved.** A column typed by a domain is written with
the domain's own name, including when the domain is built on a type Atlas does
not model:

```sql
CREATE DOMAIN point3d AS cube CHECK (cube_dim(VALUE) = 3);
CREATE TABLE scalars (c_point3d point3d NOT NULL);
```

inspects as `type = sql("point3d")`. Writing the base type there would apply
back as a bare `cube` column and take the domain's `CHECK` with it, so the
domain name is what survives the round trip (stokaro/ptah#1138).

### Blocks the compatibility surface leaves out by default

`ptah-compat schema inspect` omits three top-level HCL block types on
PostgreSQL — `extension`, `sequence`, and `policy` — **when nothing else in the
document names the object**. The pinned Atlas community binary refuses an entire
schema file that declares any one of them, answering
`postgres: extensions are not supported by this version` and the equivalent for
the other two. Output a drop-in replacement writes has to be output the tool it
replaces can read back, and one such block costs the whole document.

Nothing is dropped quietly. Every omitted object is reported on standard error,
one line each, alongside the other loss diagnostics inspection already writes,
and the message names the variable that brings it back:

```console
$ ptah-compat schema inspect --url "$PG_URL" > schema.hcl
warning: extensions.pgcrypto: omitted from Atlas-compatible schema inspect output: ... set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models
warning: sequences.order_seq: omitted from ...
warning: rls_policies.accounts_all: omitted from ...
```

The omission is scoped as narrowly as the measurement is. It applies to HCL
output on PostgreSQL only: `--format '{{ sql . }}'` still writes the extension,
the sequence, and the policy, because SQL output is read by a database rather
than by that binary. On SQLite the same three blocks are accepted, so nothing
is omitted there. Every other block Ptah renders — `role`, `function`, `view`,
`materialized`, `trigger`, `permission` — is kept, because that binary drops a
block type it does not model and reads the file anyway.

Native `ptah schema inspect` omits nothing on this account; see
[Inspect a database](../../direct/inspect/). Which block types that binary
refuses is re-measured by the Atlas CE Oracle job rather than frozen, so a
construct a later build starts modeling stops being withheld.

One other thing is left out of a PostgreSQL description, and it is not a
compatibility trade: a read defines only the roles the inspected schemas
actually use, on **both** binaries, because roles are cluster-wide and a
description of one database is not the place to list another tenant's roles.
That omission is reported on the same stream — `note: N roles Ptah manages on
this server are not described …` — and
`PTAH_POSTGRES_INSPECT_ALL_ROLES=1` describes every role Ptah manages again.
Comparison is unaffected either way. See
[PostgreSQL roles and grants](../../databases/postgresql/#roles-and-grants).

#### The document says what it does not describe

An omission is a presentation decision, and a presentation decision must not
become deletion intent. Before this was written down, inspecting a database and
applying the result back to that same database planned to remove what inspection
had left out:

```console
$ ptah-compat schema inspect --url "$PG_URL" > schema.hcl
$ ptah-compat schema apply --url "$PG_URL" --to file://schema.hcl --dev-url "$DEV_URL" --dry-run
DROP POLICY IF EXISTS "p" ON "guarded";
DROP SEQUENCE IF EXISTS "order_seq";
DROP EXTENSION IF EXISTS "pgcrypto";
```

The standard-error warnings could not prevent that: `schema apply` is a separate
process, and it reads the file rather than the terminal. So the document now
carries the fact itself, in its header:

```hcl
// ptah:not-described extension
// ptah:not-described policy
// ptah:not-described sequence
```

A comparator reading that document has three states for an object instead of
two: present, absent, and **not described**. Only the middle one is a removal.
The four commands that consume a desired-schema document — `schema diff`,
`schema apply`, `schema plan`, and `migrate diff` — each resolve that document
separately, and all four now report the database above as synced.

The lines are HCL comments, so they change nothing for any other reader: the
pinned Atlas community binary v1.3.0 reads a document carrying them at exit 0
and prints byte-identical output to the same document without them.

Split exports carry the header too, into **every** member. `write` puts the
members on a filesystem and whatever reads one of them next is handed that one
file by path, so a record living in a sibling would not be there when it
matters:

```console
$ ptah-compat schema inspect --url "$PG_URL" \
    --format '{{ hcl . | split "schema" | write "out" }}'
$ head -3 out/public.hcl
// ptah:not-described extension
// ptah:not-described policy
// ptah:not-described sequence
$ ptah-compat schema apply --url "$PG_URL" --to file://out/public.hcl --dev-url "$DEV_URL" --dry-run
Schema is synced, no changes to be made
```

All three split strategies do this — per object (the default), `split "schema"`,
and `split "type"` — because each of their members is an independently
consumable desired state. Loading several members together is fine: the loader
unions their records rather than intersecting them.

The claim is about the rule this surface applies, not about what one database
happened to contain, so an inspect of a database with no extension at all still
writes the `extension` line. A document that named only what it left out would
be asserting that the absence of every *other* extension is authoritative, which
is false the moment the document is applied somewhere else — and that assertion
is the destructive direction.

**Removing an object is still something you can ask for.** A schema file you
wrote yourself has no coverage header and is fully authoritative, so a
`DROP EXTENSION` it implies is planned. To mean the omission in a document
`ptah-compat` produced, delete the line that says otherwise:

```console
$ grep -v 'ptah:not-described' schema.hcl > desired.hcl
$ ptah-compat schema apply --url "$PG_URL" --to file://desired.hcl --dev-url "$DEV_URL" --dry-run
DROP POLICY IF EXISTS "p" ON "guarded";
DROP SEQUENCE IF EXISTS "order_seq";
DROP EXTENSION IF EXISTS "pgcrypto";
```

`PTAH_ATLAS_INSPECT_ALL_BLOCKS=1` writes no `ptah:not-described` directives,
because a document that omits nothing describes everything. Native
`ptah schema inspect` never writes coverage directives for the same reason;
its separate generated-code marker is unchanged. The same directive grammar
is read out of the leading comments of a `.sql` desired state, spelled
`-- ptah:not-described ...`; no Ptah surface writes one there, because only
the HCL rendering omits blocks.

A directive naming an object kind the build does not know is an error rather
than a line to skip past: a record nothing understands reads as no record at
all, and the absence it was protecting would become a removal.

##### When such a document is the *current* state

`schema diff --from file://schema.hcl` puts a document on the other side of the
comparison, where its header means something different: not "do not remove
these" but "this side never looked". That is a reason to distrust the conclusion
"the object is missing" — it is not a reason to discard what `--to` explicitly
asked for.

So the record withholds a creation only when the creation would need that
conclusion to be true. A statement Ptah renders with `IF NOT EXISTS` is correct
either way and is planned:

```console
$ ptah-compat schema diff --from file://schema.hcl --to file://desired.hcl --dev-url "$DEV_URL"
CREATE EXTENSION IF NOT EXISTS "citext";
```

A statement with no guard — `CREATE SEQUENCE` for a sequence declared without
`if_not_exists`, `CREATE ROLE`, `CREATE DOMAIN`, `CREATE TABLE` — would fail the
migration if the object were already there, so it is not planned. It is named on
standard error rather than dropped in silence:

```console
Warning: sequence "public.order_seq" is declared by --to but no change was
planned for it: --from records `ptah:not-described sequence`, so this comparison
cannot tell it apart from one that already exists, and the creation Ptah renders
for it has no IF NOT EXISTS guard.
```

Adding `if_not_exists = true` to the declaration is how you ask for it anyway.
Deleting the directive line from `--from` is how you assert that side really did
look.

#### A referenced block is kept, and the document says so

Suppression never leaves a reference behind. If anything else in the document
still depends on the object — a column default calling `nextval` on a sequence, a
view body selecting from it, a `permission` block targeting it, a column whose
type an extension supplies — the block stays, and the reason is reported:

```console
warning: sequences.order_seq: kept in Atlas-compatible schema inspect output because another object in this document depends on it: ...
```

Such a document **is not readable by the community binary**, and that is not a
defect Ptah can fix. Measured on PostgreSQL 17 for

```sql
CREATE SEQUENCE order_seq;
CREATE TABLE orders (id integer NOT NULL DEFAULT nextval('order_seq'::regclass));
```

that binary's own inspect emits `default = sql("nextval('order_seq'::regclass)")`
with no `sequence` block, and then refuses that output when it is fed back:
`pq: relation "order_seq" does not exist`. There is no faithful description of a
sequence-backed column default the community binary can read, including the one
it writes itself. Ptah keeps the sequence so the document stays true and stays
readable by Ptah; dropping the column's default instead would describe a
database that does not exist.

For an extension the question asked is **what it supplies**, not what it is
called, because the two are usually different words. The `isn` extension
supplies the type `isbn`; `pgcrypto` supplies the function `gen_salt`. Neither
name appears in a document that depends on it, so a test against the extension's
own label would omit the block and leave the column behind. Ptah reads the
member list from the catalog (`pg_depend` against `pg_extension`) during
inspection and keeps the extension when the document uses any of its types,
functions, relations, operator classes, or operator families.

Two kinds of member name are left out of that list, because neither one is
evidence of anything. A name `pg_catalog` also supplies keeps resolving with the
extension dropped: `citext` supplies fifteen of those, among them `max`,
`strpos` and `replace`, and `pgcrypto` supplies a `gen_random_uuid` that
PostgreSQL 13 and later supply from core. A function whose name is a SQL keyword
cannot be told from the statement that shares the word: `hstore` supplies three
functions named `delete`, and a `plpgsql` body doing `DELETE FROM audit` calls
none of them. Either name would pin the extension to a schema that has no
relationship to it, and the community binary refuses any file declaring an
extension block, so a keep that did not have to happen does not shrink the
compatibility win — it removes it.

Neither exclusion can cost a dependency the document really has, and the keyword
one carries the condition that makes that true. Reaching an extension's overload
instead of the core function takes arguments of that extension's own type, so a
genuine call to `delete` takes an `hstore` value and the type is named on a
column, in a function signature or in a cast. A keyword-named function is
therefore dropped only when the same extension also puts a type in this list
that appears in that function's signature — the entry that will keep the
extension in its place. An extension supplying `merge(text, text)` and no type
at all has its only evidence in that name, so the name stays and the block
with it.

Only function names are tested against the keyword list, which the server itself
supplies through `pg_get_keywords()`. A type, a relation and an operator class
are each named by nothing but themselves, so dropping a keyword-shaped one would
throw away the only evidence there is. The `cube` extension shows what that
leaves behind — its type and its constructor are both called `cube`, so a view
saying `GROUP BY CUBE(x)` still keeps it.

#### An extension the document may not name

Some dependencies have no name to match. PostgreSQL prints an operator class in
a `CREATE INDEX` only when that class is not the default for the key's type on
the index's access method, so with `btree_gin` installed

```sql
CREATE EXTENSION btree_gin;
CREATE TABLE t (id integer PRIMARY KEY, n integer NOT NULL);
CREATE INDEX t_gin ON t USING gin (n int4_ops);
```

is stored, and rendered back, as `CREATE INDEX t_gin ON public.t USING gin (n)`.
The document holds no token of `btree_gin` — not its label, and not one of the
support functions its member list holds, none of which anything ever writes. The
extension was omitted at exit 0 and the community binary then refused the
result: `create index "t_gin" to table: "t": pq: data type integer has no
default operator class for access method "gin"`. Ptah's own apply of the same
document failed the same way.

Ptah asks the catalog instead of the text. `pg_index.indclass` holds the
operator class each index key actually resolved to, so inspection resolves those
classes — and the index's access method — against `pg_depend` and records which
extension owns them. The same edge is read for an exclusion constraint, whose
elements print their operators and print an operator class under the same
not-the-default rule: `EXCLUDE USING gist (room WITH =, during WITH &&)` over an
`integer` column needs `btree_gist` and says nothing of it. The keep is reported
like any other, and says which question answered it:

```console
warning: extensions.btree_gin: kept in Atlas-compatible schema inspect output because the catalog resolved an index or constraint in this document to an operator class or access method it supplies: ...
```

The same rule read the other way says when the class **is** in the document: a
class is printed exactly when it is not the default, so `pg_trgm`'s
`gin_trgm_ops` on a `text` column comes back as
`CREATE INDEX w_trgm ON public.w USING gin (txt gin_trgm_ops)` and is rendered as
`ops = "gin_trgm_ops"`, or as `elements = "txt gist_trgm_ops WITH ="` on an
exclusion constraint. That extension is kept as well — omitted, the document
failed to apply with `operator class "gin_trgm_ops" does not exist for access
method "gin"` — but it is kept because the document names something it supplies,
and the report says so:

```console
warning: extensions.pg_trgm: kept in Atlas-compatible schema inspect output because another object in this document depends on it: ...
```

The name question is asked first for that reason. A keep you can find by
searching the document is never reported as one you cannot.

Reading `USING gin` as a reference to `btree_gin` would answer the same question
wrongly. `gin` is a core access method that belongs to no extension, and
`tsvector`, `jsonb` and array columns all have core GIN operator classes, so
that rule would pin the extension to indexes that do not need it and cost each
of those documents its compatibility. The catalog separates them exactly: a GIN
index over `jsonb` resolves to nothing an extension supplies, and its document
still comes out with no `extension` block.

Only an index or constraint the document actually carries counts. Inspection
drops one whose table it does not export, reports it, and writes nothing for
it — a materialized view's index is the ordinary case, because PostgreSQL
resolves its operator classes in `pg_index` like any other index while a
`materialized` block carries none:

```console
warning: index mv_gin: index cannot be rendered because the target table is absent from the exported schema
```

Keeping the extension for that index would spend the whole document on
something the file does not contain. The community binary refuses any
PostgreSQL file declaring an `extension` block, and the document applies with
or without it, so the block costs the compatibility and buys nothing.

An extension nothing depends on is still omitted, and the community binary
reads that document at exit 0. Set `PTAH_ATLAS_INSPECT_ALL_BLOCKS=1` when the
output has to carry every block regardless.

Two implicit resolutions are **not** covered, and both were measured rather than
assumed. An extension-supplied implicit cast leaves no catalog handle on the
object that uses it: across the 45 extensions in the `postgres:17` image, every
such cast has its own extension's type on one side (`citext`, `isbn` and its
siblings), so naming that type is what keeps the extension, and a cast between
two core types would be a gap. No extension in that image supplies a collation
at all. A primary key or a single-column unique constraint is backed by an index
too, but its operator class is the default for the column's own type: the
extension-supplied default classes on core types in that image are all `gin`,
`gist` or `bloom` classes and none is `btree`, so no such constraint can rest on
one without naming the type that carries it.

#### Get the full description back

```console
$ PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 ptah-compat schema inspect --url "$PG_URL"
```

Every block Ptah models is emitted and nothing is reported as omitted. The
result describes the database in full and the community binary refuses it, which
is the trade the variable exists to let you make. It is an environment variable
rather than a flag because `ptah-compat`'s flag surface is held to parity with
the pinned binary; see
[Compatibility never costs you a capability](../overview/#compatibility-never-costs-you-a-capability).

### Three rules a `permission` block is written by

Each of these is a rule about one position, measured against the pinned Atlas
community binary on the document `ptah-compat schema inspect` writes. They are
not a promise that every reference in every document resolves — a sequence block
is still refused whatever the reference naming it says, and
[the blocks left out by default](#blocks-the-compatibility-surface-leaves-out-by-default)
names the shapes that binary cannot read at all.

A schema is declared whenever anything in the document references one. That
includes a document with no tables at all: every PostgreSQL database carries
`GRANT USAGE ON SCHEMA public TO PUBLIC`, so inspecting an empty database
renders a `permission` block saying `for = schema.public` and the matching
`schema "public" {}` beside it. A document that references no schema declares
none.

A grantee is written as a `role.<name>` reference only where the same document
declares that `role` block, and as a quoted name otherwise. Grants are children
of the object granted on rather than of the grantee, so excluding roles keeps
every grant to them:

```console
$ ptah-compat schema inspect --url "$PG_URL" --exclude '*[type=role]'
permission {
  to = "app_user"
  for = table.users
  privileges = ["SELECT"]
}
```

The name is preserved either way — Ptah reads both spellings back to the same
grant, and applying that document issues `GRANT SELECT ON TABLE "public"."users"
TO "app_user"` — so nothing is lost by the quoted form, while a reference to a
block the document does not contain would cost the whole file: the pinned Atlas
community binary refuses it with `There is no variable named "role"`.

A target names the kind of block the document declares for it. PostgreSQL
reports the owner's implicit privileges on a view exactly as it does on a table,
so a database with a view in it produces `permission` blocks for the view too,
and those say `for = view.<name>`:

```console
$ ptah-compat schema inspect --url "$PG_URL"
view "v" {
  as = " SELECT id\n   FROM t;"
}

permission {
  to = role.app_user
  for = view.v
  privileges = ["SELECT"]
}
```

A reference in HCL names a block, and the block type is the first word of it, so
`table.v` reads as "the `v` attribute of the table object" and the community
binary refuses the file with `This object does not have an attribute named "v"`.
A materialized view is `materialized.<name>` for the same reason. The same rule
applies to a `trigger`'s `on`, which reaches a view whenever the database has an
`INSTEAD OF` trigger.

Where the document declares no single block to name, the target is written as a
quoted name instead — `for = "v"`, or `for = "other.v"` with the schema kept.
Two cases reach it, and neither is exotic. One is a target the document does not
contain, which a selection leaves behind. The other is a label the document
declares TWICE: relations share one namespace per schema, so a realm-scoped
inspect of a database with a view named `v` in two schemas declares `view "v"`
twice, and no reference names one of them in particular.

```console
$ ptah-compat schema inspect --url "$PG_URL" --schema public --schema other
view "v" {
  schema = schema.other
  as = " SELECT id\n   FROM other.t;"
}

view "v" {
  as = " SELECT id\n   FROM t;"
}

permission {
  to = role.app_user
  for = "other.v"
  privileges = ["SELECT"]
}
```

A block is named by its labels, so there is no traversal left that both resolves
and carries the schema: the community binary refuses `for = table.other.v` and
`for = view.other.v` alike with `This object does not have an attribute named
"other"`, and reads `for = "other.v"` at exit 0. The short `for = view.v` does
evaluate there and is still wrong — it means neither of the two blocks, and
reading it back would drop the schema for good. Ptah reads the quoted form back
to the same target, so applying that document issues `GRANT SELECT ON TABLE
"other"."v"`.

### Select what is inspected with `--include`

`--include` positively selects which top-level resources survive inspection,
with the same selector engine as [`schema apply` and `schema
diff`](#scope-the-comparison-with---schema-and---include): `--schema` names
the schema universe, `--include` picks resources inside it, and `--exclude`
subtracts from the result. Repeated and comma-separated values union.
Selectors that match nothing render no objects; an empty value carries no
selection, so inspection stays unfiltered.

```bash
ptah-compat schema inspect --url "sqlite://app.db" --include users
ptah-compat schema inspect --url "postgres://localhost/app" \
  --schema public --include 'app_*' --exclude app_scratch
```

Child resources — columns, indexes, constraints, triggers, policies, grants —
ride along with their parent and cannot be selected on their own; the
`[type=column]` spelling fails before any database is contacted. A positional
spelling such as `table.column` is not refused on its shape — it is
indistinguishable from a table literally named that — so it is carried to the
selection, where an identifier containing a dot can equally be named as
`main."my.table"`, `a\.b\.c`, or bare. A selection that matches nothing
renders no objects, keeps exit status 0, and reports itself on standard error.
A selection that keeps an object whose dependency it dropped is refused rather
than rendered, so inspected output never references an object it omits:

```text
error: the --schema/--include selection drops objects that selected objects depend on:
  - table "main.posts" depends on table "main.users" via a foreign key, but "main.users" is not selected
add the missing objects to the selection or exclude the dependent objects
```

The flag is not part of the pinned Atlas CE inspect surface: CE v1.2.0 rejects
`schema inspect --include` with `Error: unknown flag: --include`. It is
registered by Atlas, where its behavior differs from
Ptah's in two measured ways, both documented in
[the comparison](../comparison/#schema-inspect---include).

## Apply a desired schema

`ptah-compat schema apply` accepts a live database `--url` and a `--to`
desired state: one or more local schema file URLs, a `file://` directory of
`.sql` or `.hcl` schema files read in filename order, one directly connectable
database URL whose live schema becomes the desired state, one migration
directory (a `file://` directory containing `atlas.sum`) replayed on the
required `--dev-url` dev database, or one `env://<attribute>` reference
(`src`, `schema.src`, `url`, `dev`, `migration.dir`) resolved through the
evaluated `atlas.hcl` env.

All `--to` values must be one source kind, and unsupported schemes such as
`atlas://` fail before the target database is contacted.

A schema directory is an **ordered script**, not a set of declarations: Atlas
reads one by executing every file in filename order against the dev database.
A file that declares an object an earlier file already declared is therefore an
error rather than a merge, and Ptah refuses it the same way:

```text
Error: load --to schema: read state from "2_b.sql": table "users" already exists
```

A declaration that carries its own `IF NOT EXISTS` (or `OR REPLACE`) is
admitted, because the engine accepts it against an object that is already
there. A later file that only `ALTER`s what an earlier file created declares
nothing and neither refuses nor contributes.

With `--env`, Ptah can read `env.url`, `env.src`, `env.schema.src`, `env.dev`,
`env.exclude`, `env.schema.mode`, `format.schema.apply`, and supported `diff`
policy from the selected `atlas.hcl` environment, including local variable
defaults, locals, `getenv`, `file`, `fileset`, `format`, `jsonencode`, and
`data.hcl_schema.<name>.url` references. Explicit CLI flags still take
precedence.

Ptah reads the current database schema, diffs it against the desired local
schema files, prints the planned SQL, and applies it after interactive
confirmation. Use `--dry-run` to print the plan without applying it, or
`--auto-approve` to skip the prompt explicitly.

### Which schemas the current side is read at

The database is read at the scope the desired state names, so both sides of the
diff cover the same schemas. A desired state that names schemas beyond the one
the connection is on — as an inspected document of a multi-schema database does
— is compared against those schemas too, which is what makes inspecting a
database and applying its own output back a no-op. A desired state that names
only the connected schema reads only that one, so a schema the document never
mentions is never planned for removal. `--schema` outranks both.

Use `--tx-mode=file` or `--tx-mode=all` to execute the generated plan in one
transaction, or `--tx-mode=none` to execute statements without transaction
wrapping. With `--edit`, the planned SQL opens in `$VISUAL` or `$EDITOR`
before the plan is shown and approved, and the edited SQL is what gets
applied.

For Atlas script compatibility, `schema apply` also accepts the hidden
`--file/-f` alias for local HCL or SQL paths and maps it to the same
local desired-schema loading path as `--to`. `--file` and `--to` are mutually
exclusive.

```bash
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run
```

Expected output includes:

```text
Planned schema changes:
CREATE TABLE "users" (
  "id" INTEGER PRIMARY KEY,
  "email" TEXT NOT NULL UNIQUE,
  "name" TEXT
);
```

An `atlas.hcl` environment can carry the same inputs:

```hcl
data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = getenv("DATABASE_URL")
  dev = getenv("DEV_DATABASE_URL")
  schema {
    src = data.hcl_schema.app.url
    mode {
      funcs = false
    }
  }
  format {
    schema {
      apply = "{{ sql . \"  \" }}"
    }
  }
}
```

```bash
ptah-compat schema apply --env local --dry-run
```

`--dev-url` must match the target database dialect, and it is required
whenever `--to` is not already a live database — a schema file, a schema
directory, a migration directory, or an `env://` reference to one of those. A
missing dev database fails with Atlas's `--dev-url cannot be empty` message,
the same rule `schema inspect` and `schema diff` already apply. A database
`--to` needs none. `PTAH_ATLAS_APPLY_WITHOUT_DEV_URL=1` restores planning a
file desired state with no dev database, which Atlas refuses; native `ptah
schema apply` never had the requirement.

Before the apply touches the target, the generated plan is rehearsed
on the dev database: Ptah resets the dev database, recreates the target's
introspected current schema on it, and executes the ordered plan statements —
including SQL edited through `--edit` — under the same transaction mode as the
target apply. A failed rehearsal refuses the apply and leaves the target
unchanged. The rehearsal runs under `--dry-run` too, so a dry run cannot report
a plan the real apply would refuse.

On MySQL, MariaDB, and ClickHouse a schema *is* a database, so a plan
qualified with the target's schema name would modify the target whichever
connection issued it. Ptah re-scopes those statements onto the dev database
before rehearsing them, so the rehearsal runs entirely inside `--dev-url`. A
statement naming a third database — one that is neither the target nor the dev
database — cannot be re-scoped and is refused rather than executed somewhere
nobody asked for. On the PostgreSQL family, SQLite, and SQL Server a schema is
a namespace inside the connected database, so the plan is rehearsed exactly as
planned.

The dev database is handed back with nothing the rehearsal put in it, on both
the success and the failure path, so the same `--dev-url` stays usable by the
next command.

The dev database must not be the target itself or a database-URL `--to`
desired state (it is reset destructively), must be directly connectable (no
`docker://`), and must use the same schema scope as the target on
scope-parameterized dialects such as SQL Server. Ptah compares semantic
database identity rather than raw URL text, including percent-encoded SQLite
file URIs, path/symlink/hard-link aliases, network credentials, default ports,
loopback host spellings, and driver-level endpoint/database overrides.
Network URLs with the same dialect and selected database name fail closed
across different endpoints; malformed comparison inputs fail before reset.

`--lock-timeout` bounds how long the apply waits for the session advisory lock
(`ptah_schema_apply`) that serializes concurrent schema applies against one
target database. Strict CE mode first inventories an explicit `--schema` target
scope; without one, PostgreSQL-family targets inventory the user realm because
desired replay may name schemas beyond the URL's `search_path`. This happens
before the lock and before any migration-directory replay on the dev database.
The lock then covers the authoritative target reinspection, planning,
simulation, confirmation, and execution. It is released on every exit path,
including cancellation.

An empty value waits indefinitely. An elapsed timeout fails the apply before
the locked target reinspection; a strict target-policy refusal happens before
the timeout is consulted. PostgreSQL and YugabyteDB (`pg_advisory_lock`),
MySQL and MariaDB (`GET_LOCK`), and SQL Server (`sp_getapplock`) use real
database locks.

SQLite, ClickHouse, CockroachDB, and Spanner have no advisory-lock semantics:
the apply proceeds without a lock, and an explicitly passed `--lock-timeout`
prints a note on stderr.

`--lock-name` replaces the lock name for the run. Two runs serialize only when
they name the same lock, so this is how a Ptah apply coordinates with another
tool on the same database — and equally how it deliberately stops coordinating
with the default. Passing an empty value is refused rather than silently
falling back to `ptah_schema_apply`. On dialects without advisory locks the
note on stderr names the lock that was not acquired.

`--skip-lock` acquires no lock at all: no wait, no timeout, and no
serialization against another runner holding the same name. A lock another
process holds is ignored rather than waited on, so concurrent applies can
interleave. It cannot be combined with `--lock-name`, because there is no lock
to name.

`--exclude` accepts repeated or comma-separated Atlas-style glob patterns,
including `[type=...]` selectors. Ptah applies the filter to both the current
live schema and the desired local schema files before planning, so excluded
objects are ignored rather than dropped.

Disabled `schema.mode` values are mapped to the same resource-exclusion system
for object kinds represented in Ptah's schema IR. `diff.skip.drop_table = true`
removes table drops from supported local plans. For non-dry-run PostgreSQL
`schema apply` plans that actually emit a `CONCURRENTLY` index statement,
`--tx-mode none` is required: `diff.concurrent_index.create = true` for
`CREATE [UNIQUE] INDEX CONCURRENTLY`, and `diff.concurrent_index.drop = true`
for `DROP INDEX CONCURRENTLY`. PostgreSQL rejects either inside a transaction
block. `diff.skip.drop_schema` is
accepted and type-checked, but changes no plan: Ptah's schema diff has no
removed-schema list, so there is no `DROP SCHEMA` for the suppression to omit.

### How a column type is compared

Type spellings are compared after normalization, so a difference that is only
cosmetic — `INT` against `integer`, `VARCHAR(255)` against
`character varying(255)` — plans nothing, while a change in width, length, or
precision is reported in both directions.

**A domain is compared by identity, never by its name's spelling.** A column
whose declared type is a PostgreSQL domain agrees only with a desired type that
names the same domain; a base type, a different domain, or any other spelling
is a change and is planned as one.

A domain's identity is its schema and its name together, and the name alone is
not it: `public.status` and `other.status` are two types, with two `CHECK`
constraints over possibly different base types. Both halves are read from
`information_schema.columns` — `domain_name` and `domain_schema` — and both are
compared. A desired type that qualifies its schema is held to that schema
exactly. A desired type that does not is resolved through the domain the desired
schema declares by that name, and when nothing declares it the name alone
decides, because which domain an unqualified name reaches is a search-path
question Ptah does not answer for the server. That is why `alt.alt_dom` and a
bare `alt_dom` that no `CREATE DOMAIN` accounts for still name one domain, while
`other.alt_dom` never does.

Two declarations that share a bare name in different schemas leave an
unqualified reference to the name as well: a change between them is reported
only when one side spells its schema. That is a known gap rather than a claim —
nothing is dropped in that shape, so the cost is drift and not data loss.

The rule exists because normalization matches by substring, and a domain's name
belongs to whoever wrote the schema rather than to any type vocabulary:

```sql
CREATE DOMAIN waypoint AS integer CHECK (VALUE > 0);
CREATE DOMAIN context  AS integer;
CREATE TABLE t (id serial PRIMARY KEY, a waypoint NOT NULL, b context NOT NULL);
```

`waypoint` contains `int` and `context` contains `text`. Compared by spelling,
a column of either would match a desired `bigint` and `text` respectively and
no `ALTER COLUMN ... TYPE` would be planned — while the plan still carries the
`DROP DOMAIN ... CASCADE` that removing the domain requires, so applying it
would drop the columns and their data instead of converting them
(stokaro/ptah#1138). The same rule is what `ptah schema compare` reports on the
native surface.

The rule holds in the other direction too. A plain `integer` column against a
desired schema that declares `waypoint` and types the column with it is planned
as `ALTER TABLE "t" ALTER COLUMN "a" TYPE waypoint`, which is what the pinned
Atlas community binary v1.3.0 plans for the same pair. The desired side must
declare the domain for this: a bare type name that no `CREATE DOMAIN` in the
desired schema introduces is an ordinary type name, and every schema source
Ptah reads carries the declaration alongside the column.

An array is not a domain: its spelling is a type, and it keeps normalizing like
one. `format_type` writes every array with a trailing `[]`, including an array
of a domain, so the two are told apart by the catalog rather than by the name.

### Scope the comparison with `--schema` and `--include`

`--schema/-s` and `--include` positively select what both comparison sides
see, on `schema apply` and `schema diff` alike:

- `--schema` names define the schema universe. Repeated and comma-separated
  values union deterministically. On PostgreSQL-family targets the names are
  schema namespaces and unqualified objects belong to the connection's default
  schema (`public`). On MySQL and MariaDB a schema is a database, and because
  a Ptah connection is bound to one database, only the connected database's
  name selects anything. SQLite has the single schema `main`.
- `--include` picks top-level resources inside that universe with Atlas-style
  glob selectors, optionally constrained with `[type=...]`. Selectable types:
  `table`, `view`, `materialized_view`, `function`, `enum`, `extension`,
  `sequence`, `domain`, `composite_type`, `range`, and `role`. Repeated and
  comma-separated selectors union deterministically. Children of a selected
  table — columns, indexes, constraints, triggers, policies, grants, and seed
  data — ride along with it, and support objects the selection depends on
  (enums and other types used by kept columns, sequences owned by kept
  tables, roles named by kept grants, owning schemas) are retained.
  Child-resource selectors such as `[type=column]` or `[type=index]`, field
  selectors, and unknown resource types are rejected loudly because Ptah
  cannot project a partial parent faithfully. A positional spelling such as
  `main.users.email` is **not** rejected on its shape: it cannot be told apart
  from a table literally named `users.email`, so it is carried to the
  selection and answered there.

#### Selecting an identifier that contains a dot

All three spellings of a dotted name select a table named `a.b.c`:

```bash
ptah-compat schema diff … --include 'a.b.c'          # bare
ptah-compat schema diff … --include 'a\.b\.c'        # escaped
ptah-compat schema diff … --include 'main."a.b.c"'   # quoted and qualified
```

The bare spelling used to be refused as ambiguous with `schema.table.column`,
and the other two were documented as the workaround. Both workarounds still
parse and still select, so nothing written against them breaks; the bare form
works again as well.

#### A selection that matches nothing

Whether a selector matched is an **outcome**, not a property of its text.
`path.Match` treats `.` as an ordinary character — only `/` separates — so
every metacharacter that can stand for a dot reaches past a top-level
resource, and all four of these select nothing:

```bash
ptah-compat schema diff … --include 'main.users.email'
ptah-compat schema diff … --include 'main.users*email'
ptah-compat schema diff … --include 'main.users?email'
ptah-compat schema diff … --include 'main.users[.]email'
```

So does a plain typo. Each verb answers that condition differently, on
purpose:

| Verb | Exit status | Standard output | Standard error |
| --- | --- | --- | --- |
| `schema diff` | `1` | empty | `Error: the --include selection matched no objects: "…"` |
| `schema inspect` | `0` | unchanged (no objects rendered) | same warning |
| `schema apply` | `1` | empty | `Error: the --include selection matched no objects: "…"; schema apply would change nothing` |

`diff` refuses because a successful synced result can green-light a CI check
that compared nothing. `apply` refuses because the empty selection leaves its
target untouched. `inspect` keeps exit status 0 because an empty read is a
legitimate result, but its warning makes the cause visible.

This is a deliberate choice rather than a matched behavior. Atlas CE
implements no `--include` flag at all, so there is no measurement to copy for
it. The full Ptah surface keeps that Pro-like capability, including selectors
that match a real dotted top-level name, while refusing only the no-match
outcome on plan-producing verbs. `--schema` on its own is unaffected here:
narrowing to a schema that holds nothing stays an ordinary answer.
- `--exclude` and disabled `schema.mode` values subtract from the positive
  selection afterward. The composition order is fixed: schema universe first,
  include selection inside it, exclusion last.

The same projection is applied to the current database state and the desired
schema, so out-of-scope objects are invisible to the comparison and are never
created, modified, or dropped. A selected object that depends on an object the
selection dropped — a foreign key to an unselected table, a function calling
an unselected function, a view or trigger body referencing an unselected
relation, a column using an excluded enum — refuses the plan with an explicit
cross-scope diagnostic instead of emitting incomplete SQL:

```text
error: apply --schema/--include to desired schema: the --schema/--include selection drops objects that selected objects depend on:
  - table "scope_groups" depends on table "scope_users" via a foreign key, but "scope_users" is not selected
add the missing objects to the selection or exclude the dependent objects
```

An `--include` selection that matches neither the target nor the desired state
refuses the apply, because there is nothing to apply and a synced-schema
message would claim success for work that did not happen. A `--schemas`
selection that narrows to nothing is not an error. Malformed selectors fail
during validation, before any database is contacted.

`--format` accepts Atlas-style Go templates over the planned apply changes. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --dry-run \
  --format '{{ sql . "  " }}'
```

## Save and execute plan files

`ptah-compat schema plan` is the open local replacement for Atlas's Pro
registry-gated plan workflow, and it speaks Atlas's plan-file format.

It computes the same declarative plan `schema apply` would generate — from the
`--from` target database to the local `--to` schema files — and saves it as a
local plan file. The default format is the Atlas `.plan.hcl` shape: one
`plan "<name>"` block with `from`/`to` fingerprints and the migration SQL in a
heredoc, named with an Atlas-style UTC timestamp. The written file parses in
Atlas's own plan reader; the `from`/`to` values are Ptah's sha256
fingerprints, which Atlas parses but cannot verify against its
own base64 hashes (those have no local recipe — in either direction). An
`--output` path ending in `.json` writes the native JSON plan
(`format_version` 1) instead, which additionally records per-statement safety
severity, the dialect, and exclude patterns. Without
`--save`/`--output`/`--dry-run`, the plan document prints to stdout, and
`--auto-approve` is accepted for CLI compatibility.

Use `--edit` to review or rewrite the planned SQL in `$VISUAL` or `$EDITOR`
before publication. Ptah preserves statement text and comments, rejects
invalid UTF-8 and empty edited plans, and recomputes safety metadata with the
plan dialect, including MySQL and MariaDB executable comments. `--name-format` accepts a Go template over `.FromHash`
and `.ToHash`; Ptah exposes its digest bytes in the untagged standard-Base64
representation measured from Atlas. `--skip-lint` is
accepted as an explicit no-op because this command has no lint step.

Plan publication stages and syncs the complete document before an atomic
rename. Default `--save` refuses an existing entry without a check/write race;
explicit `--output` atomically replaces the named entry instead of following a
symlink or exposing a partially written document.

`schema apply --plan file://<path>` accepts both formats, detected by
content — including `.plan.hcl` files written by Atlas:

- A **JSON plan** executes after verifying the live database still matches
  the plan's recorded source fingerprint; a drifted database refuses with a
  stale-plan error instead of running reviewed SQL against unreviewed state.
  `--to` is optional.
- An **Atlas-format plan** requires `--to`, exactly as Atlas does
  (`the flag "to" is required to verify the provided plan`). Its hashes are
  re-verified with Ptah's own machinery: the plan is replayed on a dev
  database starting from the target's current schema, and the reached state
  must equal the `--to` desired state under Ptah's schema diff before the
  target is touched. SQLite targets get a throwaway dev database
  automatically; every other dialect requires `--dev-url`. A Ptah-written
  `.plan.hcl` keeps native sha256 fingerprints, so it gets the stale-plan
  check too — but the replay runs either way, because the fingerprint shape
  is public and must never be able to switch a verification off.

### The replay is not a sandbox

Before replaying, Ptah refuses statements that match a **deny-list of known
escape constructs** — `ATTACH`/`DETACH`, `VACUUM INTO`,
storage-directory pragmas, `load_extension`, routine bodies and dynamic SQL
calling file-access or `dblink` functions, `LOAD DATA INFILE`,
`SELECT ... INTO OUTFILE`/`DUMPFILE`, `LOAD_FILE`, `ENGINE=FEDERATED`,
`CREATE SERVER`, `INSTALL PLUGIN`/`COMPONENT`, `DATA`/`INDEX DIRECTORY`,
`COPY ... PROGRAM` or `COPY` with a file path, `dblink`, `postgres_fdw`,
`file_fdw`, the SQL Server `xp_`/`sp_addlinkedserver`/`OPENROWSET` family, and
ClickHouse's remote table engines:

```text
error: pre-planned migration was refused before it reached the dev database: statement 1 uses ATTACH, which attaches another SQLite database file to the session ...
```

An anonymous `DO` block is not itself refused — it is the standard PostgreSQL
idiom for idempotent DDL, and a foreign plan is full of them — but what its
body does is scanned like any other statement.

**That list is best-effort and not exhaustive, and the replay is not a
sandbox.** SQL dialects offer many ways to address something other than the
connected database — server-side language extensions, foreign-data wrappers,
storage-engine options, loadable modules, and engine-specific pragmas and
functions — and new ones arrive with new engine versions. Treat the deny-list
as a tripwire for honest mistakes and known tricks, not as a security
boundary against a hostile plan author.

The practical rule: **a `--dev-url` must point at a database you are willing
to have a foreign plan file execute arbitrary SQL against.** Use a disposable
dev database, not one that shares a server, credentials, or filesystem with
anything you care about. The lint is a tripwire in front of it, not a wall
around it.

### Where enforcement is real

The ephemeral SQLite dev database — the one Ptah creates for SQLite targets,
which you never opt into — is the exception, and it does not rely on the lint
at all. It is a throwaway file in a private temporary directory, removed when
the command exits, and its session is restricted at the engine level before
any plan SQL runs:

- `ATTACH` and `DETACH` are refused by SQLite itself, so plan SQL cannot reach
  another database file — including the real target.
- `VACUUM INTO` is refused by the same restriction, so it cannot write a
  database copy to an arbitrary path.
- Native extensions cannot be loaded.

What the engine does **not** stop, and the lint therefore still has to: the
storage-directory pragmas (`temp_store_directory`, `data_store_directory` —
the first is process-global in SQLite) and `PRAGMA writable_schema`. The last
one has a consequence worth stating: a plan that sets `writable_schema` could
edit the dev database's catalog directly, so the "converges to `--to`" verdict
is not tamper-proof against the very document being verified. The verdict is a
good-faith check, not an adversarial one.

Ptah verifies the restriction is in force before rehearsing, and refuses to
rehearse if it is not, so this cannot fail silently. These are engine
refusals: they hold for statements the lint never recognized, including ones
built by string concatenation at run time.

The restriction keys on the **dev** database's dialect, not on who supplied
it, so an operator-supplied SQLite `--dev-url` gets exactly the same engine
refusals. For any dialect other than SQLite, none of the above applies: that
database executes the plan's SQL for real, with whatever credentials and
network reach you gave it.

The verification also runs under `--dry-run`, so a plan received from someone
else can be checked without committing to apply it.

After every `--plan` apply with a desired state available, the end state is
verified again on the target — the semantic end-state verification Atlas
performs — and a mismatch fails loudly. There is no flag to disable it.

```bash
# Compute and save the plan for review (or --save for ./<timestamp>.plan.hcl).
ptah-compat schema plan \
  --from "$DATABASE_URL" \
  --to file://schema.sql \
  --output add-orders.plan.hcl

# Later, execute exactly the reviewed plan; drift refuses loudly.
ptah-compat schema apply \
  --url "$DATABASE_URL" \
  --to file://schema.sql \
  --plan file://add-orders.plan.hcl \
  --auto-approve
```

Expected output of the plan step ends with:

```text
Plan saved to file://add-orders.plan.hcl
```

If the target database changed after the plan was saved, apply refuses before
touching the target. For a plan with native fingerprints the stale-plan error
names both fingerprints:

```text
error: pre-planned migration is stale: the target database schema does not match the plan's source fingerprint (plan sha256:..., database sha256:...); the database changed since the plan was computed, so re-run `schema plan` against the current database and review the fresh plan
```

For an Atlas-authored plan the drift surfaces semantically, and the plan is
not applied to the target:

```text
error: pre-planned migration does not converge to the desired state: replaying the plan on the dev database, starting from the target's current schema, left the following schema drift against --to (the plan was not applied to the target):
...
```

## Diff schema files

`ptah-compat schema diff` accepts a desired-state source on each side: one or
more local `--from`/`--to` schema file URLs, a `file://` directory of `.sql` or
`.hcl` schema files read in filename order as an ordered script, one directly
connectable database URL whose live schema is introspected, one migration
directory (a `file://` directory containing `atlas.sum`) replayed on the
required `--dev-url` dev database, or one `env://<attribute>` reference (`src`,
`schema.src`, `url`, `dev`, `migration.dir`) resolved through the evaluated
`atlas.hcl` env.

All URLs of one flag must be one source kind. The SQL dialect is pinned by
`--dev-url` first, then by `--from` and `--to` database URLs; local schema
files alone still require `--dev-url` for dialect selection. With `--env`,
Ptah can read `env.schema.src`, `env.dev`, `env.exclude`, `env.schema.mode`,
`format.schema.diff`, and supported `diff` policy from `atlas.hcl`.

The current implementation does not execute Atlas's dev-database simulation;
the dev URL selects the dialect and hosts migration-directory replays.

```bash
ptah-compat schema diff \
  -f file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db"
```

With an `old.hcl` declaring only a `users` table and a `schema.hcl` adding a
`posts` table, expected output includes:

```text
CREATE TABLE "main"."posts" (
  "id" INTEGER PRIMARY KEY,
  "user_id" INTEGER NOT NULL
);
```

`--format` accepts Atlas-style Go templates over Ptah's local diff report. The
supported template surface includes the `sql` helper and `.MarshalSQL`:

```bash
ptah-compat schema diff \
  --from file://old.hcl \
  --to file://schema.hcl \
  --dev-url "sqlite://dev.db" \
  --format '{{ sql . "  " }}'
```

Unsupported schemes — `atlas://` registry URLs, `docker://` as a desired
state, and anything Ptah cannot connect to directly — fail during validation,
before any database is contacted. A migration-directory source without
`--dev-url` fails the same way. Non-Atlas-CE flags such as `--tx-mode` are
rejected as unknown.

`--exclude` and disabled `schema.mode` values filter both local `--from` and
`--to` schema files before diffing, and `--schema`/`--include` positively
scope both sides with the same selection semantics, composition order, and
cross-scope dependency diagnostics as `schema apply` (see [Scope the
comparison with `--schema` and
`--include`](#scope-the-comparison-with---schema-and---include)).

A diff whose change needs a dialect-specific rebuild plan — for example adding
a column to a SQLite table — fails with an explicit error instead of emitting
SQL the dialect cannot run in place.

## Format schema files

`ptah-compat schema fmt` rewrites local `.hcl` files into HCL canonical layout:

```bash
ptah-compat schema fmt schema.hcl
```

## Clean a database

`ptah-compat schema clean` plans and applies destructive cleanup of user-owned
schema objects. Preview first:

```bash
ptah-compat schema clean --url "$DATABASE_URL" --dry-run
```

Against a SQLite database containing one `users` table and one `v_const` view,
expected output includes:

```text
Planned cleanup changes: 2
- DROP TABLE IF EXISTS "users"
- DROP VIEW IF EXISTS "v_const"
[DRY RUN] No changes were applied.
```

The plan lists the object kinds the target dialect's cleanup really destroys —
see the [per-dialect coverage table](../../reference/atlas-commands/#ptah-compat-schema-clean).
Rows are ordered alphabetically by object kind; that is a report order, not the
order the statements run in. Scoped cleanup uses a separate deterministic
dependency order for known relationships, including views before tables and
tables before their implicit PostgreSQL `SERIAL` or identity sequences.
PostgreSQL reads live catalog depth to drop a dependent view or materialized
view before another selected view of the same kind. It applies the whole scoped
plan transactionally, so a later `RESTRICT` refusal rolls back earlier drops.

An implicit sequence is not independently selectable: including it without its
owning table, or excluding it while the table remains selected, is refused
before cleanup mutates the database. Select or exclude the owning table so the
child sequence rides with the same decision.

:::danger
Without `--dry-run`, cleanup drops the listed objects after confirmation
(`--auto-approve` skips the prompt). There is no undo.
:::

## Format template fields

| Command | Format data fields |
| --- | --- |
| `ptah-compat schema inspect --format` | `.Realm`, `.Schema`, `.MarshalHCL`, `.MarshalSQL`, `.MarshalJSON`, plus `hcl`, `sql`, `json`, `base64url`, `mermaid`, `split`, and `write` template helpers. |
| `ptah-compat schema apply --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for the planned SQL statements. |
| `ptah-compat schema diff --format` | `.Changes`, `.MarshalSQL`, plus the `sql` helper for generated migration SQL. |
| `ptah-compat schema clean --format` | `.Env.Driver`, `.Env.URL`, `.DryRun`, `.Applied`, `.Objects`, and `.Changes`. |

Function entries in `.Objects` and `.Changes` also carry `.Parameters`, and
their `.Cmd` includes the PostgreSQL signature so overloads remain distinct.

The shared report shape and URL redaction rules are described on the
[Atlas compatibility overview](../overview/#format-reports-and-redaction).

## Next steps

- Managing migration directories on this surface:
  [Atlas migrate commands](../migrate-commands/).
- Doing direct changes with a native-first flow:
  [Apply schema changes directly](../../direct/apply/).
- Checking the supported `atlas.hcl` inputs these commands read:
  [Atlas project config](../project-config/).
