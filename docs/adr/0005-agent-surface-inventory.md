# ADR 0005: The agent-exposure inventory is generated, and classified by what a verb does to a database

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1484](https://github.com/stokaro/ptah/issues/1484), under [#1483](https://github.com/stokaro/ptah/issues/1483)
- Supersedes: sections 1.1 and 1.2 of
  [ADR 0002](0002-read-only-agent-mvp-scope-and-transport.md). Its decisions —
  the frozen MVP operation set, the naming rule and the transport — stand.

## 1. Context

[ADR 0002](0002-read-only-agent-mvp-scope-and-transport.md) froze the read-only
MVP scope. To do that it had to say what the surface WAS, so §1.1 enumerated the
verb list and §1.2 classified the reading verbs by which of them register
`--dev-url`. Both were measured from the built binary on 2026-08-21, and the
record says so.

This repository's ADRs are written under a rule stated in
[the index](README.md): *a record is never edited to reflect a later change of
mind.* A measurement inside such a record can therefore only age. It did.

### 1.1 The inventory went stale, and nothing could notice

Enumerated again from the binary at `7655b21b`, against the table in ADR 0002
§1.1:

| group | in the binary and not in the record |
| --- | --- |
| `schema` | `approve`, `security`, `verify-approval` |
| `migrations` | `tag`, `test`, `up`, `validate` |
| top level | `assist`, `mcp` |

Nine verbs. Three of them — `schema approve`, `schema security`,
`schema verify-approval` — are exactly the kind an agent-exposure decision needs
classified, and two are this epic's own surface, added by
[#1486](https://github.com/stokaro/ptah/issues/1486) and
[#1488](https://github.com/stokaro/ptah/issues/1488) after the record was
written.

### 1.2 The axis was the flag name, and the flag name is not the axis

ADR 0002 §1.2 read: *"Measured by which of them register `--dev-url`."* That
rule is wrong in both directions, measured on the same binary.

**A false negative.** `schema test` was listed as a pure reader — "reads the
target; writes nothing anywhere". Its own help calls `--db-url` a *"Throwaway
database URL"*, and its test cases `exec` raw SQL and `apply_schema`. Measured
against PostgreSQL 17.11, with a database holding a table nobody asked Ptah to
touch:

```console
$ ptah schema test --db-url "$PG" --dir tests --root-dir models
PASS  case "converge the desired schema"
    PASS  step — desired schema already applied
    PASS  step — exec ok
$ psql -d "$PG" -c '\dt'
 public | operator_data | table
 public | widgets       | table
```

`widgets` was created by the "pure reader". `migrations test` has the same
shape and the same flag.

**A false positive.** `schema compare` registers `--dev-url` and is not in the
destructive class. Its flag says what it is for: *"used to ask the target engine
how it spells a declared generated-column expression. Only Oracle needs one"*.
It creates a probe table there and drops it again, and it preserves everything
else.

And the axis missed a whole flag. `--shadow-db` names a second database for
`migrations baseline`, `checkpoint`, `down` and `generate`, in words that leave
no doubt: *"Ephemeral shadow database URL the directory is replayed into"*.

### 1.3 One row conflated two different things

ADR 0002's pure-reader row lists `schema render`, `schema lineage`,
`schema validate`, `sql lint`, `migrations ls` and `migrations show` beside
`db read` and `migrations status`, and describes them all as "reads the target".
The first six open no connection at all. For a human that is a distinction
without a difference; for a surface an agent drives it is the difference between
an operation that can leak the contents of a production database and one that
cannot reach one.

## 2. Decision

### 2.1 The inventory moves out of the ADR and is generated

The list of verbs and their classification live in
[`docs/agent-surface.md`](../agent-surface.md), generated from the command tree
of the binary this repository builds, and checked by
`scripts/check-docsync.sh` in the lint job.

A record of a decision and a measurement of a moving surface are different
documents with different lifetimes, and putting the second inside the first
guaranteed the outcome above. This record decides where the inventory lives and
what it means; it does not carry a copy.

### 2.2 The classification has two axes, and the written half is checked against the measured one

**Target** — what the verb does to the database it is pointed at: none, reads,
writes. **Second database** — what it does to a dev, shadow or throwaway
database that is not the target: none, probes, rewrites.

Two axes rather than one grade, because the two are independent: `schema inspect`
reads its target and resets its dev database, and a single scale has nowhere to
put that.

What a verb DOES cannot be read off its flag set — that is why the
classification is written by hand, in `internal/agentsurface`. What it can be
POINTED AT can: a verb with no `--db-url` cannot touch a target, and one with no
`--dev-url` or `--shadow-db` has no second database to rewrite. The two halves
are required to agree, in both directions, by a test in that package. A verb the
classification does not name fails the same test.

That is the part that makes this durable. The previous inventory was prose
checked by nobody; this one fails the build on the day a verb is added.

### 2.3 "Safe" is answered about databases and nothing else

`Verb.DatabaseSafe` answers one question: can driving this verb change or
destroy a database. It is deliberately not a verdict on exposure. `introspect`
and `schema fmt` write files in the working tree, `schema push` and `oci copy`
publish to a registry, `assist provider test` reaches a model endpoint, and
`schema serve` opens a listener — every one of them database-safe.

Naming the answer for its scope is the correction §1.2 needed: a label that
claims more than it measured is how `schema test` came to be called a pure
reader.

## 3. Alternatives

**Keep the inventory in ADR 0002 and edit it when the surface changes.** Simple,
and it breaks the rule the ADR index states — a record that is edited is no
longer evidence of what was known when the decision was made. It also would not
have helped: nothing would have told anyone to edit it.

**Generate the inventory and skip the hand-written classification.** Everything
would be derived, and nothing would be answered. The flag set says which
databases a verb can be pointed at; it does not say what happens to them, which
is the only question an agent-exposure decision asks. A generated table of flags
would have called `schema test` and `db read` the same thing.

**Classify by parsing `--help` output.** Tempting, because the destructive
sentences are usually there — "it is reset destructively", "Throwaway database
URL". It would make the classification a text search over prose that is written
for people and changed freely, and a reworded help string would silently
reclassify an operation. The flags are a contract; the sentences are not.

**One severity scale instead of two axes.** `writes > rewrites > reads > none`
reads well until `schema inspect`, which reads its target and resets a database.
Collapsing that to its strongest grade loses the fact that the TARGET is safe,
which is what a caller pointing it at production needs to know.

## 4. Consequences

- The inventory is true on the day it is read, or the lint job is red. That is
  the whole point, and it is also a cost: adding a verb now requires
  classifying it, with a reason long enough to survive the guard.
- ADR 0002 §1.1 and §1.2 stay in place, as the record of what was known on
  2026-08-21. A reader who wants the current surface is sent here and then to
  the generated document.
- The shipped MCP server exposes four read tools — `validate_schema`,
  `render_schema`, `schema_lineage`, `read_database` — and the workspace,
  artifact and patch tools ADR 0004 governs. Every one of the four is
  database-safe under this classification, so nothing shipped moves. The
  reclassification of `schema test` costs nothing today precisely because the
  MVP list was frozen by name rather than by class.
- `Verb.DatabaseSafe` is a shortlist and not a permission. A later record that
  decides file and network exposure will narrow it, and this one says so rather
  than leaving the next reader to discover it.
