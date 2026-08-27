# ADR 0011: Database-code analysis starts from what the tree already derives

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#1270](https://github.com/stokaro/ptah/issues/1270)
- Records Stage 0 of that issue: the inventory, the first dialect, and the questions Stage 1 cannot start without
- Superseded in part: the Stage 1 context inputs by
  [ADR 0012](0012-the-canonical-core-is-removed-and-the-shipping-pipeline-migrates-in-place.md),
  which removes `schemastate.Profile`. What the analysis needs — the declared
  and observed schemas as a pair, plus the target's capabilities — is unchanged;
  only the type that was to carry the target facts is gone.

## 1. Context

#1270 asks for schema-aware analysis of database-resident code — views,
functions, triggers, policies — and freezes its own implementation:

> Do not begin implementation before GA.

Ptah is pre-GA. The newest tag is `v0.2.0` and `AGENTS.md` opens its
compatibility section with "Ptah is pre-GA." That freeze is correct and this
record does not lift it.

The same section carves out what may be done now:

> Research, design notes, and candidate-rule inventories may be recorded
> earlier.

Stage 0 of the issue is exactly that, and says so: "No user-visible
implementation required yet." This record is Stage 0. It writes down what the
tree already has, so whoever starts Stage 1 after GA begins from measurements
rather than from a survey they have to repeat.

## 2. The inventory, measured

### 2.1 The objects are modelled on both sides, with their bodies

Every programmable object #1270 names is already a first-class object in both
models, and every one carries the text an analyzer would read:

| Object | `core/schemamodel` | `catalog` | Body field |
| --- | --- | --- | --- |
| Function | 11 fields | 13 fields | `Body` |
| View | 7 fields | 6 fields | `Body` (+ `WithCheck` on the desired side) |
| MaterializedView | 6 fields | 5 fields | `Body` |
| Trigger | 10 fields | 9 fields | `Body` |
| RLSPolicy | 9 fields | 7 fields | `UsingExpression` |

The epic's Stage 0 asks whether Ptah models these. It does, on both sides of the
comparison, which is the harder half: analysis wants the before-state and the
after-state, and both are already representable in one type each.

### 2.2 The parser already reaches inside a PL/pgSQL body

`internal/parser` does not stop at the routine header. A PostgreSQL routine body
is parsed into `ast.PostgresRoutineBody`, which carries the language, the
dollar-quote delimiter, and `Statements []PostgresRoutineStatement` — a
statement list with a `Kind` that distinguishes, among others:

```text
assignment  block  case  close  cursor  declaration  delete
exception   execute  fetch  handler  if  insert  iterate
```

MySQL and SQL Server routines have their own frontends in the same package
(`mysql_routine.go`, `mysql_routine_declare.go`, `sqlserver_routine.go`).

So the "parser strategy" section of #1270 is not starting from zero. What is
missing is a consumer. Measured by naming the types: `ast.PostgresRoutineBody`
and `ast.PostgresRoutineStatement` appear in eight non-test files, and all eight
either define them (`core/ast`), produce them (`internal/parser`, four files) or
refuse a read that cannot see a body at all
(`internal/dbschema/mysql/reader.go`, which names the type only to say
`hiddenRoutineBodyError` when the connected account cannot see the routine
source). **Nothing reads the statements.** They are produced and no analysis
looks at them.

### 2.3 The findings model exists and is already used

`migration/lint.Finding` carries what a finding needs:

```go
type Finding struct {
    Rule     string
    Title    string
    Severity Severity
    File     string
    Line     int
    Message  string
    Context  *FindingContext
}
```

and the rule identifiers are already stable and namespaced by family — thirteen
of them today: `BC101`, `BC102`, `DD101`, `DS101`, `DS102`, `DS103`, `MF101`,
`MF102`, `MF103`, `MY101`, `PG101`, `PG103`, `TX101`.

#1270's "stable rule identifiers" and "findings model" sections therefore
describe something to extend rather than to invent. A code-analysis family takes
its own prefix and the existing renderers keep working.

## 3. Where the tree already goes further than the epic assumes

`internal/schemalineage` (#1712) derives **column-to-column** dependencies from
view and materialized-view bodies:

```go
type Edge struct {
    FromTable, FromColumn string
    ToView, ToColumn      string
    Materialized          bool
}
```

and — this is the part worth copying rather than the edges — it carries what it
could not resolve:

```go
type Undecided struct {
    View, Reason string
    Materialized bool
}
```

Its package comment states the rule this record adopts wholesale:

> a caller asking "is it safe to drop this column" can tell "nothing depends on
> it" from "I could not tell" — a distinction that decides whether the answer may
> be trusted.

That is #1270's "dropped dependency" rule, built, for views. It is not built for
functions, triggers or policies, and it is column-level rather than
object-level.

`internal/deporder` is the other half already present: deterministic topological
ordering over view-like objects, used to create and drop in dependency order.

## 4. Decisions

### 4.1 PostgreSQL is the first dialect

The issue suggests it and the inventory agrees: PostgreSQL is the only dialect
whose routine bodies are parsed into a typed statement list, and the one whose
programmable objects are modelled most completely.

The alternative considered and rejected: **SQL Server first**, on the argument
that its catalog validates procedures itself and would give server-assisted
findings for free. It is rejected for Stage 1 because server-assisted analysis is
Stage 3 — starting there would make the first deliverable depend on a live
server, and #1270 requires that a Ptah with no external analyzer stays
functional.

### 4.2 The dependency representation is an extension of `schemalineage`, not a new graph

Stage 0 asks to "define dependency representation". The answer is that one
exists and has the right shape: an edge list plus an explicit undecided list.

What Stage 1 needs is a **wider producer**, not a different consumer — the same
`Edge`/`Undecided` pair derived from function, trigger and policy bodies rather
than only from view bodies, and an object-level edge for the cases where a
column cannot be named.

The alternative considered and rejected: a **new general dependency graph** in
its own package, on the argument that analysis wants object-level edges and
lineage is column-level. It is rejected because two representations of "what
depends on what" is how two layers come to disagree about it, and because the
undecided bucket is the property that makes either of them safe to act on.

### 4.3 The analysis context is the pair the pipeline already carries

Stage 0 asks to "define analysis context". #1270 draws it as source + current
schema + desired schema + before-state + after-state + dependency graph +
capabilities.

Measured against the tree, that is: two `schemamodel.Database` values, the
`schemalineage.Result` derived from either, and the `schemastate.Profile` the
canonical pipeline already threads. No new context type is required for Stage 1;
what is required is that analysis take the pair rather than one side, which is
what makes it schema-aware rather than a linter.

## 5. What this does not decide

Stated as questions, because measurement cannot answer them and guessing would
put an unowned decision into the tree:

1. **Does a code-analysis finding block a migration, or only report?**
   `migration/lint` has severities and a threshold; whether a broken function
   dependency is `error` or `warning` by default is a product decision about how
   much Ptah is willing to refuse.
2. **What is the boundary of "undecidable" for a routine body?**
   Views have one measured answer already. Dynamic SQL, `EXECUTE` with a
   composed string, and a call into another routine each need a stated rule, and
   the honest default — report the boundary rather than the finding — costs
   noise.
3. **Which external analyzers, if any?** #1270 lists candidates by name;
   this record deliberately does not rank them, because no survey has been run
   and a ranking written from memory would be worse than none.
4. **Does analysis run against the desired state, the catalog, or both?**
   The pair is available; running both doubles the findings and needs a rule for
   which one a message names.

## 6. Consequences

Accepted:

- Stage 1 starts from a producer extension rather than a new subsystem, so its
  first deliverable is narrow: function, trigger and policy bodies producing the
  edges and undecided entries views already produce.
- The findings model, the rule-identifier scheme and the report renderers are
  reused, so a code-analysis finding appears wherever a lint finding appears
  without new plumbing.
- PostgreSQL-first means the first useful version helps only PostgreSQL users.
  The three other frontends exist in the parser, so the cost is analysis rules
  rather than parsing.

The cost, stated because it is real: this record makes Stage 0 done and Stage 1
schedulable, and it does not move the freeze. #1270 stays closed to
implementation until GA, and the four questions in section 5 have to be answered
before Stage 1 can be specified — not before it can be started, but before it can
be finished.
