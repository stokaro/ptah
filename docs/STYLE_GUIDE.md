# Ptah documentation style guide

This is the authoritative style guide for all Ptah documentation: the site
under `docs/site/src/content/docs/`, the repository docs under `docs/*.md`,
package READMEs, example READMEs, integration docs, and `AGENTS.md` itself.
Contributor guidance (`AGENTS.md`) and the documentation-maintenance skill
(`.agents/skills/ptah-documentation-maintenance/SKILL.md`) require it. Section
16 lists which of its rules CI enforces.

It is contributor-facing and stays out of the reader sidebar. Site pages must
not link to it on GitHub (`check-core-doc-links.mjs` forbids root-docs GitHub
links from site content); no reader page needs to.

Companion documents:

- `docs/site/content-inventory.json` — the generated factual page inventory.
- `docs/site/CONTENT_INVENTORY.md` — reader journeys and editorial decisions
  that cannot be derived from the tree.
- `.agents/skills/ptah-documentation-maintenance/SKILL.md` — the workflow for
  keeping documentation aligned with behavior.

## 1. Principles

- **Audience-first.** Name the reader and the question the page answers in the
  first two sentences. If you cannot, the page has no reason to exist.
- **Task-first.** Procedures over feature inventories. Readers arrive with a
  goal, not with curiosity about flags.
- **Evidence-backed.** Every support, behavior, or parity claim must be
  traceable to code, `--help` output, tests, runnable examples, or current
  `stokaro/ptah-atlas-conformance` reports.
- **No unowned tracking claims.** "Tracked separately", "tracked elsewhere" and
  the like promise a reader that a gap has an owner. Name the issue in the same
  paragraph, or say plainly that nothing tracks it yet. The second is honest and
  costs nothing; the first without a number upgrades an open hole into apparent
  process, and nothing in the repository disagrees with it. Enforced by
  `check-style.mjs`.
- **License-clean.** Never describe Atlas internals; describe only observable
  behavior. Never copy Atlas prose, examples, diagrams, assets, taxonomy, or
  page sequence.
- **No unsupported parity claims.** Ptah is pre-GA. Full Atlas parity or
  drop-in status is claimed only where current conformance evidence proves it.

## 2. Page taxonomy

Every page has exactly one primary type. Choose it before writing a word.

| Type | Purpose | Canonical example |
| --- | --- | --- |
| Landing | Route one product domain: orientation, common tasks, one decision cue, and links to canonical work | `inference/overview` |
| Tutorial | Learning by doing: one happy path, guaranteed end state, cleanup | `start/quick-start-migrations` |
| Concept | Why and when: mental model, minimal commands | license boundary page |
| How-to | Goal-directed steps for a competent reader; variants and failure modes allowed | `versioned/checkpoints` |
| Reference | Exhaustive and scannable; no narrative | `reference/exit-codes` |
| Troubleshooting | Symptom-keyed diagnosis and fixes | `operate/troubleshooting` |
| Status | Evidence-dated claims about parity or coverage | `atlas/conformance` |
| Contributor | Repo-layer docs, out of reader navigation | `docs/system_design.md` |

Mixing rule: a how-to may open with at most two short concept paragraphs;
anything longer moves to a concept page and is linked. Reference pages never
teach; tutorials never enumerate flags. A page that needs two types is two
pages.

## 3. Page templates

Required section flow per type. Optional sections are marked.

**Landing**: concise orientation → the situation in which the domain matters →
three to five common tasks → one decision cue → canonical task links → route to
reference. A landing page is not an exhaustive tutorial or flag inventory.

**Tutorial**: goal → prerequisites → numbered steps (each step: command,
expected output, one sentence of why) → verification → cleanup → next steps.

**Concept**: the idea in one paragraph → how Ptah models it →
consequences and tradeoffs → where it appears (links).

**How-to**: goal sentence + prerequisites → starting state → steps with
expected output → verification → failure modes (or a link to troubleshooting)
→ limitations (optional; only those affecting this task) → next steps (at most
three links, each phrased as the next decision).

**Reference**: one-sentence scope → tables or lists → exact behavior notes →
links to the owning workflow pages. A reference whose generated index is too
large to scan must provide the shared client-side filter without changing the
generated rows.

**Troubleshooting entry**: symptom as the heading, in the reader's words
(error text or observed behavior) → likely cause → diagnosis command → fix →
verify.

**Status**: claim scope → evidence date and source → matrix →
how to re-verify.

Do not combine lookup, policy, and evidence merely because all three concern one
feature. A support lookup lists current generated facts; a policy defines what
their labels promise; an evidence page records how those claims were measured
and when.

### 3.1 Frontmatter and the generated inventory

Every published page declares the editorial contract that
`src/content.config.ts` validates:

```yaml
type: how-to
audience:
  - database-engineer
readerQuestion: How do I apply an already reviewed schema plan?
goal: Apply the plan to a target database and verify the resulting schema.
sourceOfTruth:
  - cmd/schema
  - migration/planner
generated: false
overlaps:
  - /direct/apply/
disposition: keep
```

Use exactly one `type`: `landing`, `tutorial`, `how-to`, `concept`,
`reference`, `troubleshooting`, `status`, or `contributor`. Write
`readerQuestion` as the one question the page answers. `goal` completes the
sentence, "After reading this page, the reader can ...". If it contains two
independent outcomes, split the page or record `disposition: split` until the
split lands.

`sourceOfTruth` names the implementation, declaration, test, or evidence that
settles the page's claims. `owns` remains the optional feature identifier list
used by `docs/feature-inventory.json`. `overlaps` uses live docs routes and is
explicitly `[]` when the review found none. `disposition` is one of `keep`,
`rewrite`, `split`, `merge`, `move`, or `retire`.

A status page also requires:

```yaml
lastVerified: "2026-08-29"
evidence:
  - internal/capabilityprobe/cells.go
```

A fully generated page uses `generated: true` and requires both `generator` and
`editSource`. Do not edit its rendered frontmatter separately from its
generator.

Regenerate the factual inventory after any page, frontmatter, sidebar, or
internal-link change:

```bash
cd docs/site
npm run inventory:write
npm run check:content-inventory:selftest
npm run check:content-inventory
```

The generated file records routes, navigation paths, links, word and byte
counts, metadata, ownership, and freshness. Keep only non-derivable journey
findings and editorial decisions in `CONTENT_INVENTORY.md`.

## 4. Voice and language

- American English everywhere (`behavior`, `color`, `canceled`).
- Active voice. Second person ("you") for procedures; imperative for steps.
- Present tense for behavior: "Ptah writes", not "Ptah will write".
- Working targets: sentences at or under roughly 25 words; paragraphs at or
  under 4 sentences.
- No marketing adjectives. Never "simply", "easily", or "just".
- A paragraph that enumerates more than a couple of flags, formats, or failure
  modes wants a list or a table. `check:style` fails a paragraph over 900
  rendered characters, measured on the text a reader sees rather than on the
  markdown around it, so flag-dense prose is not penalized for its backticks.
- Expand an abbreviation on first use per page (DDL, RLS, OCI, UDT).
- "Ptah" is capitalized in prose; commands and file names are code-formatted
  (`ptah`, `ptah.sum`, `atlas.hcl`).
- Heading text uses sentence case ("Supported blocks", not "Supported
  Blocks"). Existing Title Case headings are corrected when a page is
  otherwise edited.

Sections 5 and 6 continue this one. Section 5 is about the words a sentence
uses; section 6 is about the tense it uses. Read all three together.

## 5. Plain English for a global audience

Ptah's readers are database and application engineers. Many of them do not read
English as a first language, and neither do many of the people who write the
next page. Every rule here exists so that a sentence reads correctly on one
pass.

### 5.1 Word choice

- Use the common, literal word. "Use", not "leverage". "Start", not "kick off".
  "Remove", not "get rid of".
- Use a direct verb where one exists. "The command reads the directory", not
  "the command goes on to read the directory".
- Leave out idioms, jokes, cultural references, and wordplay. They cost a
  reader a dictionary lookup and return nothing.
- No filler adjectives. The list is `powerful`, `seamless`, `seamlessly`,
  `robust`, `robustly`, `flexible`, `flexibly`, `comprehensive`,
  `comprehensively`, `enterprise-grade`, and `production-ready`. Each one
  praises a capability without naming one, so the sentence carrying it says
  nothing a reader can check. Enforced by `check:style`.
- `first-class` is not on that list. It names a modeling decision — a sequence
  is a first-class schema object — and a reader can check it against the code.

Replace the adjective with the behavior it stood in for. The replacement is
usually shorter, and it answers the question the adjective raised. The pair
below is from this repository's OCI reference, where the second line is the one
that ships.

Rather than:

> Its own discovery is robust whatever the registry does.

Write:

> Its own discovery works whether or not the registry answers that API.

### 5.2 Sentence and paragraph shape

- One idea per sentence. Split a sentence rather than joining two with a
  semicolon.
- Do not compress. A sentence that asks the reader to supply an omitted subject
  is shorter to write and slower to read.
- Keep em dashes down to what a reader can hold. A paragraph with four of them
  has more asides than clauses; promote one to its own sentence.
- No rhetorical question as a section introduction. A heading may be the
  question the reader is asking ("What do you need?"). The paragraph under it
  answers that question rather than asking another.

### 5.3 Constructions to leave out

- `This ensures that ...` as a closing sentence. State the guarantee where the
  behavior is described, once.
- `In other words, ...`. A first sentence that needs a restatement wants
  replacing, not supplementing.
- `not only X, but also Y` and `not X, but Y` past one use per page.
- `under the hood`, `out of the box`, `at the end of the day`, `it is worth
  noting that`, `it is important to note that`, `needless to say`. Each either
  names nothing or delays the clause that does.
- Anthropomorphism. A command does not know, want, think, or decide. It reads,
  refuses, writes, or exits. Denying anthropomorphism is allowed and sometimes
  necessary: "leaving a role out of a description does not mean Ptah treats it
  as missing" is correct, and it is why no checker reads this rule.
- Prose that repeats the table or code block below it. Introduce the block, do
  not paraphrase it.

### 5.4 What plain does not mean

Plain English is not telegraphic English. Keep the article, keep the subject,
and keep the clause that says why. A competent engineer who has never opened
this repository has to be able to read a page without reconstructing what was
left out.

Prefer:

> `ptah migrations up` applies pending migrations to a live database.

Rather than:

> Ptah provides a powerful and flexible mechanism that enables users to
> seamlessly apply pending database migrations in a predictable manner.

And rather than:

> Applies pending migrations, live database.

## 6. Timeless documentation

Reader-facing documentation describes how Ptah works now. A reader arriving
today has no earlier Ptah to compare against, and Ptah is pre-GA, so the earlier
behavior is not a surface anyone can still meet. Narrating it spends the
reader's attention on something that cannot happen to them.

### 6.1 The rule

Write the current behavior as a direct statement. Do not narrate the change that
produced it.

Rather than:

> Previously, the command returned raw SQL, but it now returns a structured
> result.

Write:

> The command returns a structured result containing the SQL and its metadata.

### 6.2 Wording that signals a narrative

These are the spellings this repository has produced. Treat each as a prompt to
reread the sentence rather than as a banned string: several have a legitimate
present-tense reading, and section 6.3 draws that line.

`formerly`, `originally`, `previously`, `used to`, `used to be`, `no longer`,
`now supports`, `the old behavior`, `the new implementation`, `in an earlier
version`, `in a previous version`, `this was added later`, `recently added`,
`the first phase`, `a later phase`, `legacy`.

None of them is machine-checked. Section 16 says why, with the measurement.

### 6.3 What the words in 6.2 also mean

The exception matters as much as the rule. Every word in section 6.2 has a
present-tense reading, and most of this repository's occurrences are that
reading. Read the **subject** of the sentence before rewriting it. A sentence is
not Ptah history when its subject is:

- **Another product or engine.** "A project using one is no longer a project
  Atlas reads" describes what Atlas reads. "Legacy Atlas rows without
  `partial_hashes`" names Atlas's own row format. "The legacy display width
  (`int` on MySQL, `int(11)` on MariaDB)" is MySQL's term. All three stay.
  Documenting Atlas compatibility means describing another product, and that
  product's past tense is not Ptah's.
- **A live runtime state.** "`ptah migrations up` refuses a directory whose
  files no longer match `ptah.sum`" is current behavior. So is "a failed retry
  cannot reduce the applied count below the previously committed prefix", where
  `previously` marks an earlier point inside one operation.
- **Ptah vocabulary that happens to contain the word.** `legacy-tested` is a
  declared support level that `ptah db capabilities` prints, and a legacy
  three-column revision table is a layout a live database can hold today.
- **Purpose rather than time.** `used to` meaning "employed in order to", as in
  "the URL used to read the current schema".

The same word on a Ptah subject is the violation. "`schema plan new`, `schema
plan validate` and `schema plan lint` are no longer stubs" narrates Ptah's
implementation progress and gets rewritten. So do "the generator used to publish
into that directory", "previously, the refusal was narrower", and "the legacy
flag name" for a spelling Ptah has stopped accepting.

Contributor and evidence pages are a separate exception, not a subject test:
section 6.4 names them.

### 6.4 Where history belongs

Historical material belongs in a changelog, a release note, an architecture
decision record under `docs/adr/`, contributor documentation, or an explicit
migration guide from another product or interface. Each of those exists to
record change, and none of them is in the reader sidebar.

Reader documentation does not celebrate internal refactoring.

### 6.5 Labels

A reader-facing label names the current state, not the decision that produced
it: "compatibility differences", not "retained divergences". The Atlas
compatibility page still carries the older label; moving it is part of the same
documentation rewrite as this rule, and the rule is what the new label answers
to.

A literal compatibility identifier a user has to type — a flag, an environment
variable, a file name — keeps the spelling the tree uses, appears in reference
documentation, and does not become part of Ptah's product model.

### 6.6 Accuracy outranks the greenfield reading

Do not make a page inaccurate to make it timeless. Where a public behavior
exists only as a compatibility surface, say what it does, open a product issue
to remove or rename it, and cite that issue in the same paragraph. An unowned
claim that a gap is tracked is itself a `check:style` failure (section 1).
Remove the explanation after the behavior changes, not before.

## 7. Ptah terminology

Canonical names. Do not introduce synonyms. This table is the registry: check a
term here before writing it, and read the **Held by** column. It says, per row,
whether a gate holds the tree to that name or a reviewer does — most rows are
held by review, and a row held by review is not a weaker rule, only an
unchecked one.

The table is generated from `docs/site/scripts/data/terminology.json`. Add or
change a term there and run
`node docs/site/scripts/check-terminology.mjs --write`; editing the table by
hand fails the check.

<!-- BEGIN GENERATED TERMINOLOGY -->
| Term | Meaning and usage rule | Held by |
| --- | --- | --- |
| native commands | The `ptah <verb>` tree. Never described with Atlas spellings; root-level Atlas aliases (`ptah migrate apply`) are documented as intentionally absent. | review |
| Atlas-compatible commands | The command tree of the separate `ptah-compat` binary. Invocations are spelled `ptah-compat <command> ...`, the name the binary ships under. The drop-in rename (installing the binary under the name `atlas`) is documented once, on the Atlas compatibility overview. | review |
| `ptah-compat` | The binary-level drop-in replacement for scripts expecting an Atlas-style executable. The only Atlas-compatible command surface; the native `ptah` binary has none. | review |
| desired schema | What the schema sources declare. Do not write "desired state" except inside the established compound for composite sources. | `check:terminology`, ratcheted |
| schema source | A Go-annotation tree, YAML file, HCL file, SQL file, external loader, or live database used as input. | review |
| composite desired schema | The merged result of multiple schema sources. | review |
| direct schema changes | The workflow that runs a computed difference against the database with no migration file in between: `ptah schema plan`, `ptah schema apply`, `ptah schema drift`. Never "declarative schema changes". `ptah migrations generate` reads the same desired schema, so "declarative" names where a change came from, not how it lands; what separates the two workflows is whether the difference runs now or is committed as a file first. | `check:terminology`; `terminologyguard` |
| versioned migrations | The workflow that records a change as ordered `*.up.sql`/`*.down.sql` files and replays them: the `ptah migrations` tree. The peer of direct schema changes, and the other half of every "which workflow" sentence. | review |
| migration directory | The versioned directory of `*.up.sql`/`*.down.sql` (or Atlas-format) files. | review |
| integrity file | `ptah.sum` (native) or `atlas.sum` (Atlas-format). | review |
| revision table | The database table recording applied migrations. | review |
| dev database | The replay target behind `--dev-url` (validate/lint/Atlas-compatible verbs). | review |
| shadow database | The verification replay target behind `--shadow-db` (generate, checkpoint, baseline). | review |
| throwaway database | The disposable database that `migrations test` and `schema test` run cases against. | review |
| dialect | A SQL rendering flavor (what Ptah generates). Distinct from the database/engine you connect to. | review |
| capability | A per-dialect feature gate. | review |
| drift | Divergence between the desired schema and a live database. | review |
| conformance | Atlas-compatibility evidence in `stokaro/ptah-atlas-conformance`. | review |
| clean-room implementation | Ptah does not use Atlas source code; only observable behavior is studied. | review |
| pre-GA | Ptah's current maturity: no legacy aliases, no compatibility wrappers, breaking cleanups allowed. | review |
<!-- END GENERATED TERMINOLOGY -->

Dev, shadow, and throwaway databases are three distinct things. Define each
once on its concept page and link; do not re-define them per page.

## 8. Code and examples

- Runnable over pseudocode. Every example starts from a stated state and is
  copy-pasteable.
- Fenced blocks always carry a language label: `bash`, `powershell`, `console`,
  `sql`, `yaml`, `hcl`, `go`, or `text` for output. In a page marked
  `quickstart: true`, a `console` block outside shell tabs is a command the
  acceptance runner executes in both Bash and PowerShell.
- Commands and expected output are separate blocks. Introduce checkable output
  with `Expected output ... on standard output:` or
  `Expected output ... on standard error:` so the acceptance runner can attach
  the assertion to the preceding command.
- Do not publish identical Bash and PowerShell tabs. Use one `console` block
  for a command that has the same bytes and semantics in both shells.
  `check:editorial-shape` fails byte-equivalent tab panels after whitespace
  normalization.
- Placeholders are environment-variable style (`"$DATABASE_URL"`). Never real
  credentials, hosts, or tokens.
- Prefer SQLite for examples that must run without a daemon.
- Tutorials end with cleanup.
- Multi-dialect or multi-source variants are colocated (consecutive labeled
  blocks or tabs) when they answer the same reader question.
- No `testify` in any Go snippet; use `quicktest` imported as `qt`, the
  standard `testing` package, or existing project helpers (repository rule,
  see `AGENTS.md`).
- Every changed example is executed against a locally built `ptah` before
  merge, or the PR states why it cannot be.
- Every top-level `examples/*` directory has a reader README with these exact
  sections: `What this example demonstrates`, `Prerequisites`, `Run`,
  `Expected result`, `Verify`, `Cleanup`, and `Learn more`.
  `docs/site/scripts/check-examples.mjs` validates that contract and generates
  `examples/README.md`; `scripts/check-examples.sh` executes or mechanically
  verifies each supported example.
- A tutorial that needs a service carries a disposable fixture and an
  acceptance script. The fixture states its ports, pins service versions,
  supplies stable input data, and removes its containers, network, volumes, and
  locally built images in one cleanup command.

## 9. Warnings and limitations

- `:::note` for context, `:::tip` for optional improvements, `:::caution` for
  foot-guns and operations near data risk, `:::danger` only for irreversible
  destructive operations.
- At most two admonitions per screen of content. A page that needs more is
  restructured, not decorated.
- Limitations live in a "Limitations" section near the end of the affected
  page, plus at most a one-line caution at the affected step. The happy path
  is never interleaved with status caveats.
- State unsupported behavior observably: "X fails with `<error>`", not "X is
  not supported yet" roadmap prose.

## 10. Tables and lists

- Tables are for comparison and lookup. A cell longer than about two rendered
  lines means the row needs a section with a heading instead. When a matrix
  needs that much prose per row, keep the table as an index of short cells that
  link to a section per row, and let the sections carry the detail.
- Wide matrices split by axis (per command group, per dialect) rather than
  scrolling horizontally.
- No table over five columns without an explicit review at mobile width.
- Lists over about seven items get subheadings or a table.
- A column whose cells are lists of code tokens squeezes its neighbors narrow.
  Two or three such columns usually want to be a bold-led list instead.
- Do not stop code spans in cells from wrapping. A cell can hold a
  ninety-character error string, and freezing it pushes the whole table past
  its container, where the right-hand columns are cut off with no hint that
  they exist.
- Escape a literal pipe inside a cell as `\|`, including inside a code span.
  An unescaped one splits the cell, and the renderer discards whatever no
  longer fits the header's column count without warning anyone.

Two limits are enforced: a cell over 350 characters fails `check:style`, and a
cell rendering over 8 lines at 1280px fails `check:responsive`. Both are well
above the two-line guidance above. They mark where a cell has stopped being a
cell, not where it stops being good.

## 11. Visual documentation

Where Ptah produces something visual, the page shows it. A rendered entity
diagram, a generated HTML document, and a running `ptah schema serve` are output
a reader recognizes on sight and cannot reconstruct from a paragraph.

### 11.1 When a page needs a visual

- A page documenting a command whose output is a picture shows that picture near
  the first invocation of the command. Raw Mermaid or DOT text may stay below it
  as reference; it is not the only representation.
- A page documenting a browser surface shows a screenshot of that surface.
- A page whose subject is a sequence of states — a migration lifecycle, a
  compare-plan-apply loop — carries a diagram of the sequence.
- A page documenting a trust or authorization boundary carries a diagram of the
  boundary.

### 11.2 Which medium

- Choose the medium for what the visual has to communicate. A schematic with
  authored labels is Mermaid, D2, Graphviz, or hand-authored accessible SVG,
  in that order of preference. Do not introduce a raster diagram containing
  authored text, including one produced by an image generator.
- PNG and WebP are reserved for real browser UI, photographs, or imagery that
  cannot reasonably be expressed as semantic vector shapes. Render a raster
  asset at enough resolution to stay sharp at its largest intended size and
  inspect it there.
- Every diagram works in both the light and dark theme. An SVG normally uses
  `currentColor` and the site's color tokens. A raster diagram uses transparency,
  theme-specific variants through `<picture>`, or a deliberate neutral
  background whose contrast has been checked in both themes.
- Screenshots are for visual and interactive output only. A terminal command and
  its output stay selectable text in fenced blocks, never a picture.
- Capture browser output at a fixed viewport, and record that width beside the
  command that generates the asset.

### 11.3 Alt text and text equivalence

- Every image and diagram carries alt text that says what the image shows, not
  what kind of thing it is. "Entity diagram: `users` has many `orders`, joined
  on `orders.user_id`", not "schema diagram". `check:style` fails an image with
  no alt text; whether the alt text is useful is a reading responsibility.
- An inline `<svg>` names itself through `role="img"` and either `aria-label` or
  an `aria-labelledby` pointing at its own `<title>`. Use `aria-labelledby` when
  the description is a sentence, so it is not duplicated into an attribute.
- No instruction, value, or limitation exists only inside an image. A reader
  using a screen reader, a reader on a slow link, and a reader reading the
  Markdown on GitHub all get the same information.

### 11.4 Assets are reproducible or traceable

- A visual that demonstrates Ptah's actual output comes from a committed fixture
  through a committed command. A reader must be able to reproduce the behavior
  the image claims.
- An illustrative, brand, or generative visual does not have to reproduce as
  identical bytes. Record its source in the repository or in the pull request
  that introduces it: the design file or prompt and references, the tool used,
  and any material post-processing. The record must let a later editor
  understand what they are changing; it does not have to make a stochastic
  generator deterministic.
- A screenshot contains no local path, credential, token, or personal host
  name. A time-bearing UI uses a fixed fixture timestamp, and a generated path
  is sanitized before capture.
- Review every page carrying a visual at 390px and 1280px, in both themes,
  before merge.
- `check-visual-assets.mjs` holds the allowed raster-output list, editable SVG
  metadata, source record, and useful-alt requirements. UI screenshots come
  from `generate-schema-ui-assets.mjs`; do not edit their pixels by hand.
- `check-visual-snapshots.mjs` captures selected desktop and mobile pages as a
  CI review artifact and fails on a missing visual or page overflow. The
  artifact is evidence for human review, not a cross-platform pixel baseline.

### 11.5 Tabs

- Use `<Tabs>` where two or more schema sources express the same intent. Do not
  build a tab set to show every format Ptah reads: a tab set whose panels are
  not equivalent misleads a reader who switches tabs expecting the same result.
- A tab set naming schema sources uses `syncKey="schema-source"`, so the
  reader's choice carries from page to page. A tab set on a different axis
  declares its own `syncKey` and reuses it wherever that axis appears.
- Components need `.mdx`. Converting a page from `.md` changes nothing a reader
  sees and everything about the file name, so it follows the move rules in
  section 12: redirect the old URL and update the inventory.

## 12. Links and sources of truth

- Each fact has exactly one canonical page. Other pages link to it and restate
  at most one sentence.
- Site-internal links are docs-relative with a trailing slash (enforced by
  `check-links.mjs`); never root-relative.
- Site pages never link to root docs on GitHub and never mention the protected
  `docs/*.md` paths (enforced by `check-core-doc-links.mjs`).
- External links go only to stable external pages: Atlas documentation for
  Atlas's own claims, the conformance repository for evidence.
- "Next steps" sections contain at most three links, each phrased as a
  decision, not a generic list.
- Moved pages always get a redirect entry; content links always point at the
  new home directly, never through a redirect. `check:redirects` validates the
  entries that exist; `check:route-retirement` is what notices the one that does
  not, by comparing the routes the site publishes today against
  `docs/site/scripts/data/published-routes.json`, the record of every route it
  has published. Measured: a page renamed with no redirect passes every other
  gate in this list and 404s on the built site.
- A page's file name is its URL, so it is lowercase, and uses only
  `[a-z0-9-]`. Astro puts each path segment through github-slugger, and the
  route gates model only the segments that survive it unchanged: anything else
  is refused by name rather than guessed at. A frontmatter `slug:` is honored
  where a page genuinely has to move without moving its file, and a basename
  starting with `_` is a partial that publishes nothing.
- Every page is named by a sidebar entry in `docs/site/src/sidebar.mjs`, and
  every entry names something that exists: a `slug:` names a page, an internal
  `link:` names a route the site publishes or a redirect it declares. Both
  directions are enforced by `check-page-health.mjs`.
- A top-level sidebar group is a disclosure control, never a link. Its first
  child is a page with `type: landing`, usually labeled `Overview`. Use a more
  specific child label when `Overview` would hide the reader's decision. The
  parent breadcrumb links to that explicit landing; a nested group without its
  own landing remains plain text. `check-navigation.mjs` enforces the structural
  rule and renders every top-level parent breadcrumb.
- `reference/exit-codes` is the canonical page for exit codes, and
  `check-exit-codes.mjs` holds it in lockstep with the codes the binaries
  return. Document an exit code there and link to it, rather than restating the
  number.
- Product terms belong in `src/glossary.ts`. The glossary page renders those
  definitions and dense tables may reuse them through `GlossaryTerm`; do not
  write a second local definition that can drift.
- Generated command and flag rows remain generator-owned. The
  `.ptah-reference-filter` affordance filters rendered rows only; it must never
  replace, truncate, or hand-copy the exhaustive inventory.

## 13. Accessibility, readability, and mechanics

- One `h1` per page (the frontmatter title). Headings descend without
  skipping levels.
- The site shell may use the full `70rem` content width for code, diagrams,
  generated matrices, and wide tables. Paragraphs, lists, blockquotes,
  admonitions, and ordinary headings stop at `40rem`, which renders near a
  72--80-character reading measure in the site typeface. Do not widen prose to
  make one reference table fit.
- A Markdown table or code block already uses the wide page measure. Give a
  custom visual or component `.ptah-wide-content`; use `.ptah-wide-table` when
  a table must preserve its desktop column widths and scroll locally on mobile.
  `check:responsive` measures the prose cap and rejects document-level overflow.
- Descriptive link text; never "here" or "this page".
- Alt text on every image and diagram: section 11.3 states the rule and
  `check:style` enforces its presence.
- Frontmatter requires `title` and `description` (enforced by
  `check-page-health.mjs`).
- Structural changes get a visual review at desktop and mobile widths before
  merge.
- Page actions appear in this order: Copy page as Markdown, Edit this page,
  View source, Report a documentation issue. Assistant destinations require
  measured use before they displace those actions.
- `lastVerified` is the date a status claim's evidence was checked. The footer's
  Git-derived Last updated date records when the page source changed. Do not use
  one as a substitute for the other.
- `searchAliases` hold reader language that the title and visible prose do not
  already cover. Do not copy the page title into the aliases. The search gate
  requires each canonical acceptance page to rank in Pagefind's first three
  results for its recorded query.

## 14. Anti-slop rules

Forbidden in all Ptah documentation:

- Generic filler introductions ("In today's fast-paced world of databases...").
- Meta introductions that start with "This page" or "This guide" when a reader
  situation, outcome, or precise lookup scope can state the same thing directly.
- Restating a command name as its description ("`ptah migrations up` runs
  migrations up").
- Summary sections that repeat the page.
- Vague benefit prose. Section 5.1 carries the list and `check:style` enforces
  it; this entry exists so the anti-slop reading finds it too.
- Fabricated example output; output shown must come from a real run.
- Unverified support or parity claims.
- Caveat-stuffing: burying the procedure under status qualifiers.
- Enumerating every flag in a workflow page; that is the reference's job.
- Work-in-progress markers (`TODO`, `TBD`, `FIXME`, "coming soon") — already
  rejected by `check-page-health.mjs`.

`check:editorial-shape` warns on long pages, mixed page-type signals, generic
first paragraphs, and near-duplicate long paragraphs. These are review prompts,
not arbitrary merge failures. A deliberate finding belongs in
`scripts/data/editorial-waivers.json` with a route, check name, and specific
reason. The check fails when a waiver names no live finding, so exemptions must
be removed after the page changes.

## 15. Review checklist

Complete this for every documentation PR:

1. Page type declared for each touched page and its template followed
   (section 2–3).
2. Terminology matches section 7; no retired synonyms introduced. Check the
   term in the table before writing it, and read its **Held by** column: a row
   naming a gate is checked in CI, and a row saying `review` is checked here
   and nowhere else.
3. Examples executed against a built binary; shown output is current.
4. Parity and conformance claims checked against current
   `stokaro/ptah-atlas-conformance` reports.
5. `npm run check:links:selftest && npm run check:links &&
   npm run check:redirects:selftest && npm run check:redirects &&
   npm run check:route-retirement:selftest && npm run check:route-retirement &&
   npm run check:core-doc-links &&
   npm run check:page-health:selftest && npm run check:page-health &&
   npm run check:content-inventory:selftest &&
   npm run check:content-inventory &&
   npm run check:editorial-shape:selftest &&
   npm run check:editorial-shape &&
   npm run check:support-matrix:selftest && npm run check:support-matrix &&
   npm run check:exit-codes:selftest && npm run check:exit-codes &&
   npm run check:style:selftest && npm run check:style &&
   npm run check:terminology:selftest && npm run check:terminology &&
   npm run check:limitations:selftest && npm run check:limitations &&
   npm run check:responsive:selftest && npm run check:accessibility:selftest &&
   npm run check:visual-snapshots:selftest && npm run check:navigation:selftest &&
   npm run check:search-ranking:selftest && npm run versions:selftest &&
   npm run build && npm run check:responsive && npm run check:accessibility &&
   npm run check:visual-snapshots -- --output /tmp/ptah-docs-snapshots &&
   npm run check:glossary:selftest && npm run check:glossary &&
   npm run check:navigation && npm run check:search-ranking`
   all pass in `docs/site`. `check:responsive` and `check:glossary` read the
   built site, so they run last. Run every `:selftest` alongside its check: a
   check whose self-test is failing is not reporting on your content.
6. `docs/site/content-inventory.json` regenerated for any page, metadata,
   sidebar, or internal-link change. Update `CONTENT_INVENTORY.md` only when a
   journey or editorial decision changes.
7. Redirects added for every moved URL; no content links through a redirect. A
   new page joins `published-routes.json` in the same PR, through
   `node scripts/check-route-retirement.mjs --write`. A line is never removed
   from that file by hand: `--forget <route> --against <ref>` is the one way,
   and it works only for a route the merge base never recorded.
8. Desktop and mobile visual pass for structural changes. `check:responsive`
   covers horizontal overflow at 390px and 1280px; judgment about hierarchy,
   density, and orientation still needs a person looking at the page.
9. Stale-term sweep: `rg` for old slugs, old command spellings, and retired
   terms across `*.md`.
10. American English pass: spelling, active voice, sentence-case headings.
    `check-style.mjs` covers the spelling deny-list; active voice and heading
    case still need a human reading.
11. Plain-English pass (section 5): read every changed paragraph for idioms,
    rhetorical questions, anthropomorphism, and prose that repeats the block
    below it. No checker reads any of those.
12. Timeless pass (section 6): no sentence narrates Ptah's own evolution. Where
    a page says `no longer`, `legacy`, `previously`, or `used to`, read the
    subject: a statement about another product's behavior or about a live
    runtime state stays (section 6.3).
13. Visual pass (section 11): every image has alt text a reader can use, its
    medium suits what it communicates, its regeneration command or source is
    recorded as section 11.4 requires, and no essential instruction lives only
    inside an image.

## 16. What is machine-enforced

These rules fail CI, so a PR cannot merge while breaking them. Everything else
in this guide is a review responsibility.

| Rule | Section | Check |
| --- | --- | --- |
| American English spelling | 4 | `check:style` |
| No `simply` / `easily` / `just` | 4, 14 | `check:style` |
| No filler adjectives | 5.1, 14 | `check:style` |
| No unowned tracking claim | 1 | `check:style` |
| Fenced blocks carry a language label | 8 | `check:style` |
| Every fenced block is closed | 8 | `check:style` |
| Admonitions limited to note/tip/caution/danger | 9 | `check:style` |
| No `testify` in code samples | 8 | `check:style` |
| A retired spelling on a section 7 row whose **Held by** names a gate | 7 | `check:terminology` |
| Section 7's table matches the registry it is generated from | 7 | `check:terminology` |
| Native help text obeys the section 7 rows `terminologyguard` holds | 7 | `cmd/internal/terminologyguard` |
| Every image carries alt text | 11.3 | `check:style` |
| `title` and `description` frontmatter | 13 | `check:page-health` |
| Page type, audience, reader question, goal, source, generated state, overlaps, and disposition | 3.1 | Astro content schema; `check:content-inventory` |
| Status verification date and evidence | 3.1 | Astro content schema; `check:content-inventory` |
| Generated page source metadata | 3.1 | Astro content schema; `check:content-inventory` |
| Factual page inventory matches content, sidebar, and link graph | 3.1 | `check:content-inventory` |
| Identical tab panels are not published | 8 | `check:editorial-shape` |
| Editorial waivers name a live route and current finding | 13, 14 | `check:editorial-shape` |
| Release-line support counts and classifications appear only in generated blocks | 12 | `check:support-matrix` |
| Every page is named by a sidebar entry | 12 | `check:page-health` |
| Every sidebar entry names a page or a route | 12 | `check:page-health` |
| Every top-level group starts with a landing page | 12 | `check:navigation` |
| Landing-backed breadcrumb ancestors are links | 12, 13 | `check:navigation` |
| Page-action order and keyboard dismissal | 13 | `check:navigation` |
| Canonical search pages rank in the top three | 13 | `check:search-ranking` |
| No `TODO`/`TBD`/`FIXME`/"coming soon" | 14 | `check:page-health` |
| Site-internal links resolve | 12 | `check:links` |
| A declared redirect is well formed | 12 | `check:redirects` |
| A retired URL keeps a redirect | 12 | `check:route-retirement` |
| A published route stays recorded | 12 | `check:route-retirement` |
| A file name Astro would re-spell | 12 | every route gate, through `docroutes` |
| Site pages never link protected root docs | 12 | `check:core-doc-links` |
| Exit-code tables stay in lockstep | 12 | `check:exit-codes` |
| Table cells under 350 characters | 10 | `check:style` |
| Table rows match the header's column count | 10 | `check:style` |
| Two rows never state different verdicts for one capability | 10 | `check:style` |
| No bare `--flag` outside a code span on site pages | 4, 8 | `check:style` |
| Paragraphs under 900 rendered characters | 4 | `check:style` |
| In-page and cross-page anchors resolve | 12 | `check:links` |
| Command and flag reference filters work with the keyboard | 3, 13 | `check:accessibility` |
| No page scrolls sideways at 390px or 1280px | 13 | `check:responsive` |
| Allowed raster assets, editable visual source, and useful alt text | 11 | `check:visual-assets` |
| Selected visual pages render at desktop and mobile widths | 11, 13 | `check:visual-snapshots` |
| Representative pages pass WCAG A/AA and keyboard interaction checks | 13 | `check:accessibility` |
| Every top-level example has the reader contract and generated index | 8 | `check:examples` |
| Supported examples execute or pass their declared mechanical checks | 8 | `scripts/check-examples.sh` |
| The inference quick start reaches a verified active generation | 8 | `check-inference-quick-start.sh` |
| No table cell renders over 8 lines at 1280px | 10, 13 | `check:responsive` |
| No table is wider than its container at 1280px | 10, 13 | `check:responsive` |

`check:style` governs every layer the guide covers — `docs/site`, `docs/*.md`,
`examples/**`, `integration/*.md`, every package `README.md`, and `AGENTS.md` —
not only the site. Package READMEs are discovered by walking the repository, so
a new package cannot opt out by existing. This file is the one exemption: it
necessarily contains the words it bans.

Section 7 is a generated rendering of
`docs/site/scripts/data/terminology.json`, and the registry is what both
checkers read: `check:terminology` for Markdown prose, and
`cmd/internal/terminologyguard` for the native command tree's help text, in
`go test ./...` rather than in this workflow, because a Go-only change does not
trigger it.

Most of section 7 is held by review, and the table's **Held by** column says so
per row rather than leaving a reader to infer it from these three entries. Such
a row carries no machine-checkable rule at all: `native commands` asks that the
tree is never described with Atlas spellings, and no checker can tell an Atlas
spelling from a sentence about one. Those rows are not a backlog working
towards zero — they are names reviewers hold each other to, which is checklist
item 2 and nothing else.

A row whose **Held by** names `check:terminology` is enforced over the corpus
below, and one that also names `terminologyguard` is enforced over the native
command tree's help text as well. A row that adds `ratcheted` is one whose
backlog has not reached zero: its count is recorded in the registry, can shrink
and cannot grow, and it becomes a plain gate in the change that clears it.
`desired schema` is the row in that state today.

`check:terminology` derives its corpus rather than listing it — every tracked
Markdown file, plus the site sources that carry reader-facing text without
being Markdown, such as the sidebar's labels. A new page or package README is
governed by existing, the same property `check:style` has and for the same
reason.

The table lists the rules this guide states. The documentation job also runs
`check:limitations`, `check:glossary`, and the four checks that hold
`atlas/feature-matrix.md` to its source data — `check-matrix-verdict-prose`,
`check-matrix-citations`, `check-matrix-flag-names`, and
`build-feature-matrix --check`. They govern named pages rather than prose in
general, and `AGENTS.md` carries the command list a contributor runs. A green
run of the table above is not a green documentation job.

Two properties keep these checks honest:

- **They run for every layer they claim.** The Docs workflow builds the site
  only for `docs/site/**` changes, so `check:style` has its own job that also
  triggers on `docs/**`, `examples/**`, `integration/**`, any `README.md`, and
  `AGENTS.md`. A change to a package README cannot merge unchecked.
- **The self-tests drive the real checker.** `check:style:selftest` and
  `check:responsive:selftest` call the same functions the checks call, on
  fixtures that must produce findings and fixtures that must not. Deleting a
  rule fails its self-test rather than passing every file forever.

Prose rules never read code, and code rules never read prose. A column may be
named `cancelled`, and this guide names `testify` in order to ban it; neither is
a violation. An HTML or JSX attribute *name* is markup rather than prose, which
is why `aria-labelledby` passes the spelling rule: it is the ARIA spelling and
has no American variant. A British spelling inside an attribute *value* is text
a reader meets and is still reported.

The alt-text rule reads Markdown images in both spellings and a single-line
`<img>` tag. A tag spread over several lines is not read, because `alt` would
sit on a line of its own and a per-line rule would report a tag that has alt
text. Keep an `<img>` on one line, or check its alt text by reading.

Three rules guard density. Two cover tables, because one measurement cannot see
both failures; the third covers the prose between them, since an enumeration
does not stop being unreadable by leaving the grid. `check:style` counts characters, which is fast and needs no browser.
`check:responsive` measures rendered height, which is the only way to catch a
short cell squeezed into a column that an unbreakable code token has made
narrow. A page whose dense cells are the reference content can be named in the
allowlist in `check-responsive.mjs`, with the reason; the check then fails if
that page stops having dense cells, so an exemption cannot outlive its reason.

Anchor links are checked against the ids Starlight generates, so renaming a
heading fails the build instead of silently dropping every reader who follows
a link into it at the top of the page.

`check:editorial-shape` separates objective defects from editorial judgment.
Identical tab panels and invalid or stale waivers fail. Page length, mixed-type
signals, formulaic openings, and near-duplicate paragraphs produce review
warnings unless a current, reasoned waiver records why the page stays as it is.

`check:responsive` needs a browser. Without one it skips locally with an install
hint, and fails in CI — a green check that rendered nothing is worse than a red
one.

### 16.1 What sections 5, 6, and 11 leave to review

Sections 5, 6, and 11 are mostly a reading responsibility. Which parts are not
is measured rather than assumed.

**Section 5 (plain English).** One rule is enforced: the filler-adjective list.
Before it was added, `check:style` reported 15 findings across 9 governed files,
one of them on a site page; this change rewrote all 15, so the rule lands with
the tree clean. Everything else in section 5 — word choice, sentence shape,
idioms, rhetorical questions, anthropomorphism, prose that repeats the block
below it — needs a reader. The clearest case is anthropomorphism: the site says
"does not mean Ptah thinks it is missing", which *denies* the anthropomorphism,
and a rule that flagged it would be worse than no rule.

**Section 6 (timeless documentation) is enforced by nobody, deliberately, and
that is the decision most worth writing down.** Two separate measurements say so.

The broad words carry a present-tense reading more often than not. Counted in
prose across the governed files, with the site subset beside it:

| Word, in prose | Governed hits | Site hits | Why a deny-list fails |
| --- | --- | --- | --- |
| `no longer` | 46 | 28 | Most name a file, directory, column, or database that no longer matches something. That is current behavior. |
| `used to` | 36 | 17 | Includes the purpose sense, as in "the URL used to read the current schema". |
| `legacy` | 25 | 11 | Every site hit is current state: the `legacy-tested` support level `ptah db capabilities` prints, legacy Atlas revision rows, a legacy three-column revision table. |
| `previously` | 10 | 4 | Includes "the previously committed prefix", a point inside one operation. |

Reading every one of those hits across the reader-facing pages, the README, the
example READMEs, and the integration docs put 59 in the rewrite pile and 83 in
the keep pile: 17 describe Atlas or a database engine, 18 sit in conformance
evidence and design records where history belongs, and 48 are the present-tense
readings above. A deny-list would report all 142.

Three of the narrow phrases are enforced. `now supports`, `recently added` and
`a later phase` have no present-tense reading at all — a release note however it
is read, an adjective with no stable meaning, and a promise with no owner — and
`check-implementation-chronology.mjs` reports them over every Markdown file in
the tree, on the prose stream `check-terminology.mjs` builds, so a code span or
a fenced block is not a finding. Its self-test carries the clean-fixture shapes
this section named: an Atlas subject, a runtime state, `legacy-tested`, and the
purpose sense of `used to`.

Four paths are exempt, each because the past is its subject rather than an
intrusion: `docs/adr/**` is a design record, `docs/conformance.md` is a dated
measurement, `atlas/retained-divergences.md` is about which divergences closed,
and this file has to spell the phrases it governs. The exemption is by path
because "is the subject Ptah's own roadmap" is not a question a regular
expression answers, and the gate says so where the list is declared.

The rest of section 6 stays unenforced for the reason above and is item 12 of
the review checklist.

**Section 11 (visual documentation).** Alt presence, the allowed raster list,
editable source records, named SVGs, and selected rendered page shapes are
enforced. Whether a diagram teaches the right idea, whether the alt text is the
best equivalent, and whether a screenshot shows the clearest state remain
review responsibilities. `check:responsive` measures every page at 390px and
1280px; `check:accessibility` separately runs axe and keyboard interaction on
representative page shapes.
