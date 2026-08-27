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

- `docs/site/CONTENT_INVENTORY.md` — the page inventory, reader journeys,
  target navigation, and the inventory maintenance rule.
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
| Tutorial | Learning by doing: one happy path, guaranteed end state, cleanup | `start/quick-start` |
| Concept | Why and when: mental model, minimal commands | license boundary page |
| How-to | Goal-directed steps for a competent reader; variants and failure modes allowed | `versioned/checkpoints` |
| Reference | Exhaustive and scannable; no narrative | `reference/exit-codes` |
| Troubleshooting | Symptom-keyed diagnosis and fixes | `operate/troubleshooting` |
| Compatibility/status | Evidence-dated claims about parity or coverage | `atlas/conformance` |
| Contributor | Repo-layer docs, out of reader navigation | `docs/system_design.md` |

Mixing rule: a how-to may open with at most two short concept paragraphs;
anything longer moves to a concept page and is linked. Reference pages never
teach; tutorials never enumerate flags. A page that needs two types is two
pages.

## 3. Page templates

Required section flow per type. Optional sections are marked.

**Tutorial**: goal → prerequisites → numbered steps (each step: command,
expected output, one sentence of why) → verification → cleanup → next steps.

**Concept**: the idea in one paragraph → how Ptah models it →
consequences and tradeoffs → where it appears (links).

**How-to**: goal sentence + prerequisites → starting state → steps with
expected output → verification → failure modes (or a link to troubleshooting)
→ limitations (optional; only those affecting this task) → next steps (at most
three links, each phrased as the next decision).

**Reference**: one-sentence scope → tables or lists → exact behavior notes →
links to the owning workflow pages.

**Troubleshooting entry**: symptom as the heading, in the reader's words
(error text or observed behavior) → likely cause → diagnosis command → fix →
verify.

**Compatibility/status**: claim scope → evidence date and source → matrix →
how to re-verify.

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
it: "compatibility differences", not "retained divergences". A literal
compatibility identifier a user has to type — a flag, an environment variable, a
file name — keeps the spelling the tree uses, appears in reference
documentation, and does not become part of Ptah's product model.

### 6.6 Accuracy outranks the greenfield reading

Do not make a page inaccurate to make it timeless. Where a public behavior
exists only as a compatibility surface, say what it does, open a product issue
to remove or rename it, and cite that issue in the same paragraph. An unowned
claim that a gap is tracked is itself a `check:style` failure (section 1).
Remove the explanation after the behavior changes, not before.

## 7. Ptah terminology

Canonical names. Do not introduce synonyms.

| Term | Meaning and usage rule |
| --- | --- |
| native commands | The `ptah <verb>` tree. Never described with Atlas spellings; root-level Atlas aliases (`ptah migrate apply`) are documented as intentionally absent. |
| Atlas-compatible commands | The command tree of the separate `ptah-compat` binary. Invocations are spelled `ptah-compat <command> ...`, the name the binary ships under. The drop-in rename (installing the binary under the name `atlas`) is documented once, on the Atlas compatibility overview. |
| `ptah-compat` | The binary-level drop-in replacement for scripts expecting an Atlas-style executable. The only Atlas-compatible command surface; the native `ptah` binary has none. |
| desired schema | What the schema sources declare. Do not write "desired state" except inside the established compound for composite sources. |
| schema source | A Go-annotation tree, YAML file, HCL file, SQL file, external loader, or live database used as input. |
| composite desired schema | The merged result of multiple schema sources. |
| migration directory | The versioned directory of `*.up.sql`/`*.down.sql` (or Atlas-format) files. |
| integrity file | `ptah.sum` (native) or `atlas.sum` (Atlas-format). |
| revision table | The database table recording applied migrations. |
| dev database | The replay target behind `--dev-url` (validate/lint/Atlas-compatible verbs). |
| shadow database | The verification replay target behind `--shadow-db` (generate, checkpoint, baseline). |
| throwaway database | The disposable database that `migrations test` and `schema test` run cases against. |
| dialect | A SQL rendering flavor (what Ptah generates). Distinct from the database/engine you connect to. |
| capability | A per-dialect feature gate. |
| drift | Divergence between the desired schema and a live database. |
| conformance | Atlas-compatibility evidence in `stokaro/ptah-atlas-conformance`. |
| clean-room implementation | Ptah does not use Atlas source code; only observable behavior is studied. |
| pre-GA | Ptah's current maturity: no legacy aliases, no compatibility wrappers, breaking cleanups allowed. |

Dev, shadow, and throwaway databases are three distinct things. Define each
once on its concept page and link; do not re-define them per page.

## 8. Code and examples

- Runnable over pseudocode. Every example starts from a stated state and is
  copy-pasteable.
- Fenced blocks always carry a language label: `bash`, `sql`, `yaml`, `hcl`,
  `go`, or `text` for output.
- Commands and expected output are separate blocks. Introduce output with
  "Expected output includes".
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

- Diagrams are SVG, inline or as a committed file. Do not ship a diagram as a
  raster image: it cannot be read at another zoom level and it cannot follow the
  reader's theme.
- A diagram uses `currentColor` and the site's own color tokens rather than
  fixed hex values, so it renders in both the light and dark theme.
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

### 11.4 Assets are generated, never hand-drawn

- Every committed visual asset comes from a committed fixture through a
  committed command, and that command is recorded next to the asset.
- A screenshot contains no local path, credential, token, host name, timestamp,
  or other value that changes between runs. Regenerating an asset from the same
  fixture produces the same bytes.
- Review every page carrying a visual at 390px and 1280px before merge.

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
  new home directly, never through a redirect.
- Every reader page is registered in the site sidebar (`astro.config.mjs`). An
  unregistered page is unreachable by navigation, so `check-page-health.mjs`
  fails it.
- `reference/exit-codes` is the canonical page for exit codes, and
  `check-exit-codes.mjs` holds it in lockstep with the codes the binaries
  return. Document an exit code there and link to it, rather than restating the
  number.

## 13. Accessibility, readability, and mechanics

- One `h1` per page (the frontmatter title). Headings descend without
  skipping levels.
- Descriptive link text; never "here" or "this page".
- Alt text on every image and diagram: section 11.3 states the rule and
  `check:style` enforces its presence.
- Frontmatter requires `title` and `description` (enforced by
  `check-page-health.mjs`).
- Structural changes get a visual review at desktop and mobile widths before
  merge.

## 14. Anti-slop rules

Forbidden in all Ptah documentation:

- Generic filler introductions ("In today's fast-paced world of databases...").
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

## 15. Review checklist

Complete this for every documentation PR:

1. Page type declared for each touched page and its template followed
   (section 2–3).
2. Terminology matches section 7; no retired synonyms introduced.
3. Examples executed against a built binary; shown output is current.
4. Parity and conformance claims checked against current
   `stokaro/ptah-atlas-conformance` reports.
5. `npm run check:links:selftest && npm run check:links &&
   npm run check:redirects:selftest && npm run check:redirects &&
   npm run check:core-doc-links && npm run check:page-health &&
   npm run check:exit-codes:selftest && npm run check:exit-codes &&
   npm run check:style:selftest && npm run check:style &&
   npm run check:limitations:selftest && npm run check:limitations &&
   npm run check:responsive:selftest && npm run versions:selftest &&
   npm run build && npm run check:responsive &&
   npm run check:glossary:selftest && npm run check:glossary`
   all pass in `docs/site`. `check:responsive` and `check:glossary` read the
   built site, so they run last. Run every `:selftest` alongside its check: a
   check whose self-test is failing is not reporting on your content.
6. `docs/site/CONTENT_INVENTORY.md` updated for any added, moved, merged,
   split, or retired page.
7. Redirects added for every moved URL; no content links through a redirect.
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
13. Visual pass (section 11): every image has alt text a reader can use, every
    committed asset names the command that regenerates it, and no essential
    instruction lives only inside an image.

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
| Every image carries alt text | 11.3 | `check:style` |
| `title` and `description` frontmatter | 13 | `check:page-health` |
| Page registered in the sidebar | 12 | `check:page-health` |
| No `TODO`/`TBD`/`FIXME`/"coming soon" | 14 | `check:page-health` |
| Site-internal links resolve | 12 | `check:links` |
| Moved URLs keep a redirect | 12 | `check:redirects` |
| Site pages never link protected root docs | 12 | `check:core-doc-links` |
| Exit-code tables stay in lockstep | 12 | `check:exit-codes` |
| Table cells under 350 characters | 10 | `check:style` |
| Table rows match the header's column count | 10 | `check:style` |
| Two rows never state different verdicts for one capability | 10 | `check:style` |
| No bare `--flag` outside a code span on site pages | 4, 8 | `check:style` |
| Paragraphs under 900 rendered characters | 4 | `check:style` |
| In-page and cross-page anchors resolve | 12 | `check:links` |
| No page scrolls sideways at 390px or 1280px | 13 | `check:responsive` |
| No table cell renders over 8 lines at 1280px | 10, 13 | `check:responsive` |
| No table is wider than its container at 1280px | 10, 13 | `check:responsive` |

`check:style` governs every layer the guide covers — `docs/site`, `docs/*.md`,
`examples/**`, `integration/*.md`, every package `README.md`, and `AGENTS.md` —
not only the site. Package READMEs are discovered by walking the repository, so
a new package cannot opt out by existing. This file is the one exemption: it
necessarily contains the words it bans.

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

### 16.1 What sections 5, 6, and 11 leave to review

Two of the three new sections are mostly a reading responsibility, and the
reason is measured rather than assumed.

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

The narrow phrases that have no present-tense reading do better and still do not
pass. `formerly`, `used to be`, `now supports`, `recently added`, `in an earlier
version`, `in a previous version`, `the old behavior`, `the new implementation`,
`this was added later`, `the first phase`, and `a later phase` report 16 hits,
12 of them on site pages. Those 12 are genuine, so the rule is not wrong; it is
early. Adding it before those pages are rewritten turns every unrelated
documentation change red. Rewrite the pages, then add the rule, and put its
clean-fixture shapes — an Atlas subject, a runtime state, `legacy-tested`, and
the purpose sense of `used to` — in the self-test in the same change.

Until then, section 6 is item 12 of the review checklist.

**Section 11 (visual documentation).** Presence of alt text is enforced.
Whether the alt text says what the image shows, whether an asset was generated
from a committed fixture, whether a screenshot leaked a local path, and whether
a page that should carry a diagram has one are all read, not measured.
`check:responsive` measures layout at 390px and 1280px; it performs no
accessibility check.

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

`check:responsive` needs a browser. Without one it skips locally with an install
hint, and fails in CI — a green check that rendered nothing is worse than a red
one.
