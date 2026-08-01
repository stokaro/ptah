# Ptah documentation style guide

This is the authoritative style guide for all Ptah documentation: the site
under `docs/site/src/content/docs/`, the repository docs under `docs/*.md`,
package READMEs, example READMEs, integration docs, and `AGENTS.md` itself.
Contributor guidance (`AGENTS.md`) and the documentation-maintenance skill
(`.agents/skills/ptah-documentation-maintenance/SKILL.md`) require it. Section
13 lists which of its rules CI enforces.

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

## 5. Ptah terminology

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

## 6. Code and examples

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

## 7. Warnings and limitations

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

## 8. Tables and lists

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

## 9. Links and sources of truth

- Each fact has exactly one canonical page. Other pages link to it and restate
  at most one sentence.
- Site-internal links are docs-relative with a trailing slash (enforced by
  `check-links.mjs`); never root-relative.
- Site pages never link to root docs on GitHub and never mention the protected
  `docs/*.md` paths (enforced by `check-core-doc-links.mjs`).
- External links go only to stable upstream pages: Atlas documentation for
  Atlas's own claims, the conformance repository for evidence.
- "Next steps" sections contain at most three links, each phrased as a
  decision, not a generic list.
- Moved pages always get a redirect entry; content links always point at the
  new home directly, never through a redirect.

## 10. Accessibility, readability, and mechanics

- One `h1` per page (the frontmatter title). Headings descend without
  skipping levels.
- Descriptive link text; never "here" or "this page".
- Alt text on every image and diagram.
- Frontmatter requires `title` and `description` (enforced by
  `check-page-health.mjs`).
- Structural changes get a visual review at desktop and mobile widths before
  merge.

## 11. Anti-slop rules

Forbidden in all Ptah documentation:

- Generic filler introductions ("In today's fast-paced world of databases...").
- Restating a command name as its description ("`ptah migrations up` runs
  migrations up").
- Summary sections that repeat the page.
- Vague benefit prose ("powerful", "seamless", "robust").
- Fabricated example output; output shown must come from a real run.
- Unverified support or parity claims.
- Caveat-stuffing: burying the procedure under status qualifiers.
- Enumerating every flag in a workflow page; that is the reference's job.
- Work-in-progress markers (`TODO`, `TBD`, `FIXME`, "coming soon") — already
  rejected by `check-page-health.mjs`.

## 12. Review checklist

Complete this for every documentation PR:

1. Page type declared for each touched page and its template followed
   (section 2–3).
2. Terminology matches section 5; no retired synonyms introduced.
3. Examples executed against a built binary; shown output is current.
4. Parity and conformance claims checked against current
   `stokaro/ptah-atlas-conformance` reports.
5. `npm run check:links:selftest && npm run check:links &&
   npm run check:redirects:selftest && npm run check:redirects &&
   npm run check:core-doc-links && npm run check:page-health &&
   npm run check:exit-codes && npm run check:style:selftest &&
   npm run check:style && npm run check:responsive:selftest &&
   npm run versions:selftest && npm run build && npm run check:responsive`
   all pass in `docs/site`. `check:responsive` needs the built site, so it runs
   last. Run every `:selftest` alongside its check: a check whose self-test is
   failing is not reporting on your content.
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

## 13. What is machine-enforced

These rules fail CI, so a PR cannot merge while breaking them. Everything else
in this guide is a review responsibility.

| Rule | Section | Check |
| --- | --- | --- |
| American English spelling | 4 | `check:style` |
| No `simply` / `easily` / `just` | 4, 11 | `check:style` |
| Fenced blocks carry a language label | 6 | `check:style` |
| Admonitions limited to note/tip/caution/danger | 7 | `check:style` |
| No `testify` in code samples | 6 | `check:style` |
| `title` and `description` frontmatter | 3 | `check:page-health` |
| Page registered in the sidebar | 9 | `check:page-health` |
| No `TODO`/`TBD`/`FIXME`/"coming soon" | 11 | `check:page-health` |
| Site-internal links resolve | 9 | `check:links` |
| Moved URLs keep a redirect | 9 | `check:redirects` |
| Site pages never link protected root docs | 9 | `check:core-doc-links` |
| Exit-code tables stay in lockstep | 9 | `check:exit-codes` |
| Table cells under 350 characters | 8 | `check:style` |
| Table rows match the header's column count | 8 | `check:style` |
| Two rows never state different verdicts for one capability | 8 | `check:style` |
| Paragraphs under 900 rendered characters | 4 | `check:style` |
| In-page and cross-page anchors resolve | 9 | `check:links` |
| No page scrolls sideways at 390px or 1280px | 10 | `check:responsive` |
| No table cell renders over 8 lines at 1280px | 8, 10 | `check:responsive` |
| No table is wider than its container at 1280px | 8, 10 | `check:responsive` |

`check:style` governs every layer the guide covers — `docs/site`, `docs/*.md`,
`examples/**`, `integration/*.md`, every package `README.md`, and `AGENTS.md` —
not only the site. Package READMEs are discovered by walking the repository, so
a new package cannot opt out by existing. This file is the one exemption: it
necessarily contains the words it bans.

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
a violation.

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
