# Documentation maintenance policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Documentation Obligations

**Before finishing a change to external behavior, inspect and update the
documentation.** External behavior: CLI names, flags, environment variables,
help, output formats, exit codes; config formats, keys, validation,
precedence; generated and parsed SQL, migration files, directives, revision
tables, hash files; public Go APIs and extension points; `ptah-compat`
behavior, conformance claims, documented limitations; user-facing errors,
warnings, diagnostics and safety checks.

- Search old and new terms (commands, flags, variables, issue numbers, exact
  errors) in `README.md`, `docs/`, `docs/site/src/content/docs/`,
  `examples/**/README.md`, `integration/*.md`, package READMEs, and this file
  when rules change.
- `go test ./internal/doccommands -count=1` fails while a command line it
  reads in a tracked page or in `--help` spells a verb or flag the command tree
  does not have, so a rename lands with the documentation it reads. Section 16
  of the style guide says which lines those are. It lists pages through git, so
  without `-count=1` a cached run does not see a page added or removed.
- Never document Atlas command paths in the native binary, and never claim
  full Atlas parity without conformance evidence.
- **Preserve the applicable gates in `.github/workflows/docs.yml`.** Start
  with focused local feedback, then account for the required checks through
  local or current-head CI evidence. The documentation-maintenance skill's
  [full suite](../skills/ptah-documentation-maintenance/references/validation-suite.md)
  lists every invocation; `check-skill-suite.mjs` keeps it equal to the
  workflow. Load it for a full reproduction or checker change. The style
  guide holds the page rules (section 7 terms, 8 code and quick starts, 11
  visuals, 15 checklist, 17 translations).
- Terms live in `docs/site/scripts/data/terminology.json` (read by
  `check-terminology.mjs` and `internal/cli/internal/terminologyguard`); render
  section 7 with `--write`. Product definitions live once in `src/glossary.ts`.
- A new page joins `docs/site/src/sidebar.mjs` and the route ledger (`node
  docs/site/scripts/check-route-retirement.mjs --write`); a retired route gets
  a `redirectRoutes` entry. A route added and renamed on an unmerged branch
  leaves the ledger only by `--forget <route> --against origin/master`, never
  by a redirect for a URL nobody was served. `docs/site/scripts/lib/docroutes.mjs`
  says which routes exist; a route is Astro's, not the file path's.
- The content inventory is computed by `buildContentInventory()` in
  `docs/site/scripts/build-content-inventory.mjs` (`--print`), never committed.
- A provider's pages live in its repository's `docs/` folder and reach the site
  through `docs/site/scripts/import-provider-docs.mjs`, at the release
  `docs/site/scripts/data/provider-docs.json` names. Edit one there, never under
  the ignored `_providers/` directory the import writes; `docs/site/README.md`
  says how to move to a newer release and how to add a provider.
- `docs/site/src/content/docs/atlas/feature-matrix.md` is generated: edit
  `docs/site/scripts/data/feature-matrix-rows.json`, run `node
  docs/site/scripts/build-feature-matrix.mjs`. A `note` is the reader's text
  and never restates a count or list that code or a generated table owns;
  `evidence` is not rendered. When a verdict flips, re-tense the old reasoning
  as history and keep its measurement (`check-matrix-verdict-prose.mjs`).
- A page claims features with `owns:`; `scripts/check-feature-inventory.sh
  --write` regenerates `docs/feature-inventory.json`. Keep the design
  `internal/featureinventory` documents: exact strings, a claimed-row floor in
  source, columns named for what is checked (never "canonical"), program rows
  from `.goreleaser.yaml`, not `go list`.
- A quick start sets `quickstart: true`; CI runs its commands on three
  platforms (`internal/quickstart`, `scripts/check-quickstart.sh`), and
  `quickstartContinues:` chains pages in one working directory. The inference
  quick start has its own Compose fixture; on a remote Docker context run
  `PTAH_DOCKER_CONTEXT=<context> PTAH_FIXTURE_HOST=<host> docs/site/scripts/check-inference-quick-start.sh`.
- `check-translations.mjs` checks only the structure and bytes of
  `README.ja.md`, `README.de.md` and `README.fr.md`; review their meaning
  yourself, accessibility text included.
- `support-matrix.md` is generated, `support-policy.md` the promise,
  `support-evidence.md` the measurement; generated counts stay off the last
  two.
- After a visual fixture or generator changes, run `PTAH_BIN=../../bin/ptah
  npm run assets:write` in `docs/site` and look at the PNGs in
  `docs/site/src/assets/`; `check-generated-assets.mjs` compares only
  `docs/site/public/samples`.
- Each top-level `examples/*` has the sections `check-examples.mjs` requires;
  `npm run examples:write` rebuilds the index, and `npm run check:examples`
  and `scripts/check-examples.sh` verify it.
- Deep documentation work: `.agents/skills/ptah-documentation-maintenance/SKILL.md`.
