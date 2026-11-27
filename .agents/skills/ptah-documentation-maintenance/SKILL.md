---
name: ptah-documentation-maintenance
description: Update Ptah documentation for changes to CLI behavior, config, SQL, migrations, public APIs, examples, conformance, or contributor workflows. Use for documentation audits and site changes. Load only references relevant to the affected behavior or page.
---

# Maintain Ptah documentation

Keep documentation aligned with the complete change and verified behavior.
Paths and commands below are relative to the repository root unless stated.

## Find the affected documentation

1. Identify the observable change and the reader's task. For an internal
   refactor, check whether output, accepted inputs, generated artifacts, or
   documented workflows changed before declaring documentation unaffected.
2. Read the relevant routes in [impact map](references/impact-map.md).
   Search old and new terms across the listed surfaces with `rg`; read matching
   passages and their context. Do not load every page or external repository.
3. For pages, choose one primary page type and the matching template. For
   generated pages, edit the owning source or generator and regenerate.
   Read [editorial workflow](references/editorial-workflow.md) when the change
   affects page structure, metadata, navigation, examples, or visuals.

## Read the relevant style rules

`docs/STYLE_GUIDE.md` is authoritative. Find its headings with
`rg -n '^##|^###' docs/STYLE_GUIDE.md` and read the needed sections:

- Prose: sections 4–6 together, plus the relevant terminology in section 7.
- Page type and frontmatter: sections 2–3.
- Commands and examples: section 8; links and routes: section 12.
- Tables and visuals: sections 10–11 and 13.
- Translations: section 17; final review: section 15.
- Checker coverage: section 16 when deciding which claims need human review.

Do not reread unchanged sections already in context. Do not fetch external
style examples for a wording correction. These loading choices keep the same
rules; they do not exempt a changed page from them.

## Get local feedback before broad validation

Start with the cheapest checks that can detect defects in the change. Read the
applicable jobs in `.github/workflows/docs.yml` for their exact commands and
prerequisites. Run standalone checks before installing site or browser tools.

- For any Markdown change, run `node docs/site/scripts/check-style.mjs` and its
  `--selftest`. Include terminology and chronology checks for changed prose.
- For page metadata or content, run editorial-shape and source-page-contracts
  checks with their self-tests. For links or routes, add the relevant link,
  redirect, and route-retirement checks.
- For command examples or changed CLI spelling, run
  `go test ./internal/doccommands -count=1` and execute changed examples against
  the current binary. A command-tree check proves spelling, not runtime results.
- For generated references, schemas, or samples, run the owning generator and
  its check. Inspect changed visuals. Never edit generated output alone.
- For navigation, layout, or styling, build once and run the relevant browser,
  search, and accessibility checks. Review mobile and desktop rendering.
- For a checker change, run its self-test and a fixture that the faulty checker
  misses. Restore the checker and verify that it reports the defect.

Reuse a successful check only while its inputs and environment are unchanged.
Do not reinstall dependencies or rerun unrelated checks after each prose edit.

## Finish with the required evidence

Local feedback is not the merge gate. Preserve all applicable checks from the
workflow and the style guide's review requirements. CI may supply expensive
engine, browser, platform, or full-suite evidence; record anything not yet run
as unverified. A timeout, missing dependency, or skip never counts as a pass.

Load [complete validation suite](references/validation-suite.md) only when
reproducing CI or running the full documentation validation. The suite is held
to the workflow by `check-skill-suite.mjs`; do not copy it into this entrypoint.

Review affected surfaces for coverage, verify the claims and examples, and
check the changed prose against the style guide. Search again for retired
terms or links. Keep native and compatibility command paths separate; support
and parity claims require current evidence. State the documentation impact,
actual validation, and remaining uncertainty in the PR.
