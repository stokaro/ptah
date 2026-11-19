# AGENTS.md

## Where changes belong

The action files and this guidance are maintained in
[the action source directory](https://github.com/stokaro/ptah/tree/master/.github/actions/ptah)
of `stokaro/ptah`. They are published at the root of `stokaro/ptah-action`.
Edit them in the source repository; direct edits in the published copy are
replaced by the next publication.

The published repository maintains its own `.github/` directory and
`CONTRIBUTING.md`; the publication workflow preserves those paths.
Changes to its PR template or smoke workflow belong in `stokaro/ptah-action`.
When working in the source directory, also follow the root `AGENTS.md`.

## PR and issue writing

- Write PR and issue titles in plain American English. Name the concrete
  problem or intended result using familiar words. Follow the repository's
  title convention, but do not make readers decode internal component
  names, unexplained acronyms, or abstract architecture terms to understand
  the purpose. Do not add tool signatures or generated-with footers.
- Start every PR with the template's `Summary`, before `Why`. In a short
  paragraph, usually two to four sentences, explain who or what is affected,
  what is wrong, missing, or unnecessarily difficult, and why it matters.
  Assume the reader knows what the project does but has not followed this
  work. Keep architecture and implementation details in later sections.
- Start every issue with the same kind of plain-English introduction,
  including issues created through a CLI or API. With an issue form, put
  this explanation in the first description field. Follow it with the
  evidence, reproduction steps, expected behavior, constraints, or proposal
  relevant to that issue.
- For maintenance, documentation, and internal changes, explain the
  concrete problem for contributors or maintainers. Do not invent an
  effect on end users, an incident, or a benefit that the evidence does
  not establish.
- Before publishing, read the title and opening without the rest of the
  description. They must explain the problem and why it matters without
  requiring the reader to inspect code, follow links, or reconstruct the
  investigation. Explain unfamiliar terms where they first appear.
- When updating a PR or issue, read its current title and description
  first. Keep the title and introduction accurate as the scope changes.

## Pull request descriptions

### Purpose and scope

- Read and follow `.github/pull_request_template.md`, including when
  creating or updating a PR through a CLI or API. Keep its headings and
  remove drafting comments and unused optional content.
- Establish intent from the request, issue, discussion, and supporting
  evidence. Do not invent user impact, urgency, performance gains, or
  security benefits. Ask for clarification when missing information
  materially changes the goal, scope, acceptable risk, or completion
  criteria.
- Use `Summary` for the plain-English problem and its consequence, `Why`
  for supporting evidence and reasoning, and `What changes` for delivered
  behavior and scope. Add information in each section instead of retelling
  the same explanation.
- For a bug fix, explain the established cause. Distinguish observations,
  conclusions drawn from code, and hypotheses.
- Describe meaningful limitations and non-obvious decisions. Include
  implementation details when they help assess correctness, risk, or use.
  Avoid file inventories, commit histories, and repeated summaries.
- Keep the essential explanation in the PR. Link issues that define its
  scope, required companion changes, and relevant remaining work.
  Use closing keywords only when the PR fully resolves an issue.

### Evidence and maintenance

- Report observed validation results and important coverage gaps.
  Explain what each significant check establishes. Do not present lint,
  compilation, skipped checks, or planned work as proof of behavior they
  do not verify.
- State the engines, versions, platforms, or component combinations
  actually exercised when they affect the claim. Provide manual steps
  only where they add useful verification.
- Before publishing, inspect the complete diff from the actual base
  branch to the current head.
- After substantive changes, revise the description for the complete
  current PR. Update behavior, scope, risks, dependencies, and validation
  claims; replace stale text instead of appending a development diary.
- Keep the description proportional to the change. Put essential results,
  material risks, and remaining uncertainty in the main text. Link or
  collapse lengthy supporting evidence.
- Follow all repository-specific testing, documentation, safety, and
  compatibility requirements alongside this description standard.

The shared wording of these sections and the PR template is maintained in
[`stokaro/ptah`](https://github.com/stokaro/ptah). Keep copies aligned across
Ptah repositories; put repository-specific additions in a separate section.

### Action review context

For runtime changes, link the source PR and state the action inputs,
outputs, permissions, and Ptah version affected. Distinguish a source check
from a published-action workflow run. Record the action ref and scenario
actually exercised when claiming consumer behavior. In the published
repository, explain whether the changed file is mirrored or maintained
locally so the next publication preserves the intended result.
