# Review and issue policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Contents

- [PR and issue writing](#pr-and-issue-writing)
- [Pull request descriptions](#pull-request-descriptions)
- [Purpose and scope](#purpose-and-scope)
- [Before and after diagram](#before-and-after-diagram)
- [Evidence and maintenance](#evidence-and-maintenance)
- [Ptah-specific review context](#ptah-specific-review-context)
- [Label every issue you file](#label-every-issue-you-file)

## PR and issue writing

- Write PR and issue titles in plain American English. Name the concrete
  problem or intended result using familiar words. Follow the repository's
  title convention, but do not make readers decode internal component
  names, unexplained acronyms, or abstract architecture terms to understand
  the purpose. Do not add tool signatures or generated-with footers.
- Start every PR with the template's `Summary`, before `Goal and motivation`.
  In a short paragraph, usually two to four sentences, explain who or what
  is affected, what is wrong, missing, or unnecessarily difficult, and why
  it matters.
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
- Establish the problem, motivation, goal, and observable conditions for
  success from the request, issue, discussion, and supporting evidence.
  Do not invent user impact, urgency, performance gains, or security
  benefits. Ask for clarification when missing information materially
  changes the goal, scope, acceptable risk, or completion criteria.
- Use `Summary` for the plain-English problem and its consequence,
  `Goal and motivation` for the intended improvement and its supported
  rationale, and `Expected outcome` for observable success conditions,
  scope, and limitations. Add information in each section instead of
  retelling the same explanation.
- For a bug fix, explain the established cause. Distinguish observations,
  conclusions drawn from code, and hypotheses.
- Distinguish delivered outcomes from proposals and deferred work, and
  expected benefits from measured results. Explain non-obvious decisions
  and include implementation details when they help assess correctness,
  risk, or use. Avoid file inventories, commit histories, and repeated
  summaries.
- Keep the essential explanation in the PR. Link issues that define its
  scope, required companion changes, and relevant remaining work.
  Use closing keywords only when the PR fully resolves an issue.

### Before and after diagram

- Every PR description must include a simple before/after diagram in its
  `Expected outcome` section. This applies to all PRs, including documentation,
  refactoring, maintenance, and dependency updates. A diagram in a linked issue
  or comment does not replace the one in the PR body.
- Use a fenced `mermaid` block with clearly labeled `Before` and
  `After (with this PR)` parts. Compare the same concrete scenario and show the
  changed steps and their result. Keep labels plain and short, and include
  only the nodes needed to explain the change.
- When runtime behavior is unchanged, show the actual change to documentation,
  structure, dependencies, or contributor workflow. Do not invent a runtime
  effect to fill the diagram. Explain the main difference in nearby prose.
- Verify that the diagram renders, and keep it aligned with the complete
  current PR when its scope or implementation changes.

### Evidence and maintenance

- Connect validation to the expected outcomes and material risks. Report
  the observed results and which success conditions remain unverified.
  Do not present lint, compilation, skipped checks, or planned work as
  proof of behavior they do not verify.
- State the engines, versions, platforms, or component combinations
  actually exercised when they affect the claim. Provide manual steps
  only where they add useful verification.
- Before publishing, inspect the complete diff from the actual base
  branch to the current head.
- After substantive changes, revise the description for the complete
  current PR. Update the goal, outcomes, scope, risks, dependencies, and
  validation claims; replace stale text instead of appending a development
  diary.
- Keep the description proportional to the change. Put essential results,
  material risks, and remaining uncertainty in the main text. Link or
  collapse lengthy supporting evidence.
- Follow all repository-specific testing, documentation, safety, and
  compatibility requirements alongside this description standard.

The shared wording of these sections and the PR template is maintained in
[`stokaro/ptah`](https://github.com/stokaro/ptah). Keep copies aligned across
Ptah repositories; put repository-specific additions in a separate section.

### Ptah-specific review context

For compatibility-surface changes, include the declaration required by
[capability ownership](compatibility.md#native-and-compatibility-capability-ownership).
For database effects, state which engine-level evidence covers the change,
or why the [engine-level test conditions](testing.md#an-engine-observable-change-carries-an-engine-level-test)
do not apply. For bug fixes, identify the missing case and regression
proof required by [the fix standard](testing.md#every-fix-carries-a-regression-test).
Keep provider-contract checks distinct from live engine checks owned by the
provider repository. Put these details in the relevant template section.

### Label every issue you file

Only web-form issues get a type label, and an unlabeled issue escapes every
planning filter, so pass labels to `gh issue create`: one type (`bug`,
`enhancement`, `feature-request`, `documentation`, `question`); areas only
where clear (`cli`, `migration`, `schema-generation`, `postgresql`, `sql`,
`rls`, `roles`, `security`, `foreign-key`, `constraints`, `field-ordering`);
`post-ga` or `critical` when they apply. Engine names have no label; classify
by subject. Milestones are not used. Prefer no label to a wrong one.

`frozen` stops work for the reason its comments give; it is not ownership,
and a stale reason is raised on the issue, not decided alone.
`hold-off-merge` means what it says. `eol-keep` on a CLOSED end-of-life pull
request marks its declaration deliberate, so the daily check opens no new
pull request for that line.
