---
name: ptah-review
description: Review a Ptah change or prepare its pull request or issue. Use for final diff inspection, regression evidence, architecture and compatibility decisions, CI status, plain-English descriptions, and the required before-and-after diagram.
---

# Review and describe a Ptah change

## Review the whole change

1. Read the request or issue and the current description. Compare the complete
   diff against the actual base, including generated files and module pins.
   Keep work from other agents separate from the intended change.
2. Trace the changed behavior through its production caller. Check the owning
   boundary and affected consumers; a helper's own tests do not prove it runs.
3. Look for lost schema information, mistaken deletion intent, partial or
   uncertain execution, retry safety, error identity, and cleanup where those
   risks apply. Use the schema or provider skill for a changed contract.
4. Check native, default compatibility, and strict behavior when they share
   the path. Declare `GENERAL CAPABILITY` or `COMPATIBILITY ADAPTER` for
   compat additions as the compatibility policy requires.
5. Compare each claim with its evidence. Confirm the regression fails for the
   original defect and passes with the fix. Separate host-contract evidence
   from live engine evidence, and compilation from runtime behavior.
6. Inspect remaining risk rather than launching broad tests again. Use
   [validation](../ptah-validation/SKILL.md) for required checks and current-head
   CI status. Treat a required failure or missing result as unresolved.

For a large or data-sensitive change, request an independent review when an
agent is available. Give it the request, base and head revisions, raw diff,
and applicable checks, with a bounded scope. Have it inspect production
callers and report concrete failure cases, not preferences. Verify each
finding against the current code before changing it. Resolve substantive
findings before repeating review; review the fixes and affected contracts
again, not unchanged parts of the tree. The
[workflow guidance](../../references/agent-workflows.md) covers delegation
and evidence for longer tasks.

## Prepare the PR or issue

Read [review policy](../../references/review.md) and
`.github/pull_request_template.md` before publishing. Keep the template's
headings and remove drafting comments. Use short, plain American English.

- Start `Summary` with the affected contributor or user, the concrete
  problem, and its consequence. Do not start with a file inventory.
- Use `Goal and motivation` for the supported reason and intended improvement.
  Separate measurements from expected benefits; do not invent token savings,
  speedups, security impact, or parity claims.
- Use `Expected outcome` for observable success conditions and scope. Include
  the required Mermaid diagram comparing the same scenario under `Before`
  and `After (with this PR)`. Internal changes show the contributor workflow
  or structure, without inventing a runtime effect. Verify the diagram.
- State material risks, component/release dependencies, and limitations in
  `Risks and compatibility`.
- In `Verification`, state commands, results, environments, and unverified
  outcomes. Distinguish passed, failed, blocked, deferred, and inapplicable
  checks. Include the missing regression case for a behavioral fix.
- Keep issue labels accurate, honor `frozen` and `hold-off-merge`, and use a
  closing keyword only when the complete issue is resolved.

Read the title and opening without the rest of the description. They must
explain why the change matters. After substantive edits, replace stale claims
throughout the description instead of appending a progress diary.
