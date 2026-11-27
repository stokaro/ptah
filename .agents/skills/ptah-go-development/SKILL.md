---
name: ptah-go-development
description: Use when implementing or reviewing Go changes in Ptah, including package boundaries, public APIs, errors, resource ownership, concurrency, portable code, and regression tests. Pair with the schema, provider, or compatibility skill when those contracts change.
---

# Ptah Go development

## Locate the contract

1. Identify the affected module with `scripts/list-go-modules.sh`. Root
   `go test ./...` does not visit nested modules. Read that module's `go.mod`;
   the `go` directive is its compatibility floor, not the CI toolchain.
2. Find the production caller, nearest implementation, and existing tests with
   scoped `rg` searches. Read the interface and its consumers before changing
   the implementation. Do not copy an adjacent pattern that violates policy.
3. Search the affected package or symbol in `docs/public_api.md`. Read its
   entry and the compatibility guard when an exported contract changes; do not
   load the entire ledger. Pre-v1 cleanup still needs the reviewed approval
   required for an intentional Stable Embedder API break.
4. Read the relevant section of [Go policy](../../references/go-style.md).
   Use `.golangci.yml` for actual settings. Keep detailed rules there rather
   than duplicating them in new instructions.

## Implement at the existing boundary

- Keep the change with the package that owns the behavior. Follow
  `docs/architecture_boundaries.md` for an import or ownership change. Shared
  code uses selected services and capabilities; moving an engine branch into
  a shared helper does not remove the coupling.
- Prefer an existing type or concrete implementation. Add an interface when
  a consumer needs the seam, with the methods that consumer uses. Do not add
  generic frameworks, speculative configuration, or pass-through wrappers to
  make a local fix appear reusable.
- State who owns mutable values. Clone retained caller slices and maps when
  the contract requires isolation. `engine/migration_planner.go` shows option
  cloning before a selected factory runs. Avoid process-global mutable
  registrations where runtimes must select services independently.
- Preserve `core/ptaherr` sentinels and structured errors at public boundaries.
  Wrap causes with `%w`; use `errors.Is` or `errors.As` where callers branch.
  Add operation context without repeating the same message at every layer.
- Pass the request context through database and process work. Give detached
  cleanup a reason and a finite timeout; `internal/providerhost/session.go`
  closes a session even when the request was canceled. Do not detach ordinary
  work merely to make cancellation tests pass.
- Release resources on success, failure, and abandonment. Check `Rows.Err()`
  after iteration; `Close` alone does not prove a complete read. Preserve the
  primary error if cleanup also fails. Use existing resource helpers.
- Preserve deterministic output without sorting away meaningful order. Test
  equivalent inputs in different orders when maps or sets reach output.
- For filesystem or process changes, read the portability section of
  [Go policy](../../references/go-style.md). Use the existing
  URL, executable-extension, and fixture helpers; avoid matching OS prose.

## Prove the changed behavior

Read the applicable [test rules](../../references/testing.md), then add the
smallest regression through the production path that exposed the defect.
Record the failure without the fix and the pass with it. A control must reject
an implementation that skips the behavior entirely.

- Use Quicktest as `qt`, a checker local to each `t.Run`, data-only table rows,
  and separate success and failure cases. Do not hide assertion selection in
  helpers or callbacks. Keep black-box tests unless the documented exception
  applies.
- Assert caller-visible results and error classes. Add ordering, cancellation,
  nil/zero-value, or aliasing cases only where the changed contract makes them
  relevant. Do not mirror private implementation steps in tests.
- A field addition requires checking its consumers, constructors, copies,
  serializers, and equality logic. A helper with only test callers does not
  prove the production rule is active.
- Use the validation policy for the affected packages, modules, tags, and
  platforms. Inspect the final diff after formatting or lint fixes. Do not
  widen exclusions, weaken assertions, or refresh a baseline to hide a failure.

## Finish with evidence

Update the public contract and affected documentation when observable behavior
changes. Report the tested path and result, the missing case the regression
covers, and any required check still pending. A passing compile or lint run is
not evidence of database behavior or cross-platform execution.
