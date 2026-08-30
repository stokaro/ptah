# Reusable Go components example

## What this example demonstrates

These tests exercise Ptah's supported embedder surface without invoking a CLI:
AST construction and rendering, Go and Atlas HCL schema parsing, diff planning,
migration integrity and linting, custom lint rules, and capability-aware SQL.

## Prerequisites

- The Go toolchain declared in the repository's `go.mod`.
- A Ptah checkout with its module dependencies available.

## Run

From the repository root:

```bash
go test ./examples/reusable_components -count=1
```

## Expected result

The command exits 0 and reports the package as `ok`. It opens no listener and
connects to no database.

## Verify

Run one focused contract when changing an individual component:

```bash
go test ./examples/reusable_components -run TestDiffAndPlan -count=1
```

The test requires one SQLite `CREATE TABLE "users"` statement from the public
schema-diff and planner packages.

## Cleanup

The tests use in-memory filesystems and values, so there is no example state to
remove.

## Learn more

Use [Extend Ptah with Go](https://stokaro.github.io/ptah/edge/extend/overview/)
to choose a supported package, or open the
[public API ledger](https://stokaro.github.io/ptah/edge/extend/public-api/)
before adding an import.
