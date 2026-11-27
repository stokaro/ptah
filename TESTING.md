# Testing Ptah

Start with the behavior your change affects, then expand to its callers and
contracts. The [Makefile](Makefile) and [CI workflows](.github/workflows)
define executable checks. The [test standards](.agents/references/testing.md)
define regression, engine-level, fixture, and assertion requirements.

## Prerequisites

Use the Go versions declared in `go.mod`; its `toolchain` directive identifies
the build toolchain. Check `go version` before running Go commands. The
Makefile and shell scripts require Make and Bash. Database scenarios need
Docker Compose; documentation checks use the dependencies described in
[the site README](docs/site/README.md).

Run commands from the repository root unless a command uses `go -C` to select
another module. Keep changes in the linked worktree required by
[AGENTS.md](AGENTS.md).

## Iterate on a focused test

Select a test from the affected package. For example, this renderer test
runs without a database server:

```bash
go test ./engine/builtin -run '^TestNewVisitorMethods_UnitTests$' -count=1 -timeout=2m
```

Confirm that the filter executes the intended test. For a bug fix, prove
that the new regression fails for the defect and passes with the fix. Then
run the owning package and affected consumers. Use an explicit timeout;
downloads and compilation also contribute to wall time.

Keep Go's normal cache for ordinary iteration. Use `-count=1` for regression
proof and tests that read external state, including Git inventories and live
databases. For documentation command changes, run:

```bash
go test ./internal/doccommands -count=1 -timeout=5m
```

## Broader checks

| Command | What it covers |
| --- | --- |
| `make build` | Shipped binaries and the integration scenario runner |
| `go test ./... -count=1 -timeout=10m` | Untagged tests in the root Go module |
| `make lint` | golangci-lint, qtlint, and nolintguard across discovered modules and tag configurations |
| `scripts/check-test-style.sh` | The declarative-test baseline |
| `make integration-test` | Docker Compose scenarios through `cmd/integration-test` |

Run these when their scope is relevant. A focused local pass does not replace
required CI checks. Use the existing CI jobs for complete engine and platform
coverage instead of repeatedly starting the same expensive suite locally.
Report blocked or deferred checks with the specific evidence still needed.

The [validation skill](.agents/skills/ptah-validation/SKILL.md) describes
local budgets, escalation, and reporting. The
[validation policy](.agents/references/validation.md) names the full checks
and their constraints.

## Modules, tags, and platforms

A root `go test ./...` does not enter nested Go modules. Discover modules
with `scripts/list-go-modules.sh` and test the affected module explicitly.
For example:

```bash
go -C provider-sdk test ./... -timeout=5m
```

The [module workflow](.github/workflows/go-module-tests.yml) builds and tests
every discovered nested module with `GOWORK=off`. A local workspace can link
unreleased modules, but it does not prove that their published dependencies
work. Follow the [provider SDK module boundary](provider-sdk/README.md#module-boundary)
when changing the SDK or host together.

Discover opt-in tags with `scripts/list-build-tags.sh`. Default and tagged
builds can select different files. The
[unit workflow](.github/workflows/go-unit-tests.yml) owns race checks,
Windows runs, the `js/wasm` build, and opt-in tag checks. Report only the
platforms and execution modes actually exercised.

## Integration evidence

The tagged Go integration suite and the Compose scenario runner are separate
checks. After configuring the services, tools, and environment declared by
[the integration workflow](.github/workflows/go-integration-tests.yml), run
the complete tagged suite through its existing runner:

```bash
go run ./internal/cmd/testcontour --tags integration --timeout 40m
```

The runner discovers tests, rejects skips, and verifies complete execution.
CI can split its work into shards and verify their combined coverage. A
filtered tagged `go test` is a local diagnostic only; it does not establish
a complete integration pass. The Compose targets do not replace this runner.

Use `dbtarget` in live tests and remove everything the test creates. Avoid
concurrent runs that share mutable database state or fixed ports. Inspect
Docker resources before using cleanup targets; `make docker-clean` also
prunes the Docker daemon.

When SQL or engine behavior changes, retain both the unit regression and the
required engine-level test. Provider engine evidence belongs in the provider
repository; Ptah's fixture proves the host contract. Name any pending live
check in the PR and verify it before claiming the behavior is covered.
