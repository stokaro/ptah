package main

// White-box testing required: the YAML workflow auditor is an internal CI
// command whose AST acceptance and fail-closed diagnostics have no exported API.

import (
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestAuditCanonicalWorkflowsRequiresExpectedSetupGoSteps(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")
	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 27)

	writeFile(c, root, ".github/workflows/go-unit-tests.yml", `jobs:
  test:
    steps:
      - run: go test ./...
`)
	pins, err = auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `(?s).*go-unit-tests\.yml job unit-tests is missing from the workflow; expected 1 setup-go steps.*go-unit-tests\.yml job windows-publication is missing from the workflow; expected 1 setup-go steps.*`)
	c.Assert(pins, qt.HasLen, 25)
}

func TestAuditCanonicalWorkflowsRequiresNewSetupGoStepsInInventory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)
	writeFile(c, root, ".github/workflows/new.yml", `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
`)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*\.github/workflows/new\.yml job test declares 1 setup-go steps but is missing from the canonical inventory.*`)
	c.Assert(pins, qt.HasLen, 28)
}

func TestAuditCanonicalWorkflowsRequiresNewUnpinnedWorkflowInInventory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)
	writeFile(c, root, ".github/workflows/new.yml", `jobs:
  build:
    steps:
      - run: go test ./...
`)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*\.github/workflows/new\.yml job build declares 0 setup-go steps but is missing from the canonical inventory.*`)
	c.Assert(pins, qt.HasLen, 27)
}

func TestAuditCanonicalWorkflowsRequiresNewUnpinnedJobInInventory(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)
	writeFile(c, root, ".github/workflows/go-unit-tests.yml", `jobs:
  unit-tests:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
  windows-publication:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
  unpinned:
    steps:
      - run: go test ./...
`)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*go-unit-tests\.yml job unpinned declares 0 setup-go steps but is missing from the canonical inventory.*`)
	c.Assert(pins, qt.HasLen, 27)
}

func TestAuditCanonicalWorkflowsRequiresExpectedZeroCountJob(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)
	writeFile(c, root, ".github/workflows/docs.yml", `jobs:
  build:
    steps:
      - run: npm run build
  changes:
    steps:
      - run: git diff --quiet
  style:
    steps:
      - run: npm run lint
`)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*docs\.yml job deploy is missing from the workflow; expected 0 setup-go steps.*`)
	c.Assert(pins, qt.HasLen, 27)
}

func TestAuditCanonicalWorkflowsRequiresSetupGoInExpectedJob(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeExpectedWorkflowFixtures(c, root)
	writeFile(c, root, ".github/workflows/go-unit-tests.yml", `jobs:
  unit-tests:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
  windows-publication:
    steps:
      - run: go test ./...
`)

	pins, err := auditCanonicalWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `(?s).*go-unit-tests\.yml job unit-tests declares 2 setup-go steps; expected 1.*go-unit-tests\.yml job windows-publication declares 0 setup-go steps; expected 1.*`)
	c.Assert(pins, qt.HasLen, 27)
}

func TestAuditWorkflowsAcceptsCanonicalSelectors(t *testing.T) {
	tests := []struct {
		name     string
		workflow string
		selector string
		line     int
	}{
		{
			name: "quoted keys values and outside comments",
			workflow: `jobs:
  test:
    steps:
      - "uses": "actions/setup-go@v7" # current action
        "with":
          "go-version": "1.26.6" # current version
`,
			selector: "go-version",
			line:     4,
		},
		{
			name: "flow mapping",
			workflow: `jobs:
  test:
    steps: [{uses: actions/setup-go@v7, with: {go-version-file: go.mod, cache: true}}]
`,
			selector: "go-version-file",
			line:     3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeWorkflow(c, test.workflow)

			pins, err := auditWorkflows(root, "1.26.6")

			c.Assert(err, qt.IsNil)
			c.Assert(pins, qt.HasLen, 1)
			c.Assert(pins[0].path, qt.Equals, ".github/workflows/guard.yml")
			c.Assert(pins[0].line, qt.Equals, test.line)
			c.Assert(pins[0].selector, qt.Equals, test.selector)
		})
	}
}

func TestAuditWorkflowsRefusesAmbiguousOrNonCanonicalSetup(t *testing.T) {
	tests := []struct {
		name     string
		workflow string
		match    string
	}{
		{
			name: "missing selector",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with: {cache: true}
`,
			match: `.*declares 0 version selectors.*`,
		},
		{
			name: "multiple selectors",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
          go-version-file: go.mod
`,
			match: `.*declares 2 version selectors.*`,
		},
		{
			name: "dynamic selector",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "${{ matrix.go }}"
`,
			match: `.*dynamic version selectors are not supported.*`,
		},
		{
			name: "selector outside with",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        env:
          go-version: "1.26.6"
        with: {cache: true}
`,
			match: `.*declares 0 version selectors.*`,
		},
		{
			name: "selector belongs to following step",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with: {cache: true}
      - uses: example/other-action@v1
        with:
          go-version: "1.26.6"
`,
			match: `.*declares 0 version selectors.*`,
		},
		{
			name: "hash inside quoted selector",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6 # pinned"
`,
			match: `.*uses Go 1\.26\.6 # pinned; expected 1\.26\.6.*`,
		},
		{
			name: "hash inside quoted action",
			workflow: `jobs:
  test:
    steps:
      - uses: "actions/setup-go@v7 # pinned"
        with:
          go-version: "1.26.6"
`,
			match: `.*invalid setup-go action reference.*`,
		},
		{
			name: "stale selector under quoted uses key",
			workflow: `jobs:
  test:
    steps:
      - "uses": actions/setup-go@v7
        with:
          go-version: "1.26.5"
`,
			match: `.*uses Go 1\.26\.5; expected 1\.26\.6.*`,
		},
		{
			name: "selector alias",
			workflow: `version: &version "1.26.6"
jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: *version
`,
			match: `.*YAML aliases are not supported.*`,
		},
		{
			name: "selector custom tag",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: !version "1.26.6"
`,
			match: `.*custom YAML tag "!version" is not supported.*`,
		},
		{
			name: "selector non scalar",
			workflow: `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: ["1.26.6"]
`,
			match: `.*version selector must be an untagged string.*`,
		},
		{
			name: "step merge key",
			workflow: `defaults: &defaults
  name: setup
jobs:
  test:
    steps:
      - <<: *defaults
        uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
`,
			match: `(?s).*YAML merge keys are not supported.*`,
		},
		{
			name: "with alias",
			workflow: `setup: &setup
  go-version: "1.26.6"
jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with: *setup
`,
			match: `.*YAML aliases are not supported.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeWorkflow(c, test.workflow)

			_, err := auditWorkflows(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

func TestAuditWorkflowsRefusesDuplicateKeys(t *testing.T) {
	c := qt.New(t)
	root := writeWorkflow(c, `jobs:
  test:
    steps:
      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
          "go-version": "1.26.6"
`)

	_, err := auditWorkflows(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*duplicate mapping key "go-version".*`)
}

func TestAuditGoModulesDiscoversNestedModules(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, "go.mod", "module example/root\n\ngo 1.26.6\n")
	writeFile(c, root, "plugins/new/go.mod", "module example/plugin\n\ngo 1.26.5\n")

	pins, err := auditGoModules(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*plugins/new/go\.mod uses Go 1\.26\.5; expected 1\.26\.6.*`)
	c.Assert(pins, qt.HasLen, 2)
	c.Assert(pins[0].path, qt.Equals, "go.mod")
	c.Assert(pins[1].path, qt.Equals, "plugins/new/go.mod")
}

func TestAuditGoModulesDiscoversModulesFromDotRoot(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, "go.mod", "module example/root\n\ngo 1.26.6\n")
	writeFile(c, root, "plugins/new/go.mod", "module example/plugin\n\ngo 1.26.5\n")
	t.Chdir(root)

	pins, err := auditGoModules(".", "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*plugins/new/go\.mod uses Go 1\.26\.5; expected 1\.26\.6.*`)
	c.Assert(pins, qt.HasLen, 2)
}

func TestAuditGoModulesIgnoresKnownToolWorktrees(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, "go.mod", "module example/root\n\ngo 1.26.6\n")
	writeFile(c, root, ".codex/worktrees/old/go.mod", "module example/old\n\ngo 1.25.0\n")
	writeFile(c, root, ".codex/worktress/old/go.mod", "module example/old-typo\n\ngo 1.25.0\n")
	writeFile(c, root, ".claude/worktrees/old/go.mod", "module example/claude-old\n\ngo 1.25.0\n")

	pins, err := auditGoModules(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 1)
	c.Assert(pins[0].path, qt.Equals, "go.mod")
}

func TestAuditGoModulesDiscoversModulesInCommittedDotDirectories(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, "go.mod", "module example/root\n\ngo 1.26.6\n")
	writeFile(c, root, ".github/actions/tool/go.mod", "module example/tool\n\ngo 1.26.5\n")
	writeFile(c, root, ".tools/helper/go.mod", "module example/helper\n\ngo 1.26.6\n")

	pins, err := auditGoModules(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*\.github/actions/tool/go\.mod uses Go 1\.26\.5; expected 1\.26\.6.*`)
	c.Assert(pins, qt.HasLen, 3)
}

func TestAuditCompositeActionLinksSetupGoToVersionInput(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, ".github/actions/ptah/action.yml", `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.go-version }}
`)

	pins, err := auditCompositeAction(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 1)
	c.Assert(pins[0].selector, qt.Equals, "go-version")
}

func TestAuditCompositeActionsDiscoversEveryLocalCompositeAction(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/actions/other/action.yaml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*\.github/actions/other/action\.yaml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsDiscoversRepositoryWideLocalActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, "build/tool/action.yaml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*build/tool/action\.yaml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsAuditsCaseVariantSetupGoReferences(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, "build/tool/action.yml", `runs:
  using: composite
  steps:
    - uses: Actions/Setup-Go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*build/tool/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsAcceptsCurrentZeroSetupAndNonCompositeActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/actions/current/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version-file: go.mod
`)
	writeFile(c, root, ".github/actions/javascript/action.yaml", `runs:
  using: node24
  main: index.js
`)
	writeFile(c, root, ".github/actions/no-setup/action.yml", `runs:
  using: composite
  steps:
    - run: echo no-setup
`)

	pins, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 2)
	c.Assert(pins[1].path, qt.Equals, ".github/actions/current/action.yml")
	c.Assert(pins[1].selector, qt.Equals, "go-version-file")
}

func TestAuditCompositeActionsIgnoresKnownToolWorktrees(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	for _, path := range []string{
		".claude/worktrees/old/action.yml",
		".codex/worktrees/old/action.yml",
		".codex/worktress/old/action.yml",
	} {
		writeFile(c, root, path, `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)
	}

	pins, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 1)
}

func TestAuditCompositeActionsDiscoversActionsInCommittedDotDirectories(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".tools/helper/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*\.tools/helper/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsFollowsReferencedExcludedActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/tool
`)
	writeFile(c, root, "vendor/tool/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*vendor/tool/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsFollowsDollarReferencedExcludedActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: $/vendor/tool
`)
	writeFile(c, root, "vendor/tool/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*vendor/tool/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsAuditsAtInWorkspacePath(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/tool@v1
`)
	writeFile(c, root, "vendor/tool@v1/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*vendor/tool@v1/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsAcceptsCurrentAtInWorkspacePath(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/tool@v1
`)
	writeFile(c, root, "vendor/tool@v1/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.6"
`)

	pins, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 2)
}

func TestAuditCompositeActionsResolvesReferencesFromDotRoot(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/tool
`)
	writeFile(c, root, "vendor/tool/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.6"
`)
	t.Chdir(root)

	pins, err := auditCompositeActions(".", "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 2)
}

func TestAuditCompositeActionsFollowsNestedReferencedExcludedActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/outer
`)
	writeFile(c, root, "vendor/outer/action.yml", `runs:
  using: composite
  steps:
    - uses: ./node_modules/inner
`)
	writeFile(c, root, "node_modules/inner/action.yaml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*node_modules/inner/action\.yaml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsFollowsNestedDollarReferencedExcludedActions(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/vendor-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/outer
`)
	writeFile(c, root, "vendor/outer/action.yml", `runs:
  using: composite
  steps:
    - uses: $/node_modules/inner
`)
	writeFile(c, root, "node_modules/inner/action.yaml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*node_modules/inner/action\.yaml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsFollowsReferencesFromCanonicalAction(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeFile(c, root, ".github/actions/ptah/action.yml", `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.go-version }}
    - uses: ./vendor/tool
`)
	writeFile(c, root, "vendor/tool/action.yml", `runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`)

	_, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.ErrorMatches, `.*vendor/tool/action\.yml:.*uses Go 1\.26\.5; expected 1\.26\.6.*`)
}

func TestAuditCompositeActionsRefusesBrokenLocalReferences(t *testing.T) {
	tests := []struct {
		name      string
		reference string
		files     map[string]string
		match     string
	}{
		{
			name:      "missing manifest",
			reference: "./vendor/missing",
			match:     `.*local action reference "\./vendor/missing" resolves to 0 action manifests.*`,
		},
		{
			name:      "ambiguous manifest",
			reference: "./vendor/ambiguous",
			files: map[string]string{
				"vendor/ambiguous/action.yml":  "runs:\n  using: node24\n  main: index.js\n",
				"vendor/ambiguous/action.yaml": "runs:\n  using: node24\n  main: index.js\n",
			},
			match: `.*local action reference "\./vendor/ambiguous" resolves to 2 action manifests.*`,
		},
		{
			name:      "outside repository",
			reference: "./../outside",
			match:     `.*local action reference "\./\.\./outside" escapes the repository root.*`,
		},
		{
			name:      "ref suffix",
			reference: "$/vendor/tool@v1",
			match:     `.*local action reference "\$/vendor/tool@v1" must not include a ref suffix.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeCurrentCanonicalAction(c, root)
			writeFile(c, root, ".github/workflows/local-action.yml", fmt.Sprintf(`jobs:
  test:
    steps:
      - uses: %s
`, test.reference))
			for path, contents := range test.files {
				writeFile(c, root, path, contents)
			}

			_, err := auditCompositeActions(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

func TestAuditCompositeActionsHandlesLocalReferenceCycles(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	writeCurrentCanonicalAction(c, root)
	writeFile(c, root, ".github/workflows/local-action.yml", `jobs:
  test:
    steps:
      - uses: ./vendor/first
`)
	writeFile(c, root, "vendor/first/action.yml", `runs:
  using: composite
  steps:
    - uses: ./node_modules/second
`)
	writeFile(c, root, "node_modules/second/action.yml", `runs:
  using: composite
  steps:
    - uses: ./vendor/first
`)

	pins, err := auditCompositeActions(root, "1.26.6")

	c.Assert(err, qt.IsNil)
	c.Assert(pins, qt.HasLen, 1)
}

func TestAuditCompositeActionsRefusesMalformedActionStructure(t *testing.T) {
	tests := []struct {
		name   string
		action string
		match  string
	}{
		{
			name:   "missing runs",
			action: "name: missing-runs\n",
			match:  `.*runs must be a mapping.*`,
		},
		{
			name:   "non-mapping runs",
			action: "runs: composite\n",
			match:  `.*runs must be a mapping.*`,
		},
		{
			name: "missing using",
			action: `runs:
  main: index.js
`,
			match: `.*runs\.using must be an untagged string.*`,
		},
		{
			name: "non-string using",
			action: `runs:
  using: 24
`,
			match: `.*runs\.using must be an untagged string.*`,
		},
		{
			name: "composite missing steps",
			action: `runs:
  using: composite
`,
			match: `.*composite action steps must be a sequence.*`,
		},
		{
			name: "composite non-sequence steps",
			action: `runs:
  using: composite
  steps: {}
`,
			match: `.*composite action steps must be a sequence.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeCurrentCanonicalAction(c, root)
			writeFile(c, root, ".github/actions/malformed/action.yml", test.action)

			_, err := auditCompositeActions(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

func TestAuditCompositeActionRefusesBrokenVersionInputLinks(t *testing.T) {
	tests := []struct {
		name   string
		action string
		match  string
	}{
		{
			name: "stale hard-coded selector",
			action: `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: "1.26.5"
`,
			match: `.*must consume.*inputs\.go-version.*`,
		},
		{
			name: "removed setup step",
			action: `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - run: echo no-setup
`,
			match: `.*declares 0 setup-go steps; expected exactly one.*`,
		},
		{
			name: "disconnected input",
			action: `inputs:
  go-version:
    default: "1.26.6"
  other-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.other-version }}
`,
			match: `.*must consume.*inputs\.go-version.*`,
		},
		{
			name: "stale input default",
			action: `inputs:
  go-version:
    default: "1.26.5"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.go-version }}
`,
			match: `.*go-version default uses Go 1\.26\.5; expected 1\.26\.6.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			writeFile(c, root, ".github/actions/ptah/action.yml", test.action)

			_, err := auditCompositeAction(root, "1.26.6")

			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

func writeCurrentCanonicalAction(c *qt.C, root string) {
	writeFile(c, root, ".github/actions/ptah/action.yml", `inputs:
  go-version:
    default: "1.26.6"
runs:
  using: composite
  steps:
    - uses: actions/setup-go@v7
      with:
        go-version: ${{ inputs.go-version }}
`)
}

func writeExpectedWorkflowFixtures(c *qt.C, root string) {
	for path, jobs := range expectedWorkflowSetupGoCounts() {
		var workflow strings.Builder
		workflow.WriteString("jobs:\n")
		for _, job := range slices.Sorted(maps.Keys(jobs)) {
			fmt.Fprintf(&workflow, "  %s:\n    steps:\n", job)
			workflow.WriteString(strings.Repeat(`      - uses: actions/setup-go@v7
        with:
          go-version: "1.26.6"
`, jobs[job]))
		}
		writeFile(c, root, path, workflow.String())
	}
}

func writeWorkflow(c *qt.C, contents string) string {
	root := c.TempDir()
	writeFile(c, root, ".github/workflows/guard.yml", contents)
	return root
}

func writeFile(c *qt.C, root, name, contents string) {
	path := filepath.Join(root, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
}
