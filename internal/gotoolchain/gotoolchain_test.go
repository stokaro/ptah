package gotoolchain_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/gotoolchain"
)

// writeManifest puts one YAML file in a temporary tree and parses it.
func parse(c *qt.C, name, body string) gotoolchain.Manifest {
	c.Helper()
	root := c.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(root, filepath.Dir(name)), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, name), []byte(body), 0o600), qt.IsNil)
	manifest, err := gotoolchain.ParseManifest(root, name)
	c.Assert(err, qt.IsNil)
	return manifest
}

// findings runs the step rules over one manifest.
func findings(c *qt.C, name, body string) []gotoolchain.Finding {
	c.Helper()
	return gotoolchain.CheckSteps(parse(c, name, body), make(map[string][]string))
}

// TestCheckSteps_RefusesALiteralPin is the failure the whole gate exists for: a
// version restated where it can drift from go.mod.
func TestCheckSteps_RefusesALiteralPin(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "a bare literal",
			body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version: 1.26.5\n",
		},
		{
			// Quoting is where the shell scan was caught out twice. The parser
			// removes it, so all three spellings are one value.
			name: "a double-quoted literal",
			body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version: \"1.26.5\"\n",
		},
		{
			name: "a single-quoted literal",
			body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version: '1.26.5'\n",
		},
		{
			name: "a quoted KEY as well as value",
			body: "jobs:\n  build:\n    steps:\n      - \"uses\": \"actions/setup-go@v7\"\n        \"with\":\n          \"go-version\": \"1.26.5\"\n",
		},
		{
			// A block scalar's value is on the following lines. The shell had to
			// refuse it unread; a parser reads it and judges it like any other.
			name: "a block scalar",
			body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version: >-\n            1.26.5\n",
		},
		{
			name: "a selector that is not numeric",
			body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version: stable\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found := findings(c, ".github/workflows/x.yml", test.body)
			c.Assert(found, qt.HasLen, 1)
			c.Assert(found[0].Message, qt.Contains, "The toolchain is declared once, in the root go.mod")
		})
	}
}

// TestCheckSteps_RefusesAStepWithNoVersionSource covers the step that names
// neither key: setup-go picks its own default and the module decides nothing.
func TestCheckSteps_RefusesAStepWithNoVersionSource(t *testing.T) {
	c := qt.New(t)

	found := findings(c, ".github/workflows/x.yml",
		"jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "names no Go version source")
}

// TestCheckSteps_RefusesANestedModule is the honest-looking failure: the step
// derives, from a module that carries a floor and no toolchain directive.
func TestCheckSteps_RefusesANestedModule(t *testing.T) {
	c := qt.New(t)

	found := findings(c, ".github/workflows/x.yml",
		"jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version-file: examples/orm-loaders/gorm/go.mod\n")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "not the module that declares the toolchain")
}

// TestCheckSteps_AcceptsTheRootModule is the control. Without it, a rule that
// refused every step would satisfy every row above.
func TestCheckSteps_AcceptsTheRootModule(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "bare", body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version-file: go.mod\n"},
		{name: "dot-slash", body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version-file: ./go.mod\n"},
		{name: "quoted", body: "jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version-file: \"go.mod\"\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(findings(c, ".github/workflows/x.yml", test.body), qt.HasLen, 0)
		})
	}
}

// TestCheckSteps_AnExpressionIsRefusedOutsideACompositeAction is the rule that
// makes the exemption narrow: an expression shows that a value is derived,
// never from what, and a workflow has a module to read.
func TestCheckSteps_AnExpressionIsRefusedOutsideACompositeAction(t *testing.T) {
	c := qt.New(t)

	found := findings(c, ".github/workflows/x.yml",
		"jobs:\n  build:\n    steps:\n      - uses: actions/setup-go@v7\n        with:\n          go-version-file: ${{ inputs.go-version-file }}\n")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "only a composite action manifest may forward an input")
}

// TestCheckSteps_AcceptsTheTwoForwardingShapes covers the composite action, and
// records that these two are the whole accepted language.
func TestCheckSteps_AcceptsTheTwoForwardingShapes(t *testing.T) {
	c := qt.New(t)

	forwarded := make(map[string][]string)
	manifest := parse(c, ".github/actions/ptah/action.yml",
		"runs:\n  using: composite\n  steps:\n    - uses: actions/setup-go@v7\n      with:\n"+
			"        go-version: ${{ inputs.go-version }}\n"+
			"        go-version-file: ${{ inputs.go-version == '' && inputs.go-version-file || '' }}\n")

	c.Assert(gotoolchain.CheckSteps(manifest, forwarded), qt.HasLen, 0)
	c.Assert(forwarded["version"], qt.DeepEquals, []string{".github/actions/ptah/action.yml:go-version"})
	// The guarded shape forwards its SECOND reference; the first is the
	// condition, and reading the first would judge the wrong input's default.
	c.Assert(forwarded["file"], qt.DeepEquals, []string{".github/actions/ptah/action.yml:go-version-file"})
}

// TestCheckSteps_RefusesAnExpressionThatIsNeitherShape keeps the exemption from
// widening into "any expression in an action manifest".
func TestCheckSteps_RefusesAnExpressionThatIsNeitherShape(t *testing.T) {
	c := qt.New(t)

	found := findings(c, ".github/actions/ptah/action.yml",
		"runs:\n  using: composite\n  steps:\n    - uses: actions/setup-go@v7\n      with:\n        go-version: ${{ env.GO_VERSION }}\n")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "The toolchain is declared once")
}

// TestCheckForwardedDefaults_JudgesEachInputByItsRole is the rule a rename put
// outside the old detector's reach: the shell selected defaults to judge BY
// NAME, and a forwarded input can be called anything.
func TestCheckForwardedDefaults_JudgesEachInputByItsRole(t *testing.T) {
	tests := []struct {
		name    string
		role    string
		body    string
		wantHas string
	}{
		{
			name: "the version input defaults to a value",
			role: "version",
			body: "inputs:\n  anything:\n    default: stable\n",
			// Not "not a version": stable, oldstable and 1.x are all valid
			// selectors, and any of them non-empty makes setup-go ignore
			// go-version-file entirely.
			wantHas: "makes setup-go prefer it and ignore go-version-file",
		},
		{
			name:    "the module input defaults elsewhere",
			role:    "file",
			body:    "inputs:\n  anything:\n    default: examples/orm-loaders/gorm/go.mod\n",
			wantHas: "not the module that declares the toolchain",
		},
		{
			name:    "the module input declares no default",
			role:    "file",
			body:    "inputs:\n  anything:\n    description: a module\n",
			wantHas: "declares no default",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			manifest := parse(c, ".github/actions/x/action.yml", test.body)
			found := gotoolchain.CheckForwardedDefaults(
				map[string]gotoolchain.Manifest{".github/actions/x/action.yml": manifest},
				map[string][]string{test.role: {".github/actions/x/action.yml:anything"}})
			c.Assert(found, qt.HasLen, 1)
			c.Assert(found[0].Message, qt.Contains, test.wantHas)
		})
	}
}

// TestCheckForwardedDefaults_AcceptsTheDeclaredContract is the control for the
// three rows above.
func TestCheckForwardedDefaults_AcceptsTheDeclaredContract(t *testing.T) {
	c := qt.New(t)

	manifest := parse(c, ".github/actions/x/action.yml",
		"inputs:\n  go-version:\n    default: \"\"\n  go-version-file:\n    default: go.mod\n")

	found := gotoolchain.CheckForwardedDefaults(
		map[string]gotoolchain.Manifest{".github/actions/x/action.yml": manifest},
		map[string][]string{
			"version": {".github/actions/x/action.yml:go-version"},
			"file":    {".github/actions/x/action.yml:go-version-file"},
		})

	c.Assert(found, qt.HasLen, 0)
}

// TestCheckModule_RefusesASourceThatNamesNoToolchain is the deletion spelled as
// a declaration.
//
// `toolchain default` is valid Go and means "no pin". Measured on this module,
// everything else unchanged: `toolchain go1.26.6` gives GOVERSION=go1.26.6 and
// `toolchain default` gives the `go` directive instead -- the drift this gate
// exists to stop, reached without removing a line.
func TestCheckModule_RefusesASourceThatNamesNoToolchain(t *testing.T) {
	tests := []struct {
		name    string
		module  gotoolchain.Module
		wantHas string
	}{
		{
			name:    "toolchain default",
			module:  gotoolchain.Module{Go: "1.26.5", Toolchain: "default", Toolchains: 1},
			wantHas: "names no toolchain",
		},
		{
			name:    "no directive at all",
			module:  gotoolchain.Module{Go: "1.26.5", Toolchains: 0},
			wantHas: "declares 0 `toolchain` directives",
		},
		{
			name:    "two directives",
			module:  gotoolchain.Module{Go: "1.26.5", Toolchain: "go1.26.6", Toolchains: 2},
			wantHas: "declares 2 `toolchain` directives",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			found := gotoolchain.CheckModule(test.module)
			c.Assert(found, qt.HasLen, 1)
			c.Assert(found[0].Message, qt.Contains, test.wantHas)
		})
	}
}

// TestCheckModule_AcceptsANamedToolchain is the control.
func TestCheckModule_AcceptsANamedToolchain(t *testing.T) {
	c := qt.New(t)

	found := gotoolchain.CheckModule(gotoolchain.Module{Go: "1.26.5", Toolchain: "go1.26.6", Toolchains: 1})

	c.Assert(found, qt.HasLen, 0)
}

// TestReadModule_ReadsBothDirectives keeps the two facts apart: the floor and
// what CI builds with have different lifecycles.
func TestReadModule_ReadsBothDirectives(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(c.TempDir(), "go.mod")
	c.Assert(os.WriteFile(path, []byte("module example.com/x\n\ngo 1.26.5\n\ntoolchain go1.26.6\n"), 0o600), qt.IsNil)

	module, err := gotoolchain.ReadModule(path)

	c.Assert(err, qt.IsNil)
	c.Assert(module.Go, qt.Equals, "1.26.5")
	c.Assert(module.Toolchain, qt.Equals, "go1.26.6")
	c.Assert(module.Toolchains, qt.Equals, 1)
}
