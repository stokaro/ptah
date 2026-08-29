// Package gotoolchain checks that the Go toolchain is declared once, in the
// root go.mod, and that everything else derives it.
//
// The failure this exists to stop: a dependency bump titled "update dependency
// golang to v1.26.6" merged having changed one file, `.golangci.yml`, because
// that was the only file its updater could reach. The literal stood in eighteen
// setup-go steps and an action input default, so CI kept building with the older
// toolchain and govulncheck reported seven standard-library vulnerabilities
// against it. Nothing failed while the declarations disagreed.
//
// The detectors enumerate POSITIVELY: they find every setup-go step and require
// each to derive its version, rather than hunting for a known-bad literal. A
// workflow copied from an older one fails on arrival, and no detector mentions
// the current version, so the check cannot quietly stop working the day the
// toolchain moves.
//
// It reads YAML with a YAML parser. The 642 lines of shell this replaces
// hand-rolled step extraction by indentation, quoted-key handling, scalar quote
// stripping, block-scalar refusal and a small parser for two GitHub expression
// shapes -- and grew a detector every time YAML permitted an equivalent
// spelling the previous scan had not anticipated (stokaro/ptah#2511). A quoted
// value, a folded scalar and a flow mapping are all just values here.
package gotoolchain

import (
	"fmt"
	"os"
	"strings"
)

// Finding is one violation, addressed the way a GitHub annotation is.
type Finding struct {
	File    string
	Line    int
	Message string
}

func (f Finding) String() string {
	return fmt.Sprintf("::error file=%s,line=%d::%s", f.File, f.Line, f.Message)
}

// Module is what a go.mod declares about versions.
type Module struct {
	// Go is the compatibility floor, and Toolchain is what CI builds with.
	// They are different facts with different lifecycles.
	Go, Toolchain string
	// ToolchainLine is where the directive sits, for a diagnostic.
	ToolchainLine int
	// Toolchains counts the directives, because two is as wrong as none.
	Toolchains int
	// GoLine is where the go directive sits.
	GoLine int
}

// ReadModule parses the directives this gate is about.
//
// Line parsing rather than a module parser: `go` and `toolchain` are
// single-token directives at column zero, and what this needs from them --
// including how MANY there are -- is not something a parser that normalizes
// them would report.
func ReadModule(path string) (Module, error) {
	body, err := os.ReadFile(path) //#nosec G304 -- a path this repository's own checker names
	if err != nil {
		return Module{}, err
	}
	module := Module{}
	for i, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "go":
			if module.Go == "" {
				module.Go, module.GoLine = fields[1], i+1
			}
		case "toolchain":
			module.Toolchains++
			if module.Toolchain == "" {
				module.Toolchain, module.ToolchainLine = fields[1], i+1
			}
		}
	}
	return module, nil
}

// SetupGoStep is one `actions/setup-go` step, with the two version keys read.
type SetupGoStep struct {
	File string
	Line int
	// Version and VersionFile are the two keys setup-go accepts. BOTH are
	// judged, never whichever appears first: setup-go takes them on one step,
	// and the unread one is what decides when the first resolves to empty.
	Version, VersionFile         string
	VersionLine, VersionFileLine int
	HasVersion, HasVersionFile   bool
}

// Manifest is one parsed workflow or action file.
type Manifest struct {
	Path  string
	Steps []SetupGoStep
	// Inputs are a composite action's declared inputs, by name.
	Inputs map[string]Input
	// Mentions counts the lines naming actions/setup-go@ at all, which is the
	// census the step enumeration is checked against.
	Mentions int
}

// Input is one composite-action input declaration.
type Input struct {
	Name         string
	Default      string
	HasDefault   bool
	DefaultLine  int
	DeclaredLine int
}
