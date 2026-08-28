package analysisboundary_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// analysisTrees are the packages that read SQL, schemas and migrations to say
// something about them. A process started from any of these is an analyzer
// reaching outside the tool.
var analysisTrees = []string{
	"internal/sqllint",
	"internal/schemalineage",
	"internal/lintcatalog",
	"migration/lint",
	"internal/migrationlintreport",
}

// permittedProcesses are the commands an analysis path may start, each with the
// reason it is not an external analyzer.
//
// A command added here is a decision someone made. A command started and absent
// is what this file exists to catch.
var permittedProcesses = map[string]string{
	"git": "reads the merge base for --git-base; it inspects the repository, not the schema",
}

// TestAnalysisStartsNoProcessButThePermittedOnes holds the state #1270's
// criterion asked about.
//
// The criterion asks that external-process execution follow Ptah's security
// requirements. There are none written down, and nothing here starts such a
// process, so the honest thing to pin is the second half: while this stays
// true, no policy is owed, and when it stops being true this fails and says one
// is (stokaro/ptah#2395).
func TestAnalysisStartsNoProcessButThePermittedOnes(t *testing.T) {
	c := qt.New(t)

	started := processesStartedIn(c, analysisTrees)

	c.Assert(started, qt.Not(qt.HasLen), 0,
		qt.Commentf("the scan found no process at all, so it is measuring nothing"))
	for _, command := range started {
		c.Assert(permittedProcesses[command], qt.Not(qt.Equals), "",
			qt.Commentf(
				"an analysis path starts %q.\n"+
					"An analyzer that runs an external process needs a written policy to follow, "+
					"and this repository has none. Record the command here with the reason it is "+
					"not one, or write the policy first.", command))
	}
}

// TestEveryPermittedProcessIsStillStarted is the control.
//
// A permitted command nothing starts leaves an exemption standing for nothing,
// and the next reader takes the list as current.
func TestEveryPermittedProcessIsStillStarted(t *testing.T) {
	c := qt.New(t)

	started := processesStartedIn(c, analysisTrees)

	for command := range permittedProcesses {
		c.Assert(started, qt.Contains, command,
			qt.Commentf("%s is permitted and no analysis path starts it", command))
	}
}

// processesStartedIn returns the command names the given trees start, reading
// the source rather than a list of them.
func processesStartedIn(c *qt.C, trees []string) []string {
	c.Helper()
	root := repositoryRoot(c)
	commands := make([]string, 0)
	for _, tree := range trees {
		commands = appendUnique(commands, processesUnder(c, filepath.Join(root, tree))...)
	}
	return commands
}

// processesUnder walks one tree for exec.Command and exec.CommandContext calls
// and returns the literal command each names.
func processesUnder(c *qt.C, dir string) []string {
	c.Helper()
	commands := make([]string, 0)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		commands = appendUnique(commands, processesInFile(c, path)...)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return commands
}

// processesInFile reads one file's exec calls.
//
// A command built from a variable is reported under its own name, because a
// process this cannot read the name of is exactly the one a policy would have
// to cover.
func processesInFile(c *qt.C, path string) []string {
	c.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	c.Assert(err, qt.IsNil)

	commands := make([]string, 0)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := execCommandName(call)
		if !ok {
			return true
		}
		commands = appendUnique(commands, name)
		return true
	})
	return commands
}

// execCommandName returns the command a call starts, and whether it starts one.
func execCommandName(call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "exec" {
		return "", false
	}
	if selector.Sel.Name != "Command" && selector.Sel.Name != "CommandContext" {
		return "", false
	}
	return commandArgument(call, selector.Sel.Name)
}

// commandArgument reads the literal name, or reports the call under a name a
// reader can act on when it is built from a variable.
func commandArgument(call *ast.CallExpr, form string) (string, bool) {
	index := 0
	if form == "CommandContext" {
		index = 1
	}
	if len(call.Args) <= index {
		return "a command with no name", true
	}
	literal, ok := call.Args[index].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return "a command named at run time", true
	}
	return strings.Trim(literal.Value, `"`), true
}

// repositoryRoot walks up to the directory holding go.mod.
func repositoryRoot(c *qt.C) string {
	c.Helper()
	dir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	for range 8 {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	c.Fatalf("no go.mod above %s", dir)
	return ""
}

// appendUnique adds the values the slice does not already carry.
func appendUnique(values []string, more ...string) []string {
	for _, value := range more {
		if !slices.Contains(values, value) {
			values = append(values, value)
		}
	}
	return values
}
