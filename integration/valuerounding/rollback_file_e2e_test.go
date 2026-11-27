//go:build integration

package valuerounding_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

const (
	rollbackNarrowTable = "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,1));\n"
	rollbackWideTable   = "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3));\n"
	rollbackInitialRow  = "INSERT INTO prices VALUES (1, 1.5);\n"
	rollbackLaterRows   = "INSERT INTO prices VALUES (2, 1.567), (3, -1.567);\n"
	rollbackRead        = "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"
	rollbackHeld        = "1.500,1.567,-1.567"
	rollbackRounded     = "1.5,1.6,-1.6"
)

// rollbackSurface selects each shipped path that executes a generated down
// file, including the compatibility path that builds its own formatted run.
type rollbackSurface struct {
	name    string
	program clirun.Target
	apply   []string
	down    []string
	shadow  []string
	allow   []string
	env     []string
	failure int
	request string
}

var rollbackSurfaces = []rollbackSurface{
	{
		name: "native", program: clirun.Ptah,
		apply: []string{"migrations", "up", "--db-url", "{target}", "--migrations-dir", "{dir}", "--dir-format", "atlas"},
		down: []string{
			"migrations", "down", "--db-url", "{target}", "--migrations-dir", "{dir}", "--dir-format", "atlas",
			"--limit", "1", "--confirm",
		},
		shadow: []string{"--shadow-db", "{shadow}"}, allow: []string{"--allow-value-rounding"},
		failure: 2, request: "--allow-value-rounding",
	},
	{
		name: "compat forwarded", program: clirun.Compat,
		apply:  []string{"migrate", "apply", "--url", "{target}", "--dir", "{atlas}", "--allow-dirty"},
		down:   []string{"migrate", "down", "--url", "{target}", "--dir", "{atlas}"},
		shadow: []string{"--dev-url", "{shadow}"}, env: []string{"PTAH_ALLOW_VALUE_ROUNDING=1"},
		failure: 1, request: "PTAH_ALLOW_VALUE_ROUNDING=1",
	},
	{
		name: "compat formatted", program: clirun.Compat,
		apply:  []string{"migrate", "apply", "--url", "{target}", "--dir", "{atlas}", "--allow-dirty"},
		down:   []string{"migrate", "down", "--url", "{target}", "--dir", "{atlas}", "--format", "{{ .Error }}"},
		shadow: []string{"--dev-url", "{shadow}"}, env: []string{"PTAH_ALLOW_VALUE_ROUNDING=1"},
		failure: 1, request: "PTAH_ALLOW_VALUE_ROUNDING=1",
	},
}

func (s rollbackSurface) run(c *qt.C, target, dir, shadow string, args, env []string) clirun.Result {
	c.Helper()
	replace := strings.NewReplacer("{target}", target, "{dir}", dir,
		"{atlas}", "file://"+filepath.ToSlash(dir), "{shadow}", shadow)
	resolved := make([]string, len(args))
	for i, arg := range args {
		resolved[i] = replace.Replace(arg)
	}
	return clirun.Run(c, s.program, clirun.Options{Dir: c.TempDir(), Env: append(os.Environ(), env...)}, resolved...)
}

// wideningDirectory uses the real generator while every value fits the old
// type. Its down check must protect values written after the up file runs.
func wideningDirectory(c *qt.C, target string) string {
	c.Helper()
	result, dir := generate(c, roundingEngine{desired: rollbackWideTable}, target)
	c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
	return dir
}

// writeRollbackBody edits the one generated file in the requested direction.
// The opened root keeps the name returned by Glob within the fixture directory.
func writeRollbackBody(c *qt.C, dir, direction, before, after string) {
	c.Helper()
	root, err := os.OpenRoot(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(root.Close(), qt.IsNil) })
	files, err := fs.Glob(root.FS(), "*."+direction+".sql")
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	body, err := root.ReadFile(files[0])
	c.Assert(err, qt.IsNil)
	c.Assert(root.WriteFile(files[0], []byte(before+string(body)+after), 0o600), qt.IsNil)
}

func hashRollbackDirectory(c *qt.C, dir string) {
	c.Helper()
	result := compat(c, nil, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
}

// widenedTarget applies the generated widening through the selected surface,
// with a row that still fits the original type and an optional ordinary check.
func widenedTarget(c *qt.C, surface rollbackSurface, downPreamble string) (target, dir string) {
	c.Helper()
	target = postgresServer.scratch(c, rollbackNarrowTable, rollbackInitialRow)
	dir = wideningDirectory(c, target)
	writeRollbackBody(c, dir, "down", downPreamble, "")
	hashRollbackDirectory(c, dir)
	applied := surface.run(c, target, dir, "", surface.apply, nil)
	c.Assert(applied.ExitCode, qt.Equals, 0, outcome(applied))
	return target, dir
}

func TestMigrationsDownRefusesValuesInsertedAfterWideningE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, dir := widenedTarget(c, surface, "")
			run(c, target, rollbackLaterRows)

			result := surface.run(c, target, dir, "", surface.down, nil)

			c.Assert(result.ExitCode, qt.Equals, surface.failure, outcome(result))
			c.Assert(result.Stderr, qt.Contains, checkRefusal)
			c.Assert(result.Stderr, qt.Contains, "or ask for the change with "+surface.request)
			c.Assert(result.Stderr, qt.Not(qt.Contains), "--skip-checks")
			c.Assert(read(c, target, rollbackRead), qt.Equals, rollbackHeld)
		})
	}
}

func TestMigrationsDownRoundsValuesOnRequestE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, dir := widenedTarget(c, surface, "")
			run(c, target, rollbackLaterRows)

			result := surface.run(c, target, dir, "", append(slices.Clone(surface.down), surface.allow...), surface.env)

			c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
			c.Assert(result.Stderr, qt.Contains, skippedCheck)
			c.Assert(read(c, target, rollbackRead), qt.Equals, rollbackRounded)
		})
	}
}

func TestMigrationsDownKeepsFittingValuesE2E(t *testing.T) {
	for _, surface := range rollbackSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)
			target, dir := widenedTarget(c, surface, "")

			result := surface.run(c, target, dir, "", surface.down, nil)

			c.Assert(result.ExitCode, qt.Equals, 0, outcome(result))
			c.Assert(result.Stderr, qt.Not(qt.Contains), skippedCheck)
			c.Assert(read(c, target, rollbackRead), qt.Equals, "1.5")
		})
	}
}
