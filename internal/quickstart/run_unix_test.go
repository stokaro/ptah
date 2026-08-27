//go:build !windows

package quickstart_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/quickstart"
)

// The PowerShell half of this file is the windows-latest leg of
// .github/workflows/quickstart-acceptance.yml, which runs the published pages
// rather than this fixture. A Bash-only assertion made to compile on Windows
// would run nothing there and stay green, which is the shape AGENTS.md refuses.

// TestRun_HappyPath drives the whole pipeline: extraction, script generation, a
// real shell, and the assertions. The fixture's commands are plain shell, so
// this needs no ptah binary and no database.
func TestRun_HappyPath(t *testing.T) {
	c := qt.New(t)

	page := loadPage(c, optedInPage)

	result, err := quickstart.Run(context.Background(), page, quickstart.Bash, quickstart.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Failures, qt.HasLen, 0)
	c.Assert(result.ExitCode, qt.Equals, 0)
	c.Assert(result.Steps, qt.Equals, 3)
	c.Assert(result.Asserted, qt.Equals, 2)
	c.Assert(result.OK(), qt.IsTrue)
}

// TestRun_FailurePath edits the page rather than the code, because that is the
// direction this runner exists to catch: the page says one thing and the
// command does another.
func TestRun_FailurePath(t *testing.T) {
	c := qt.New(t)

	path := rewritten(c, optedInPage,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n```\n\n## Say where it went",
		"CREATE TABLE users (id BIGINT);\n```\n\n## Say where it went")
	page := loadPage(c, path)

	result, err := quickstart.Run(context.Background(), page, quickstart.Bash, quickstart.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsFalse)
	c.Assert(result.Failures, qt.HasLen, 1)
	c.Assert(result.Failures[0].Step, qt.Equals, 2)
	c.Assert(result.Failures[0].Missing, qt.Equals, "CREATE TABLE users (id BIGINT);")
	c.Assert(result.Failures[0].Got, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
}

// TestRun_AFailingCommandStopsTheRun pins the property `set -euo pipefail`
// buys: a step that fails ends the page there rather than letting every later
// step run against a state the reader would never have reached.
func TestRun_AFailingCommandStopsTheRun(t *testing.T) {
	c := qt.New(t)

	path := rewritten(c, optedInPage, "cat schema.sql", "cat schema.sql --no-such-option")
	page := loadPage(c, path)

	result, err := quickstart.Run(context.Background(), page, quickstart.Bash, quickstart.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsFalse)
	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(result.Failures, qt.HasLen, 1)
	c.Assert(result.Failures[0].Step, qt.Equals, 2)
	c.Assert(result.Failures[0].Problem, qt.Equals, "the step did not finish; the run stopped here")
}

// rewritten copies a fixture page with one substitution and returns the copy's
// path.
func rewritten(c *qt.C, path, from, to string) string {
	source, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	edited := strings.Replace(string(source), from, to, 1)
	c.Assert(edited, qt.Not(qt.Equals), string(source))

	// A fixed name: the copy's file name is not part of what is under test.
	copied := filepath.Join(c.TempDir(), "page.mdx")
	// #nosec G703 -- copied is filepath.Join of this subtest's own TempDir and a
	// constant name; nothing from the fixture reaches the path.
	c.Assert(os.WriteFile(copied, []byte(edited), 0o600), qt.IsNil)
	return copied
}
