//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// memoryApplyCase is a schema apply with the target and the dev database in
// memory. urlFlag is the binary's spelling of the target flag.
type memoryApplyCase struct {
	name    string
	binary  clirun.Target
	urlFlag string
	target  string
	dev     string
}

// memoryApplyArgs is the schema apply of test, with the migration directory
// at migrations as the desired state.
func memoryApplyArgs(test memoryApplyCase, migrations string) []string {
	return []string{
		"schema", "apply", test.urlFlag, test.target, "--dev-url", test.dev,
		"--to", "file://" + filepath.ToSlash(migrations), "--auto-approve",
	}
}

// TestSchemaApplyInMemoryDevDatabaseE2E_HappyPath applies the history to a
// target in memory with a dev database in memory. Two private in-memory
// databases are two databases, under one URL too, and so are two shared-cache
// names, so the shipped binaries replay the history on the dev database and
// apply the plan, as the pinned community binary does for each pair. The
// target lives in the process, so what the run applied is read from the plan
// it printed.
func TestSchemaApplyInMemoryDevDatabaseE2E_HappyPath(t *testing.T) {
	tests := []memoryApplyCase{
		{
			name: "native, two private names", binary: clirun.Ptah, urlFlag: "--db-url",
			target: "sqlite://target?mode=memory", dev: "sqlite://dev?mode=memory",
		},
		{
			name: "native, two private databases under one URL", binary: clirun.Ptah, urlFlag: "--db-url",
			target: "sqlite://db?mode=memory", dev: "sqlite://db?mode=memory",
		},
		{
			name: "native, two shared-cache names", binary: clirun.Ptah, urlFlag: "--db-url",
			target: "sqlite://target?mode=memory&cache=shared", dev: "sqlite://dev?mode=memory&cache=shared",
		},
		{
			name: "compatibility, two private names", binary: clirun.Compat, urlFlag: "--url",
			target: "sqlite://target?mode=memory", dev: "sqlite://dev?mode=memory",
		},
		{
			name: "compatibility, two private databases under one URL", binary: clirun.Compat, urlFlag: "--url",
			target: "sqlite://db?mode=memory", dev: "sqlite://db?mode=memory",
		},
		{
			name: "compatibility, two shared-cache names", binary: clirun.Compat, urlFlag: "--url",
			target: "sqlite://target?mode=memory&cache=shared", dev: "sqlite://dev?mode=memory&cache=shared",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := writeMemoryDevHistory(c, dir)
			opts := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.binary, opts, memoryApplyArgs(test, migrations)...)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, `CREATE TABLE "c"`)
			c.Assert(applied.Stdout, qt.Contains, "Schema apply completed successfully.\n")
		})
	}
}

// TestSchemaApplyInMemoryDevDatabaseE2E_FailurePath names one shared-cache
// database as the target and the dev database. It is one database in the
// process, so the dev database's reset would empty the target, and both
// binaries refuse the pair. The pinned community binary applies it: we are
// stricter here, deliberately.
//
// SQLite applies the last cache value of a URL that gives two, so a name whose
// last cache is shared is the same one database, and is refused too.
func TestSchemaApplyInMemoryDevDatabaseE2E_FailurePath(t *testing.T) {
	const shared = "sqlite://db?mode=memory&cache=shared"
	const lastShared = "sqlite://db?mode=memory&cache=private&cache=shared"
	tests := []struct {
		memoryApplyCase
		wantExit int
	}{
		{
			memoryApplyCase: memoryApplyCase{
				name: "native", binary: clirun.Ptah, urlFlag: "--db-url", target: shared, dev: shared,
			},
			wantExit: 2,
		},
		{
			memoryApplyCase: memoryApplyCase{
				name: "compatibility", binary: clirun.Compat, urlFlag: "--url", target: shared, dev: shared,
			},
			wantExit: 1,
		},
		{
			memoryApplyCase: memoryApplyCase{
				name: "native, the last cache shared", binary: clirun.Ptah, urlFlag: "--db-url", target: lastShared, dev: lastShared,
			},
			wantExit: 2,
		},
		{
			memoryApplyCase: memoryApplyCase{
				name: "compatibility, the last cache shared", binary: clirun.Compat, urlFlag: "--url", target: lastShared, dev: lastShared,
			},
			wantExit: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := writeMemoryDevHistory(c, dir)
			opts := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

			applied := clirun.Run(c, test.binary, opts, memoryApplyArgs(test.memoryApplyCase, migrations)...)

			c.Assert(applied.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, "--dev-url must not point at the target database")
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "Schema apply completed successfully.")
		})
	}
}
