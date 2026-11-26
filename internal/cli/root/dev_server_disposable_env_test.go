package root_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// disposableDeclarationNativeCases are the native commands that can replay a
// migration directory on a dev database, each invoked so that it stops at an
// early refusal of its own and never reaches a replay. The directory they name
// does not exist, and none of them is given a dev database.
var disposableDeclarationNativeCases = []struct {
	name string
	args func(missing string) []string
}{
	{
		name: "migrations lint",
		args: func(missing string) []string { return []string{"migrations", "lint", "--dir", missing} },
	},
	{
		name: "migrations validate",
		args: func(missing string) []string { return []string{"migrations", "validate", "--dir", missing} },
	},
	{
		name: "migrations generate",
		args: func(missing string) []string { return []string{"migrations", "generate", "--migrations-dir", missing} },
	},
	{
		name: "schema inspect",
		args: func(missing string) []string { return []string{"schema", "inspect", "--migrations-dir", missing} },
	},
	{
		name: "schema diff",
		args: func(missing string) []string {
			return []string{"schema", "diff", "--from", "file://" + missing, "--to", "file://" + missing}
		},
	},
	{
		name: "schema apply",
		args: func(missing string) []string { return []string{"schema", "apply", "--to", "file://" + missing} },
	},
}

// TestNativeReplayCommandsRefuseAMalformedDisposableDeclaration pins that every
// native command that can replay resolves PTAH_DEV_SERVER_DISPOSABLE before its
// own early refusals. Each invocation would stop on a missing directory or a
// missing database URL, so a command that read the variable only on the way
// to a replay would report that instead and leave the typo unnoticed.
func TestNativeReplayCommandsRefuseAMalformedDisposableDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
			missing := filepath.Join(t.TempDir(), "missing")

			stdout, stderr, err := executeRootCommand(test.args(missing)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+err.Error(), qt.Contains,
				`invalid boolean value "maybe" for PTAH_DEV_SERVER_DISPOSABLE`)
		})
	}
}

// TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheDeclaration is the
// control for the test above: with the variable unset, the same invocations
// fail on what they were given and say nothing about the variable. The
// refusal above is therefore the variable's, not an accident of the arguments.
func TestNativeReplayCommandsReachTheirOwnRefusalWithoutTheDeclaration(t *testing.T) {
	for _, test := range disposableDeclarationNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(devdocker.DisposableServerEnvVar)(t)
			missing := filepath.Join(t.TempDir(), "missing")

			stdout, stderr, err := executeRootCommand(test.args(missing)...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stdout+stderr+fmt.Sprint(err), qt.Not(qt.Contains), devdocker.DisposableServerEnvVar)
		})
	}
}

// TestNativeCommandsThatDoNotReplayIgnoreTheDisposableDeclaration pins the
// other half of the rule: a command that never replays does not own the
// variable, so a malformed value does not break it.
func TestNativeCommandsThatDoNotReplayIgnoreTheDisposableDeclaration(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
	dir := t.TempDir()

	stdout, stderr, err := executeRootCommand("migrations", "hash", "--dir", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
}

// shadowReplayNativeCases are the native commands that replay a migration
// history on a shadow database and on no dev database. Each runs to
// completion on SQLite, its shadow replay included: setup prepares the target
// in dir, and args is the run.
var shadowReplayNativeCases = []struct {
	name  string
	setup [][]string
	args  []string
	// tables are created on the target outside any migration.
	tables []string
}{
	{
		name: "migrations up --check-drift",
		args: []string{"migrations", "up", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations",
			"--check-drift", "--shadow-db", "sqlite://shadow.db"},
	},
	{
		name:  "migrations drift",
		setup: [][]string{{"migrations", "up", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations"}},
		args: []string{"migrations", "drift", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations",
			"--shadow-db", "sqlite://shadow.db"},
	},
	{
		name:  "migrations down --shadow-db",
		setup: [][]string{{"migrations", "up", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations"}},
		args: []string{"migrations", "down", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations",
			"--shadow-db", "sqlite://shadow.db", "--target", "0", "--confirm"},
	},
	{
		name:  "migrations down --plan",
		setup: [][]string{{"migrations", "up", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations"}},
		args: []string{"migrations", "down", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations",
			"--shadow-db", "sqlite://shadow.db", "--plan", "--target", "0", "--confirm"},
	},
	{
		name:   "migrations baseline --shadow-db",
		tables: []string{"CREATE TABLE items (id integer PRIMARY KEY)"},
		args: []string{"migrations", "baseline", "--db-url", "sqlite://target.db", "--migrations-dir", "migrations",
			"--shadow-db", "sqlite://shadow.db"},
	},
	{
		name: "migrations checkpoint",
		args: []string{"migrations", "checkpoint", "--migrations-dir", "migrations", "--shadow-db", "sqlite://shadow.db"},
	},
}

// TestNativeShadowReplayCommandsIgnoreTheDisposableDeclaration pins that a
// command which replays only on a shadow database does not own
// PTAH_DEV_SERVER_DISPOSABLE: the variable declares a dev server disposable and
// never a shadow database's, so a malformed value leaves each run, replay
// included, to succeed. A command that read the variable would refuse here, as
// the commands of TestNativeReplayCommandsRefuseAMalformedDisposableDeclaration
// do.
func TestNativeShadowReplayCommandsIgnoreTheDisposableDeclaration(t *testing.T) {
	for _, test := range shadowReplayNativeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "maybe")(t)
			t.Chdir(t.TempDir())
			writeShadowReplayHistory(c, test.tables)
			for _, args := range test.setup {
				stdout, stderr, err := executeRootCommand(args...)
				c.Assert(err, qt.IsNil, qt.Commentf("setup %v\nstdout:\n%s\nstderr:\n%s", args, stdout, stderr))
			}

			stdout, stderr, err := executeRootCommand(test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout+stderr, qt.Not(qt.Contains), devdocker.DisposableServerEnvVar)
		})
	}
}

// writeShadowReplayHistory writes a one-migration history to ./migrations
// and creates tables on ./target.db outside any migration.
func writeShadowReplayHistory(c *qt.C, tables []string) {
	c.Helper()
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"0000000001_items.up.sql":   "CREATE TABLE items (id integer PRIMARY KEY);\n",
		"0000000001_items.down.sql": "DROP TABLE items;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join("migrations", name), []byte(body), 0o600), qt.IsNil)
	}
	db, err := sql.Open("sqlite", "target.db")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	for _, statement := range tables {
		_, err := db.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
}
