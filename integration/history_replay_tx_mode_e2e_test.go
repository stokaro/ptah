//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// The rollback rehearsal of `ptah migrations down --shadow-db`, the derived
// rollback of `migrations down --plan`, the verification of `migrations
// baseline --shadow-db` and the dev replay of `ptah-compat migrate down
// --dev-url` each replay a target's applied history on a disposable database.
// Migration 1 holds a VACUUM, which SQLite and PostgreSQL refuse inside a
// transaction, beside a statement a transaction accepts, so it applies under
// --tx-mode none, and each replay runs it without a transaction, with no mode
// stated. PTAH_TX_MODE and the env's migration.tx_mode, which name the mode
// pending files run in, change nothing.

// historyReplayEngine is an engine the history replays on.
type historyReplayEngine struct {
	name string
	// database returns a new, empty database named for role.
	database func(c *qt.C, role string) string
	// vacuum is the VACUUM statement of migration 1.
	vacuum string
	// noteColumns counts the note columns of the items table, which
	// migration 2 adds.
	noteColumns string
}

var historyReplayEngines = []historyReplayEngine{
	{
		name: "SQLite",
		database: func(c *qt.C, role string) string {
			return atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), role+".db"))
		},
		vacuum:      "VACUUM;",
		noteColumns: "SELECT count(*) FROM pragma_table_info('items') WHERE name = 'note'",
	},
	{
		name:        "PostgreSQL",
		database:    newEmptyReadScopeDatabase,
		vacuum:      "VACUUM items;",
		noteColumns: "SELECT count(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'items' AND column_name = 'note'",
	},
}

// historyReplayCreate is migration 1's statement a transaction accepts, and
// historyReplayNote is migration 2.
const (
	historyReplayCreate = "CREATE TABLE items (id integer PRIMARY KEY, name text NOT NULL);"
	historyReplayNote   = "ALTER TABLE items ADD COLUMN note text;"
	historyReplayDrop   = "ALTER TABLE items DROP COLUMN note;"
)

// historyReplay is a target at version 2 of the history, a disposable
// database beside it, and the directory the history is in.
type historyReplay struct {
	engine historyReplayEngine
	root   string
	dir    string
	target string
	shadow string
}

// writeHistoryReplayFiles writes the history to a directory under root, named
// as format names a migration's up and down files, and returns the directory.
func writeHistoryReplayFiles(c *qt.C, root string, engine historyReplayEngine, up, down func(version, name string) string) string {
	c.Helper()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		up("1", "items"):   historyReplayCreate + "\n" + engine.vacuum + "\n",
		down("1", "items"): "DROP TABLE items;\n",
		up("2", "note"):    historyReplayNote + "\n",
		down("2", "note"):  historyReplayDrop + "\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	return dir
}

// nativeHistoryReplay writes the history in the Ptah layout and applies it
// with `migrations up --tx-mode none`.
func nativeHistoryReplay(c *qt.C, engine historyReplayEngine) historyReplay {
	c.Helper()
	h := newHistoryReplay(c, engine)
	h.dir = writeHistoryReplayFiles(c, h.root, engine,
		func(version, name string) string { return "000000000" + version + "_" + name + ".up.sql" },
		func(version, name string) string { return "000000000" + version + "_" + name + ".down.sql" })
	applied := runHistoryReplay(c, clirun.Ptah, h, nil,
		"migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir, "--tx-mode", "none")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))
	return h
}

// baselineHistoryReplay writes the history in the Ptah layout and runs its
// statements on the target outside any migration, so the target holds
// version 2 and no revision row.
func baselineHistoryReplay(c *qt.C, engine historyReplayEngine) historyReplay {
	c.Helper()
	h := newHistoryReplay(c, engine)
	h.dir = writeHistoryReplayFiles(c, h.root, engine,
		func(version, name string) string { return "000000000" + version + "_" + name + ".up.sql" },
		func(version, name string) string { return "000000000" + version + "_" + name + ".down.sql" })
	for _, statement := range []string{historyReplayCreate, historyReplayNote} {
		compatDriftExec(c, h.target, statement)
	}
	return h
}

// compatHistoryReplay writes the history in the Atlas layout, hashes it, and
// applies it with `ptah-compat migrate apply --tx-mode none`.
func compatHistoryReplay(c *qt.C, engine historyReplayEngine) historyReplay {
	c.Helper()
	h := newHistoryReplay(c, engine)
	h.dir = writeHistoryReplayFiles(c, h.root, engine,
		func(version, name string) string { return version + "_" + name + ".sql" },
		func(version, name string) string { return version + "_" + name + ".down.sql" })
	hashed := runHistoryReplay(c, clirun.Compat, h, nil, "migrate", "hash", "--dir", "file://migrations")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))
	applied := runHistoryReplay(c, clirun.Compat, h, nil,
		"migrate", "apply", "--url", h.target, "--dir", "file://migrations", "--tx-mode", "none")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))
	return h
}

func newHistoryReplay(c *qt.C, engine historyReplayEngine) historyReplay {
	c.Helper()
	return historyReplay{
		engine: engine,
		root:   c.TempDir(),
		target: engine.database(c, "history_target"),
		shadow: engine.database(c, "history_shadow"),
	}
}

// runHistoryReplay runs program in the history's root, in an environment that
// carries no PTAH_ variable but env.
func runHistoryReplay(c *qt.C, program clirun.Target, h historyReplay, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, program, clirun.Options{Dir: h.root, Env: append(environmentWithoutPtahVariables(), env...)}, args...)
}

// historyReplayNoteColumns counts the note columns the target's items table
// holds: 1 at version 2, 0 once migration 2 is rolled back.
func historyReplayNoteColumns(c *qt.C, h historyReplay) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), h.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), h.engine.noteColumns).Scan(&count), qt.IsNil)
	return count
}

// historyReplaySurface is one command that replays the history.
type historyReplaySurface struct {
	name string
	// history prepares the target the command runs on.
	history func(c *qt.C, engine historyReplayEngine) historyReplay
	program clirun.Target
	// args is the command line, given the history.
	args func(h historyReplay) []string
	// stdout is what the run prints once it succeeds, and note the note
	// columns the target then holds.
	stdout string
	note   int
}

var historyReplaySurfaces = []historyReplaySurface{
	{
		name:    "ptah migrations down --shadow-db",
		history: nativeHistoryReplay,
		program: clirun.Ptah,
		args: func(h historyReplay) []string {
			return []string{"migrations", "down", "--db-url", h.target, "--migrations-dir", h.dir,
				"--shadow-db", h.shadow, "--target", "1", "--confirm"}
		},
		stdout: "Rollback plan verified on shadow database",
		note:   0,
	},
	{
		name:    "ptah migrations down --plan",
		history: nativeHistoryReplay,
		program: clirun.Ptah,
		args: func(h historyReplay) []string {
			return []string{"migrations", "down", "--db-url", h.target, "--migrations-dir", h.dir,
				"--shadow-db", h.shadow, "--plan", "--target", "1", "--confirm"}
		},
		stdout: "Derived rollback plan:",
		note:   0,
	},
	{
		name:    "ptah migrations baseline --shadow-db",
		history: baselineHistoryReplay,
		program: clirun.Ptah,
		args: func(h historyReplay) []string {
			return []string{"migrations", "baseline", "--db-url", h.target, "--migrations-dir", h.dir,
				"--shadow-db", h.shadow}
		},
		stdout: "Baselined 2 migration(s) through version 2",
		note:   1,
	},
	{
		name:    "ptah-compat migrate down --dev-url",
		history: compatHistoryReplay,
		program: clirun.Compat,
		args: func(h historyReplay) []string {
			return []string{"migrate", "down", "--url", h.target, "--dir", "file://migrations",
				"--dev-url", h.shadow, "--to-version", "1"}
		},
		stdout: "Rollback plan verified on shadow database",
		note:   0,
	},
	{
		name:    "ptah-compat migrate down --dev-url --format",
		history: compatHistoryReplay,
		program: clirun.Compat,
		args: func(h historyReplay) []string {
			return []string{"migrate", "down", "--url", h.target, "--dir", "file://migrations",
				"--dev-url", h.shadow, "--to-version", "1", "--format", "{{ .Current }}->{{ .Target }}"}
		},
		stdout: "2->1",
		note:   0,
	},
}

// TestHistoryReplayTxModeE2E_HappyPath replays the history with no mode
// stated, and with PTAH_TX_MODE naming file: each command does what it was
// asked.
func TestHistoryReplayTxModeE2E_HappyPath(t *testing.T) {
	for _, surface := range historyReplaySurfaces {
		for _, engine := range historyReplayEngines {
			for _, env := range [][]string{nil, {"PTAH_TX_MODE=file"}} {
				t.Run(surface.name+"/"+engine.name+"/"+strings.Join(env, " "), func(t *testing.T) {
					c := qt.New(t)
					h := surface.history(c, engine)

					got := runHistoryReplay(c, surface.program, h, env, surface.args(h)...)

					c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stdout, qt.Contains, surface.stdout)
					c.Assert(historyReplayNoteColumns(c, h), qt.Equals, surface.note)
				})
			}
		}
	}
}

// historyReplayProject writes an atlas.hcl whose env "local" names the
// history's target, dev database and directory, with migration lines after
// the directory.
func historyReplayProject(c *qt.C, h historyReplay, migration string) {
	c.Helper()
	project := "env \"local\" {\n" +
		"  url = " + strconv.Quote(h.target) + "\n" +
		"  dev = " + strconv.Quote(h.shadow) + "\n" +
		"  migration {\n    dir = \"file://migrations\"\n" + migration + "  }\n" +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(h.root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
}

// historyReplayProjectPaths are the two compatibility rollbacks: the one
// forwarded to `migrations down`, and the --format one, which builds its own
// rehearsal.
var historyReplayProjectPaths = []struct {
	name   string
	flags  []string
	stdout string
}{
	{name: "forwarded", stdout: "Rollback plan verified on shadow database"},
	{name: "--format", flags: []string{"--format", "{{ .Current }}->{{ .Target }}"}, stdout: "2->1"},
}

// The env's migration.tx_mode names the mode pending files run in, and the
// rollback's replay reads it on neither path.
func TestHistoryReplayProjectTxModeE2E_HappyPath(t *testing.T) {
	for _, rollback := range historyReplayProjectPaths {
		for _, engine := range historyReplayEngines {
			t.Run(rollback.name+"/"+engine.name, func(t *testing.T) {
				c := qt.New(t)
				h := compatHistoryReplay(c, engine)
				historyReplayProject(c, h, "    tx_mode = \"file\"\n")

				got := runHistoryReplay(c, clirun.Compat, h, nil,
					append([]string{"migrate", "down", "--env", "local", "--to-version", "1"}, rollback.flags...)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout, qt.Contains, rollback.stdout)
				c.Assert(historyReplayNoteColumns(c, h), qt.Equals, 0)
			})
		}
	}
}
