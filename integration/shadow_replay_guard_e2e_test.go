//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// Every command that replays a migration history on a shadow database runs
// the replay through the dev replay guard `migrations validate` runs: the
// rollback rehearsal of `migrations down --shadow-db`, the derived rollback of
// `migrations down --plan`, the verification of `migrations baseline
// --shadow-db`, the drift checks of `migrations up --check-drift`,
// `migrations drift` and an atlas.hcl drift block on `ptah-compat migrate
// apply`, the verification of `migrations generate --shadow-db`, the replay of
// `migrations checkpoint`, and the rehearsals of `ptah-compat migrate down
// --dev-url`, forwarded and with --format.
//
// Migration 1 of the history writes outside the shadow database: on MySQL it
// creates a table in another database on the same server, and on PostgreSQL a
// DO block creates a role. Neither server is the run's own, so each command
// refuses the replay before its first statement and the other database, or the
// server's roles, read back unchanged, as do the target and the migration
// directory. Two controls show the refusal is the guard's: the same commands
// replay a history without that statement, and with the operator's trust in the
// replayed SQL they replay the statement itself.

// shadowGuardEngine is a server the commands replay on.
type shadowGuardEngine struct {
	name string
	// database returns a new, empty database named for role.
	database func(c *qt.C, role string) string
	// outside prepares what migration 1 writes outside the shadow database,
	// and returns the statement that writes it and the query that counts it.
	outside func(c *qt.C) shadowGuardOutside
	// tables lists the tables of the database a connection opens, in name
	// order, the revision tables included.
	tables string
}

// shadowGuardOutside is the write outside the shadow database: the statement
// migration 1 carries, and a count of what it wrote, read on the server.
type shadowGuardOutside struct {
	statement string
	count     func(c *qt.C) int
}

var shadowGuardEngines = []shadowGuardEngine{
	{
		name: "MySQL",
		database: func(c *qt.C, role string) string {
			_, url := newMySQLScratch(c).database(c, "sguard_"+role)
			return url
		},
		outside: mySQLShadowGuardOutside,
		tables:  "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY table_name",
	},
	{
		name:     "PostgreSQL",
		database: func(c *qt.C, role string) string { return newEmptyReadScopeDatabase(c, "sguard_"+role) },
		outside:  postgresShadowGuardOutside,
		tables: "SELECT table_schema || '.' || table_name FROM information_schema.tables " +
			"WHERE table_schema NOT IN ('pg_catalog', 'information_schema') ORDER BY 1",
	},
}

// mySQLShadowGuardOutside is a database of its own on the MySQL server, which
// migration 1 creates a table in.
func mySQLShadowGuardOutside(c *qt.C) shadowGuardOutside {
	c.Helper()
	scratch := newMySQLScratch(c)
	name, _ := scratch.database(c, "sguard_other")
	return shadowGuardOutside{
		statement: "CREATE TABLE IF NOT EXISTS `" + name + "`.`replayed` (id integer PRIMARY KEY);",
		count: func(c *qt.C) int {
			c.Helper()
			var count int
			c.Assert(scratch.admin.QueryRowContext(c.Context(),
				"SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'replayed'", name,
			).Scan(&count), qt.IsNil)
			return count
		},
	}
}

// postgresShadowGuardOutside is a role on the PostgreSQL server, which a DO
// block in migration 1 creates when it is missing. The role outlives every
// database, so it is dropped when the test ends.
func postgresShadowGuardOutside(c *qt.C) shadowGuardOutside {
	c.Helper()
	role := fmt.Sprintf("ptah_sguard_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbtarget.URL(c, dbtarget.PostgreSQL)))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return shadowGuardOutside{
		statement: "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '" + role + "') THEN " +
			`CREATE ROLE "` + role + `"; END IF; END $$;`,
		count: func(c *qt.C) int {
			c.Helper()
			var count int
			c.Assert(admin.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_roles WHERE rolname = $1", role).Scan(&count), qt.IsNil)
			return count
		},
	}
}

// shadowGuardRefusal is the refusal of migration 1 on a server the run
// neither owns nor trusts the SQL on: the guard's, naming trust as a remedy.
const shadowGuardRefusal = `(?s).*migration replay rejects .* because its effects cannot be confined to the disposable database realm; .*` +
	`if you trust this SQL to run with the dev connection's privileges, set PTAH_TRUST_REPLAY_SQL=1\nSQL: .*`

// shadowGuardRun is a target, a shadow database beside it, the directory the
// history is in, and the write outside the shadow database migration 1 makes.
type shadowGuardRun struct {
	engine                    shadowGuardEngine
	root, dir, target, shadow string
	outside                   shadowGuardOutside
}

// shadowGuardLayout names a migration file in a layout. An Atlas directory is
// hashed into atlas.sum, which ptah-compat reads; a Ptah directory carries no
// checksum file.
type shadowGuardLayout struct {
	up, down func(version, name string) string
	hashed   bool
}

var (
	shadowGuardPtahLayout = shadowGuardLayout{
		up:   func(version, name string) string { return "000000000" + version + "_" + name + ".up.sql" },
		down: func(version, name string) string { return "000000000" + version + "_" + name + ".down.sql" },
	}
	shadowGuardAtlasLayout = shadowGuardLayout{
		up:     func(version, name string) string { return version + "_" + name + ".sql" },
		down:   func(version, name string) string { return version + "_" + name + ".down.sql" },
		hashed: true,
	}
)

// shadowGuardItems and shadowGuardNotes are the tables migrations 1 and 2
// create, and shadowGuardExtra the table the generated migration adds.
const (
	shadowGuardItems = "CREATE TABLE items (id integer PRIMARY KEY);"
	shadowGuardNotes = "CREATE TABLE notes (id integer PRIMARY KEY);"
	shadowGuardExtra = "CREATE TABLE extra (id integer PRIMARY KEY);"
)

// shadowGuardTarget is the state a command finds its target in.
type shadowGuardTarget struct {
	// tables are created on the target outside any migration.
	tables []string
	// recorded is the version the revision table names, set without running
	// a migration; empty records nothing.
	recorded string
}

var (
	shadowGuardAtVersion2 = shadowGuardTarget{tables: []string{shadowGuardItems, shadowGuardNotes}, recorded: "2"}
	shadowGuardAtVersion1 = shadowGuardTarget{tables: []string{shadowGuardItems}, recorded: "1"}
	shadowGuardUnrecorded = shadowGuardTarget{tables: []string{shadowGuardItems, shadowGuardNotes}}
)

// shadowGuardSurface is one command that replays the history on the shadow
// database.
type shadowGuardSurface struct {
	name    string
	program clirun.Target
	layout  shadowGuardLayout
	target  shadowGuardTarget
	// args is the command line, given the run.
	args func(r shadowGuardRun) []string
	// project is the atlas.hcl the run's directory holds, given the run;
	// nil writes none.
	project func(r shadowGuardRun) string
	// trustArgs and trustEnv declare trust in the replayed SQL.
	trustArgs, trustEnv []string
}

// nativeShadowGuardTrust and compatShadowGuardTrust are the declarations of
// trust each binary reads: the native flag, and the variable ptah-compat
// reads without one.
var (
	nativeShadowGuardTrust = []string{"--trust-replay-sql"}
	compatShadowGuardTrust = []string{"PTAH_TRUST_REPLAY_SQL=1"}
)

var shadowGuardSurfaces = []shadowGuardSurface{
	{
		name: "ptah migrations down --shadow-db", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardAtVersion2, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "down", "--db-url", r.target, "--migrations-dir", r.dir,
				"--shadow-db", r.shadow, "--target", "1", "--confirm"}
		},
	},
	{
		name: "ptah migrations down --plan", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardAtVersion2, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "down", "--db-url", r.target, "--migrations-dir", r.dir,
				"--shadow-db", r.shadow, "--plan", "--target", "1", "--confirm"}
		},
	},
	{
		name: "ptah migrations baseline --shadow-db", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardUnrecorded, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "baseline", "--db-url", r.target, "--migrations-dir", r.dir,
				"--shadow-db", r.shadow}
		},
	},
	{
		name: "ptah migrations up --check-drift", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardAtVersion1, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "up", "--db-url", r.target, "--migrations-dir", r.dir,
				"--check-drift", "--shadow-db", r.shadow}
		},
	},
	{
		name: "ptah migrations drift", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardAtVersion2, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "drift", "--db-url", r.target, "--migrations-dir", r.dir,
				"--shadow-db", r.shadow}
		},
	},
	{
		name: "ptah migrations generate --shadow-db", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		target: shadowGuardAtVersion2, trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "generate", "--db-url", r.target, "--migrations-dir", r.dir,
				"--schema-file", filepath.Join(r.root, "desired.sql"), "--shadow-db", r.shadow, "--name", "extra"}
		},
	},
	{
		name: "ptah migrations checkpoint", program: clirun.Ptah, layout: shadowGuardPtahLayout,
		trustArgs: nativeShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrations", "checkpoint", "--migrations-dir", r.dir, "--shadow-db", r.shadow}
		},
	},
	{
		name: "ptah-compat migrate down --dev-url", program: clirun.Compat, layout: shadowGuardAtlasLayout,
		target: shadowGuardAtVersion2, trustEnv: compatShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrate", "down", "--url", r.target, "--dir", "file://migrations",
				"--dev-url", r.shadow, "--to-version", "1"}
		},
	},
	{
		name: "ptah-compat migrate down --dev-url --format", program: clirun.Compat, layout: shadowGuardAtlasLayout,
		target: shadowGuardAtVersion2, trustEnv: compatShadowGuardTrust,
		args: func(r shadowGuardRun) []string {
			return []string{"migrate", "down", "--url", r.target, "--dir", "file://migrations",
				"--dev-url", r.shadow, "--to-version", "1", "--format", "{{ .Current }}->{{ .Target }}"}
		},
	},
	{
		name: "ptah-compat migrate apply with a declared drift check", program: clirun.Compat, layout: shadowGuardAtlasLayout,
		target: shadowGuardAtVersion1, trustEnv: compatShadowGuardTrust,
		args: func(shadowGuardRun) []string { return []string{"migrate", "apply", "--env", "local"} },
		project: func(r shadowGuardRun) string {
			return "env \"local\" {\n" +
				"  url = " + strconv.Quote(r.target) + "\n" +
				"  dev = " + strconv.Quote(r.shadow) + "\n" +
				"  migration {\n    dir = \"file://migrations\"\n  }\n" +
				compatDriftBlock +
				"}\n"
		},
	},
}

// newShadowGuardRun writes the history in surface's layout, with migration 1
// carrying first after the table it creates, prepares the target as surface
// says, and returns the run.
func newShadowGuardRun(c *qt.C, engine shadowGuardEngine, surface shadowGuardSurface, outside shadowGuardOutside, first string) shadowGuardRun {
	c.Helper()
	r := shadowGuardRun{
		engine:  engine,
		root:    c.TempDir(),
		target:  engine.database(c, "target"),
		shadow:  engine.database(c, "shadow"),
		outside: outside,
	}
	r.dir = filepath.Join(r.root, "migrations")
	c.Assert(os.MkdirAll(r.dir, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		surface.layout.up("1", "items"):   shadowGuardItems + "\n" + first + "\n",
		surface.layout.down("1", "items"): "DROP TABLE items;\n",
		surface.layout.up("2", "notes"):   shadowGuardNotes + "\n",
		surface.layout.down("2", "notes"): "DROP TABLE notes;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(r.dir, name), []byte(body), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	desired := strings.Join([]string{shadowGuardItems, shadowGuardNotes, shadowGuardExtra}, "\n") + "\n"
	c.Assert(os.WriteFile(filepath.Join(r.root, "desired.sql"), []byte(desired), 0o600), qt.IsNil)
	if surface.layout.hashed {
		hashed := runShadowGuard(c, clirun.Compat, r, nil, "migrate", "hash", "--dir", "file://migrations")
		c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))
	}
	shadowGuardPrepareTarget(c, surface, r)
	return r
}

// shadowGuardPrepareTarget creates surface's target tables on r's target and
// records its version, in the revision table the surface's binary reads.
func shadowGuardPrepareTarget(c *qt.C, surface shadowGuardSurface, r shadowGuardRun) {
	c.Helper()
	for _, statement := range surface.target.tables {
		compatDriftExec(c, r.target, statement)
	}
	if surface.target.recorded == "" {
		return
	}
	setters := map[clirun.Target][]string{
		clirun.Ptah:   {"migrations", "set", "--db-url", r.target, "--migrations-dir", r.dir, "--version", surface.target.recorded},
		clirun.Compat: {"migrate", "set", "--url", r.target, "--dir", "file://migrations", surface.target.recorded},
	}
	recorded := runShadowGuard(c, surface.program, r, nil, setters[surface.program]...)
	c.Assert(recorded.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", recorded.Stdout, recorded.Stderr))
}

// runShadowGuard runs program in the run's root, in an environment that
// carries no PTAH_ variable but env.
func runShadowGuard(c *qt.C, program clirun.Target, r shadowGuardRun, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, program, clirun.Options{Dir: r.root, Env: append(environmentWithoutPtahVariables(), env...)}, args...)
}

// runShadowGuardSurface writes the surface's project, when it has one, and
// runs it with extraArgs after its command line.
func runShadowGuardSurface(c *qt.C, surface shadowGuardSurface, r shadowGuardRun, env, extraArgs []string) clirun.Result {
	c.Helper()
	if surface.project != nil {
		c.Assert(os.WriteFile(filepath.Join(r.root, "atlas.hcl"), []byte(surface.project(r)), 0o600), qt.IsNil)
	}
	return runShadowGuard(c, surface.program, r, env, append(surface.args(r), extraArgs...)...)
}

// shadowGuardCensus is what the run's target and directory hold: the target's
// tables in name order, the revision tables included, and the directory's
// file names.
func shadowGuardCensus(c *qt.C, r shadowGuardRun) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), r.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), r.engine.tables)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var census []string
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		census = append(census, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	entries, err := os.ReadDir(r.dir)
	c.Assert(err, qt.IsNil)
	for _, entry := range entries {
		census = append(census, "file:"+entry.Name())
	}
	return strings.Join(census, " ")
}

// TestShadowReplayGuardE2E_FailurePath runs each command on a server the run
// neither owns nor trusts the SQL on. Each refuses migration 1 before the
// replay runs, and the write outside the shadow database, the target and the
// directory read back as they were.
func TestShadowReplayGuardE2E_FailurePath(t *testing.T) {
	for _, surface := range shadowGuardSurfaces {
		for _, engine := range shadowGuardEngines {
			t.Run(surface.name+"/"+engine.name, func(t *testing.T) {
				c := qt.New(t)
				outside := engine.outside(c)
				r := newShadowGuardRun(c, engine, surface, outside, outside.statement)
				before := shadowGuardCensus(c, r)

				got := runShadowGuardSurface(c, surface, r, nil, nil)

				c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stdout+got.Stderr, qt.Matches, shadowGuardRefusal)
				c.Assert(outside.count(c), qt.Equals, 0)
				c.Assert(shadowGuardCensus(c, r), qt.Equals, before)
			})
		}
	}
}

// TestShadowReplayGuardAcceptedHistoryE2E_HappyPath is the first control:
// without the statement that writes outside the shadow database, each command
// replays the history and does what it was asked.
func TestShadowReplayGuardAcceptedHistoryE2E_HappyPath(t *testing.T) {
	for _, surface := range shadowGuardSurfaces {
		for _, engine := range shadowGuardEngines {
			t.Run(surface.name+"/"+engine.name, func(t *testing.T) {
				c := qt.New(t)
				outside := engine.outside(c)
				r := newShadowGuardRun(c, engine, surface, outside, "")

				got := runShadowGuardSurface(c, surface, r, nil, nil)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(outside.count(c), qt.Equals, 0)
			})
		}
	}
}

// TestShadowReplayGuardTrustedE2E_HappyPath is the second control: with the
// operator's trust in the replayed SQL, the native flag or the variable
// ptah-compat reads, each command replays migration 1 and its write outside
// the shadow database, and does what it was asked.
func TestShadowReplayGuardTrustedE2E_HappyPath(t *testing.T) {
	for _, surface := range shadowGuardSurfaces {
		for _, engine := range shadowGuardEngines {
			t.Run(surface.name+"/"+engine.name, func(t *testing.T) {
				c := qt.New(t)
				outside := engine.outside(c)
				r := newShadowGuardRun(c, engine, surface, outside, outside.statement)

				got := runShadowGuardSurface(c, surface, r, surface.trustEnv, surface.trustArgs)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(outside.count(c), qt.Equals, 1)
			})
		}
	}
}
