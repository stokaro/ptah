package atlasschema

// White-box testing required: these tests pin the WIRING between the escape
// lint and the engine-level restrictions, which is only observable from inside
// the package. They neutralize the unexported lint seam so the remaining
// refusal can only come from the database engine, and they drive the
// unexported rehearsal core directly so the assertion covers every caller of
// it rather than one command path.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/migrator"
)

// goschemaDatabaseFixture is a non-nil desired state; these tests never reach
// the comparison that would read it.
var goschemaDatabaseFixture = goschema.Database{}

// withoutPlanLint neutralizes the escape lint for one test, so anything that
// still refuses a statement is doing so for real.
func withoutPlanLint(tb testing.TB) {
	c := qt.New(tb)
	c.Helper()
	original := checkPlanStatements
	checkPlanStatements = func([]string, string) error { return nil }
	c.Cleanup(func() { checkPlanStatements = original })
}

func connectSQLiteForWiring(tb testing.TB, dbPath string) *dbschema.DatabaseConnection {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func victimHasTable(tb testing.TB, victimPath, table string) bool {
	c := qt.New(tb)
	c.Helper()
	db, err := sql.Open("sqlite", victimPath)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	c.Assert(db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
	).Scan(&count), qt.IsNil)
	return count > 0
}

// TestRehearsalCoreRefusesEscapeWithoutTheLint is the test that says the
// containment is real. With the lint neutralized, the original ATTACH payload
// goes through the shared rehearsal core untouched, and the only thing left to
// stop it is the engine restriction the core's session carries. It fails if
// the restriction is dropped, applied to the wrong connection, or never
// verified.
func TestRehearsalCoreRefusesEscapeWithoutTheLint(t *testing.T) {
	c := qt.New(t)
	withoutPlanLint(c.TB)
	dir := t.TempDir()
	devPath := filepath.Join(dir, "dev.db")
	victimPath := filepath.Join(dir, "victim.db")

	victim, err := sql.Open("sqlite", victimPath)
	c.Assert(err, qt.IsNil)
	_, err = victim.Exec(`CREATE TABLE untouched (id INTEGER PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)
	c.Assert(victim.Close(), qt.IsNil)

	devConn := connectSQLiteForWiring(c.TB, devPath)
	targetConn := connectSQLiteForWiring(c.TB, filepath.Join(dir, "target.db"))
	statements := []string{
		fmt.Sprintf("ATTACH DATABASE '%s' AS victim", victimPath),
		"CREATE TABLE victim.pwned (id integer)",
	}

	err = rehearseStatementsOnDev(context.Background(), targetConn, devConn, nil, migrator.MigrationTxModeNone, statements)

	// The refusal comes from SQLite, not from Ptah's scanner.
	c.Assert(err, qt.IsNotNil)
	c.Assert(IsPlanEscape(err), qt.IsFalse, qt.Commentf("the lint is disabled; this refusal must come from the engine"))
	c.Assert(err.Error(), qt.Contains, "too many attached databases")
	c.Assert(victimHasTable(c.TB, victimPath, "pwned"), qt.IsFalse)
	c.Assert(victimHasTable(c.TB, victimPath, "untouched"), qt.IsTrue)
}

// TestRehearsalCoreRefusesEscapeWithoutTheLintUnderEveryTxMode covers the
// transaction modes separately: the restriction must hold whether or not the
// statements run inside a transaction.
func TestRehearsalCoreRefusesEscapeWithoutTheLintUnderEveryTxMode(t *testing.T) {
	tests := []struct {
		name   string
		txMode migrator.MigrationTxMode
	}{
		{name: "none", txMode: migrator.MigrationTxModeNone},
		{name: "file", txMode: migrator.MigrationTxModeFile},
		{name: "all", txMode: migrator.MigrationTxModeAll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			withoutPlanLint(c.TB)
			dir := t.TempDir()
			victimPath := filepath.Join(dir, "victim.db")
			devConn := connectSQLiteForWiring(c.TB, filepath.Join(dir, "dev.db"))
			targetConn := connectSQLiteForWiring(c.TB, filepath.Join(dir, "target.db"))

			err := rehearseStatementsOnDev(context.Background(), targetConn, devConn, nil, tt.txMode,
				[]string{fmt.Sprintf("ATTACH DATABASE '%s' AS victim", victimPath)})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "too many attached databases")
			_, statErr := os.Stat(victimPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("ATTACH must not have created the database file"))
		})
	}
}

// TestRehearsalCoreLintsStatementsTheEngineWouldAccept pins the lint call site
// inside the shared core, which is what gates the apply-path simulation as
// well as plan files. It uses a construct the SQLite engine accepts — the
// pragma points at a directory that exists, which the engine requires — but
// the lint refuses, so only the lint can produce this outcome.
func TestRehearsalCoreLintsStatementsTheEngineWouldAccept(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	devConn := connectSQLiteForWiring(c.TB, filepath.Join(dir, "dev.db"))
	targetConn := connectSQLiteForWiring(c.TB, filepath.Join(dir, "target.db"))

	err := rehearseStatementsOnDev(context.Background(), targetConn, devConn, nil, migrator.MigrationTxModeNone,
		[]string{fmt.Sprintf("PRAGMA temp_store_directory = '%s'", dir)})

	c.Assert(IsPlanEscape(err), qt.IsTrue, qt.Commentf("err=%v", err))
	c.Assert(err, qt.ErrorMatches, `.*statement 1 uses PRAGMA temp_store_directory.*`)
}

// TestRehearsePlanStatementsLintsBeforeTouchingTheTarget pins the second lint
// call site: the plan path refuses before it reads the target database. The
// target connection here is already closed, so reaching introspection at all
// would surface a database error instead of the escape refusal.
func TestRehearsePlanStatementsLintsBeforeTouchingTheTarget(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.db")
	target, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+targetPath)
	c.Assert(err, qt.IsNil)
	c.Assert(target.Close(), qt.IsNil)

	err = RehearsePlanStatements(context.Background(), target,
		[]string{`PRAGMA temp_store_directory = '/tmp/evil'`},
		&goschemaDatabaseFixture, PlanRehearsalOptions{DevURL: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db"))})

	c.Assert(IsPlanEscape(err), qt.IsTrue, qt.Commentf("err=%v", err))
}
