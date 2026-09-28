//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// alterGuardTable is the table every ALTER TABLE guard below acts on, with an
// object of each kind the guards name.
const alterGuardTable = "CREATE TABLE p (id int PRIMARY KEY);\n" +
	"CREATE TABLE c (id int PRIMARY KEY, x int, z int, w int, KEY ix (z), KEY ix_x (x), " +
	"CONSTRAINT fk FOREIGN KEY (x) REFERENCES p (id), CONSTRAINT ck CHECK (z > 0));\n"

// alterGuardClauses are the ALTER TABLE clauses carrying an existence guard.
// MariaDB has no DROP CHECK spelling at all, which stokaro/ptah#3894 owns, so
// that clause is in the MySQL list only.
var alterGuardClauses = []string{
	"DROP INDEX IF EXISTS ix",
	"DROP KEY IF EXISTS ix",
	"DROP FOREIGN KEY IF EXISTS fk",
	"DROP CONSTRAINT IF EXISTS ck",
	"DROP COLUMN IF EXISTS w",
	"DROP IF EXISTS w",
	"ADD COLUMN IF NOT EXISTS y int",
}

// alterGuardRun is one schema file ending in an ALTER TABLE guard, run on the
// server as written and planned by `schema apply --dry-run`.
type alterGuardRun struct {
	serverErr error
	ptahErr   error
	out       string
}

// runAlterGuard writes the schema file that ends in clause, runs it on a
// scratch database of engine, and plans it against another.
func runAlterGuard(c *qt.C, engine dbtarget.Engine, clause string) alterGuardRun {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	schema := alterGuardTable + "ALTER TABLE c " + clause + ";\n" // #nosec G202 -- a fixture built from the static clause table above, run to ask the server whether it takes the clause
	built, _ := scratch.database(c, "guard_built")
	_, target := scratch.database(c, "guard_target")
	_, dev := scratch.database(c, "guard_dev")
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(schema), 0o600), qt.IsNil)
	conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, built)+"?multiStatements=true")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	_, serverErr := conn.ExecContext(c.Context(), schema)
	out, ptahErr := runCompatVerb("schema", "apply", "--url", target, "--to", "file://"+file,
		"--dev-url", dev, "--dry-run")
	return alterGuardRun{serverErr: serverErr, ptahErr: ptahErr, out: out}
}

// TestSchemaApplyRefusesAMySQLAlterGuardE2E runs a schema file ending in an
// ALTER TABLE guard on MySQL, which answers ERROR 1064 to every one, and plans
// it: Ptah refuses the file too, naming the guard. Atlas CE v1.3.0 refuses
// such a file with the server's error. Without the refusal the guard is read,
// and the plan leaves out the object it names (stokaro/ptah#3877).
func TestSchemaApplyRefusesAMySQLAlterGuardE2E(t *testing.T) {
	for _, clause := range append(slices.Clone(alterGuardClauses), "DROP CHECK IF EXISTS ck") {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)

			run := runAlterGuard(c, dbtarget.MySQLAdmin, clause)

			c.Assert(run.serverErr, qt.ErrorMatches, `Error 1064 \(42000\): You have an error in your SQL syntax.*`)
			c.Assert(run.ptahErr, qt.ErrorMatches, `(?s).*IF (NOT )?EXISTS at position \d+ in ALTER TABLE .*mysql takes no .*`)
		})
	}
}

// TestSchemaApplyTakesAMariaDBAlterGuardE2E is the control: MariaDB runs each
// guard, and Ptah plans the file.
func TestSchemaApplyTakesAMariaDBAlterGuardE2E(t *testing.T) {
	for _, clause := range alterGuardClauses {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)

			run := runAlterGuard(c, dbtarget.MariaDBAdmin, clause)

			c.Assert(run.serverErr, qt.IsNil)
			c.Assert(run.ptahErr, qt.IsNil, qt.Commentf("%s", run.out))
		})
	}
}
