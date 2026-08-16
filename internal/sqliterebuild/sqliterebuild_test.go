package sqliterebuild_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/sqliteforeignkeys"
	"go.5x5.cz/ptah/internal/sqliterebuild"
)

// rebuildBody is the create-copy-drop-rename sequence SQLite's own ALTER TABLE
// procedure prescribes. Executed with foreign-key enforcement on, its DROP is a
// violation as soon as another table refers to "t".
var rebuildBody = []string{
	`CREATE TABLE "__rebuild_t" ("id" INTEGER PRIMARY KEY, "a" TEXT NOT NULL)`,
	`INSERT INTO "__rebuild_t" ("id", "a") SELECT "id", "a" FROM "t"`,
	`DROP TABLE "t"`,
	`ALTER TABLE "__rebuild_t" RENAME TO "t"`,
}

func bracketed(body []string) []string {
	statements := []string{sqliteforeignkeys.DisableStatement}
	statements = append(statements, body...)
	return append(statements, sqliteforeignkeys.EnableStatement)
}

// connectSeeded opens a SQLite database holding a row that refers to the table
// the rebuild replaces, with lock waits bounded so a failure surfaces as a
// failed assertion rather than as a hung package.
func connectSeeded(t *testing.T) (*dbschema.DatabaseConnection, context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	path := filepath.Join(t.TempDir(), "rebuild.sqlite")
	url := "sqlite://" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(2000)"
	conn, err := dbschema.ConnectToDatabase(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	seed := []string{
		`CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)`,
		`CREATE TABLE c (id INTEGER PRIMARY KEY, t_id INTEGER REFERENCES t(id))`,
		`INSERT INTO t (id, a) VALUES (1, 'x')`,
		`INSERT INTO c (id, t_id) VALUES (1, 1)`,
	}
	for _, statement := range seed {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed %q: %v", statement, err)
		}
	}
	return conn, ctx
}

func applyThrough(
	t *testing.T,
	conn *dbschema.DatabaseConnection,
	ctx context.Context,
	statements []string,
) error {
	t.Helper()

	tx, err := sqliterebuild.BeginTransaction(ctx, conn, statements)
	if err != nil {
		return err
	}
	t.Cleanup(func() { _ = tx.Rollback() })

	for _, statement := range statements {
		if execErr := tx.ExecuteSQL(ctx, statement); execErr != nil {
			return execErr
		}
	}
	return tx.Commit()
}

func TestABracketedPlanRebuildsATableOtherTablesReferTo(t *testing.T) {
	c := qt.New(t)
	conn, ctx := connectSeeded(t)

	c.Assert(applyThrough(t, conn, ctx, bracketed(rebuildBody)), qt.IsNil)

	var children int
	c.Assert(conn.QueryRowContext(ctx, `SELECT count(*) FROM c`).Scan(&children), qt.IsNil)
	c.Assert(children, qt.Equals, 1)

	var definition string
	c.Assert(
		conn.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name = 't'`).Scan(&definition),
		qt.IsNil,
	)
	c.Assert(definition, qt.Contains, "NOT NULL")
}

func TestAnUnbracketedPlanKeepsEnforcement(t *testing.T) {
	c := qt.New(t)
	conn, ctx := connectSeeded(t)

	// The bracket is what asks for the session, and nothing else does. Without
	// it the same statements run under an ordinary transaction, where the DROP
	// is refused -- which is both the pre-fix behavior and the proof that the
	// suspension is scoped to plans that ask for it rather than applied to
	// every apply.
	err := applyThrough(t, conn, ctx, rebuildBody)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "FOREIGN KEY constraint failed")
}
