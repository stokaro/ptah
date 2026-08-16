package sqlite_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/sqlite"
)

// rebuildStatements is the create-copy-drop-rename sequence SQLite's own ALTER
// TABLE procedure prescribes, which is what the planner emits between the
// foreign-key pragmas.
func rebuildStatements(copyPredicate string) []string {
	return []string{
		`CREATE TABLE "__rebuild_t" ("id" INTEGER PRIMARY KEY, "a" TEXT NOT NULL)`,
		`INSERT INTO "__rebuild_t" ("id", "a") SELECT "id", "a" FROM "t"` + copyPredicate,
		`DROP TABLE "t"`,
		`ALTER TABLE "__rebuild_t" RENAME TO "t"`,
	}
}

// openRebuildDB opens a file database whose lock waits are bounded. A rebuild
// test that fails mid-transaction leaves that transaction open, and every
// later query on the pool would then block on the file lock until the whole
// test binary timed out -- a hang reads as infrastructure trouble rather than
// as the assertion it is.
func openRebuildDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "rebuild.sqlite")
	db, err := sql.Open("sqlite", path+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(2000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// beginRebuild starts the session-scoped transaction and guarantees it is
// closed even when an assertion below it fails.
func beginRebuild(t *testing.T, writer *sqlite.Writer) types.SchemaTransaction {
	t.Helper()

	tx, err := writer.BeginTransactionWithoutForeignKeys(boundedContext(t))
	if err != nil {
		t.Fatalf("begin rebuild transaction: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

func seedReferencedTable(t *testing.T, db *sql.DB) {
	t.Helper()
	execSQL(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT)`)
	execSQL(t, db, `CREATE TABLE c (id INTEGER PRIMARY KEY, t_id INTEGER REFERENCES t(id))`)
	execSQL(t, db, `INSERT INTO t (id, a) VALUES (1, 'x')`)
	execSQL(t, db, `INSERT INTO c (id, t_id) VALUES (1, 1)`)
}

// rebuildTestTimeout bounds every database call these tests make. A rebuild
// that fails mid-transaction can leave that transaction open, and a leaked
// transaction holds its pooled connection forever: the next query would then
// wait for a connection that is never coming back, which database/sql does
// without a deadline of its own. Bounded, that surfaces as a failed assertion
// instead of a hung package.
const rebuildTestTimeout = 5 * time.Second

func boundedContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), rebuildTestTimeout)
	t.Cleanup(cancel)
	return ctx
}

func executeAll(t *testing.T, tx interface {
	ExecuteSQL(context.Context, string, ...any) error
}, statements []string,
) error {
	t.Helper()
	for _, statement := range statements {
		err := tx.ExecuteSQL(boundedContext(t), statement)
		if err != nil {
			return err
		}
	}
	return nil
}

func scalar(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	var value sql.NullString
	if err := db.QueryRowContext(boundedContext(t), query).Scan(&value); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return value.String
}

func TestRebuildTransactionKeepsInboundReferences(t *testing.T) {
	c := qt.New(t)
	db := openRebuildDB(t)
	seedReferencedTable(t, db)
	writer := sqlite.NewSQLiteWriter(db, "")

	// Without the session, DROP TABLE t fails outright: the child row makes it
	// a foreign-key violation. Measured on SQLite 3.51.
	tx := beginRebuild(t, writer)
	c.Assert(executeAll(t, tx, rebuildStatements("")), qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)

	c.Assert(scalar(t, db, `SELECT count(*) FROM c`), qt.Equals, "1")
	c.Assert(scalar(t, db, `SELECT count(*) FROM t`), qt.Equals, "1")
	c.Assert(scalar(t, db, `SELECT sql FROM sqlite_master WHERE name = 't'`), qt.Contains, "NOT NULL")
	c.Assert(scalar(t, db, `SELECT count(*) FROM pragma_foreign_key_check`), qt.Equals, "0")
}

func TestRebuildTransactionRefusesToOrphanARow(t *testing.T) {
	c := qt.New(t)
	db := openRebuildDB(t)
	seedReferencedTable(t, db)
	writer := sqlite.NewSQLiteWriter(db, "")

	// Enforcement is off for the whole transaction, so nothing stops a rebuild
	// from leaving a child row pointing at a parent that no longer exists. The
	// check before the commit is what turns that into a refusal instead of a
	// quietly inconsistent database.
	tx := beginRebuild(t, writer)
	c.Assert(executeAll(t, tx, rebuildStatements(` WHERE "id" <> 1`)), qt.IsNil)

	err := tx.Commit()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unresolved foreign-key reference")
	c.Assert(err.Error(), qt.Contains, "c references missing t")

	// The refusal rolled the rebuild back rather than leaving it half-applied.
	c.Assert(scalar(t, db, `SELECT count(*) FROM t`), qt.Equals, "1")
	c.Assert(scalar(t, db, `SELECT sql FROM sqlite_master WHERE name = 't'`), qt.Not(qt.Contains), "NOT NULL")
	c.Assert(scalar(t, db, `SELECT count(*) FROM pragma_foreign_key_check`), qt.Equals, "0")
}

func TestRefusedRebuildLeavesAPinnedConnectionUsable(t *testing.T) {
	c := qt.New(t)
	db := openRebuildDB(t)
	seedReferencedTable(t, db)

	// Pinned, because that is where the refusal has to roll back for itself.
	// On a pool the connection goes back when the session releases it and the
	// driver ends the transaction on the way; a pinned connection is never
	// released, so a refusal that only reported the violation would leave the
	// transaction open on it forever.
	conn, err := db.Conn(boundedContext(t))
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(conn.Close(), qt.IsNil) }()

	writer := sqlite.NewSQLiteWriterForConnection(conn, "")
	tx := beginRebuild(t, writer)
	c.Assert(executeAll(t, tx, rebuildStatements(` WHERE "id" <> 1`)), qt.IsNil)

	err = tx.Commit()
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unresolved foreign-key reference")

	// The connection answers, which it cannot do with a transaction still open
	// on it, and it answers with the schema the refusal preserved.
	var definition string
	c.Assert(
		conn.QueryRowContext(boundedContext(t), `SELECT sql FROM sqlite_master WHERE name = 't'`).Scan(&definition),
		qt.IsNil,
	)
	c.Assert(definition, qt.Not(qt.Contains), "NOT NULL")
}

func TestRebuildTransactionRestoresEnforcementOnTheConnection(t *testing.T) {
	tests := []struct {
		name   string
		finish func(tx interface {
			Commit() error
			Rollback() error
		}) error
	}{
		{
			name: "after a commit",
			finish: func(tx interface {
				Commit() error
				Rollback() error
			}) error {
				return tx.Commit()
			},
		},
		{
			name: "after a rollback",
			finish: func(tx interface {
				Commit() error
				Rollback() error
			}) error {
				return tx.Rollback()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			db := openRebuildDB(t)
			seedReferencedTable(t, db)

			// Pinned rather than pooled, so the assertion names the connection
			// the session actually borrowed. On a pool it could read any other.
			conn, err := db.Conn(boundedContext(t))
			c.Assert(err, qt.IsNil)
			defer func() { c.Assert(conn.Close(), qt.IsNil) }()

			writer := sqlite.NewSQLiteWriterForConnection(conn, "")
			tx := beginRebuild(t, writer)
			c.Assert(executeAll(t, tx, rebuildStatements("")), qt.IsNil)
			_ = tt.finish(tx)

			var enforced int
			c.Assert(
				conn.QueryRowContext(boundedContext(t), "PRAGMA foreign_keys").Scan(&enforced),
				qt.IsNil,
			)
			c.Assert(enforced, qt.Equals, 1)
		})
	}
}
