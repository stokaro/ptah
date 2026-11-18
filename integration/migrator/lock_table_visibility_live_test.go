//go:build integration

package migrator_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dblock"
	"ptah.run/migration/generator"
)

// lockTable is the table Oracle's migration lock keeps its rows in.
const lockTable = "ptah_locks"

// readTableNames reads the schema of conn and returns the names of its tables
// in lower case, as the engines that fold unquoted names report them.
func readTableNames(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	schema, err := conn.Reader().ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, strings.ToLower(table.Name))
	}
	return names
}

// planAgainstNothing generates the migration that brings conn to an empty
// desired schema, and returns its up file, or "" when there is nothing to do.
func planAgainstNothing(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	files, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		Runtime:       must.Must(builtin.New()),
		Generated:     &schemamodel.Database{},
		DBConn:        conn,
		MigrationName: "against_nothing",
		OutputDir:     c.TempDir(),
	})
	c.Assert(err, qt.IsNil)
	if files == nil {
		return ""
	}
	c.Assert(files.Files, qt.HasLen, 1)
	up, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	return string(up)
}

// On an engine whose migration lock keeps no table, Ptah never creates
// ptah_locks, so a table of that name is the user's: the reader reports it,
// and a desired schema without it plans its DROP. Left out as bookkeeping, it
// would be invisible to diff and apply, and the plan would keep a table the
// user removed from the schema.
func TestLockTableNameIsTheUsersWhereTheLockKeepsNoTableLive(t *testing.T) {
	for _, engine := range []repeatableEngine{repeatablePostgres, repeatableMySQL, repeatableMariaDB, repeatableSQLServer} {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, engine)
			_, err := r.conn.ExecContext(context.Background(), "CREATE TABLE "+lockTable+" (name VARCHAR(100) PRIMARY KEY)")
			c.Assert(err, qt.IsNil)

			names := readTableNames(c, r.conn)
			up := planAgainstNothing(c, r.conn)

			c.Assert(names, qt.DeepEquals, []string{lockTable})
			c.Assert(up, qt.Matches, `(?is).*DROP TABLE[^;]*ptah_locks.*`)
		})
	}
}

// On Oracle the migration lock creates ptah_locks the first time it is taken
// in a schema, so the table is Ptah's: the reader leaves it out, and a desired
// schema without it plans nothing.
func TestLockTableIsBookkeepingOnOracleLive(t *testing.T) {
	c := qt.New(t)
	r := newRepeatableEngineTarget(c, t, repeatableOracle)
	lock, err := dblock.Acquire(context.Background(), r.conn, "ptah_migrate", time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Release(context.Background()), qt.IsNil)
	var created int
	c.Assert(r.conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:1)", lockTable).Scan(&created), qt.IsNil)

	names := readTableNames(c, r.conn)
	up := planAgainstNothing(c, r.conn)

	c.Assert(created, qt.Equals, 1)
	c.Assert(names, qt.HasLen, 0)
	c.Assert(up, qt.Equals, "")
}

// Only the lock's own spelling of ptah_locks is bookkeeping on Oracle. The
// lock creates the table unquoted, so the catalog holds PTAH_LOCKS; a quoted
// "ptah_locks" beside it is another table, the user's, which the reader
// reports. Left out by a comparison without regard to case, it would be
// invisible to diff and apply.
func TestQuotedLockTableNamesakesAreTheUsersOnOracleLive(t *testing.T) {
	c := qt.New(t)
	r := newRepeatableEngineTarget(c, t, repeatableOracle)
	lock, err := dblock.Acquire(context.Background(), r.conn, "ptah_migrate", time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Release(context.Background()), qt.IsNil)
	_, err = r.conn.ExecContext(context.Background(), `CREATE TABLE "ptah_locks" (name VARCHAR2(100) PRIMARY KEY)`)
	c.Assert(err, qt.IsNil)

	schema, err := r.conn.Reader().ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}

	c.Assert(names, qt.DeepEquals, []string{"ptah_locks"})
}
