package migrator_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// An interrupt cancels the context while a migration transaction is open.
// database/sql rolls that transaction back itself, so the migration changed
// nothing, and the Atlas revision row written for it is discarded exactly as
// the row of an ordinary rolled-back failure is (stokaro/ptah#3315).

// cancelingInterceptor cancels the run before the statement it names, which is
// where an interrupt lands. It handles nothing, so the statement then executes
// through Ptah under the canceled context.
type cancelingInterceptor struct {
	cancel context.CancelFunc
	before string
}

func (i *cancelingInterceptor) ValidateDirectives(map[string]string) error { return nil }

func (i *cancelingInterceptor) ExecuteStatement(
	_ context.Context,
	_ *dbschema.DatabaseConnection,
	stmt string,
	_ map[string]string,
) (bool, error) {
	if strings.Contains(stmt, i.before) {
		i.cancel()
	}
	return false, nil
}

func TestMigrateUp_AtlasFormatCanceledMigrationLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, m := newInterruptedRollbackMigrator(c, &cancelingInterceptor{cancel: cancel, before: "gadgets"})

	err := m.MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{DiscardRolledBackFailure: true})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(atlasRevisionVersionList(c, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(t, conn, "gadgets"), qt.IsFalse)
}

// TestMigrateUp_AtlasFormatFailedMigrationLeavesNoRevision is the control the
// case above is measured against: the same discard, for a migration that failed
// on its own SQL rather than on an interrupt.
func TestMigrateUp_AtlasFormatFailedMigrationLeavesNoRevision(t *testing.T) {
	c := qt.New(t)
	conn, m := newInterruptedRollbackMigrator(c, nil)

	err := m.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{DiscardRolledBackFailure: true})

	c.Assert(err, qt.IsNotNil)
	c.Assert(atlasRevisionVersionList(c, conn), qt.DeepEquals, []string{"1"})
	c.Assert(sqliteTableExists(t, conn, "gadgets"), qt.IsFalse)
}

// newInterruptedRollbackMigrator builds a two-migration directory on SQLite in
// the Atlas format. Migration 2 creates gadgets and then fails, so a run that
// reaches it always ends in a rolled-back transaction; an interceptor turns
// that failure into a cancellation instead.
func newInterruptedRollbackMigrator(c *qt.C, interceptor *cancelingInterceptor) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+filepath.Join(c.TempDir(), "interrupt.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	fsys := fstest.MapFS{
		"1_widgets.sql": &fstest.MapFile{Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")},
		"2_gadgets.sql": &fstest.MapFile{Data: []byte(
			"CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\nTHIS IS A FAILING STATEMENT;\n")},
	}
	opts := []migrator.FSProviderOption{migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas)}
	opts = append(opts, interceptorOptions(interceptor)...)
	m, err := migrator.NewFSMigrator(conn, fsys, opts...)
	c.Assert(err, qt.IsNil)
	return conn, m.WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)
}

func interceptorOptions(interceptor *cancelingInterceptor) []migrator.FSProviderOption {
	if interceptor == nil {
		return nil
	}
	return []migrator.FSProviderOption{migrator.WithStatementInterceptor(interceptor)}
}

func atlasRevisionVersionList(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()

	rows, err := conn.Query("SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()

	versions := []string{}
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}
