//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A view holding the metadata name is refused on Oracle, where a lookup that
// read only ALL_TABLES would report the name free.
//
// `oracleCreateTableIfAbsent` suppresses ORA-00955 for the collision, so the
// create succeeds silently and a post-create check reading tables alone would
// see nothing either. A view with matching columns and an INSTEAD OF trigger
// is then read and written like the table it stands in for
// (stokaro/ptah#3474).
func TestOracleForeignViewHoldingTheMetadataNameIsRefusedLive(t *testing.T) {
	c := qt.New(t)
	admin, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, dbtarget.OracleAdmin))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	owner := fmt.Sprintf("PTAH_VIEW_%d", time.Now().UnixNano()%1_000_000)
	oracleViewFixture(c, t, admin, owner)

	conn, err := dbschema.ConnectToDatabase(t.Context(), dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_widgets.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_view_widgets (id NUMBER(10) PRIMARY KEY);\n"),
		},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE ptah_view_widgets PURGE;\n")},
	})
	c.Assert(err, qt.IsNil)

	err = mig.WithMigrationsTable(owner, "SCHEMA_MIGRATIONS").Initialize(t.Context())

	c.Assert(err, qt.ErrorIs, migrator.ErrForeignMetadataTable)
}

// oracleViewFixture creates an account holding a view under the metadata name.
// A schema is a user on Oracle, so the account is what makes the object
// somebody else's.
func oracleViewFixture(c *qt.C, t *testing.T, admin *dbschema.DatabaseConnection, owner string) {
	c.Helper()
	for _, statement := range []string{
		fmt.Sprintf(`CREATE USER %s IDENTIFIED BY ptah_password`, owner),
		fmt.Sprintf(`ALTER USER %s QUOTA UNLIMITED ON USERS`, owner),
		fmt.Sprintf(`GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO %s`, owner),
		fmt.Sprintf(`CREATE TABLE %s.backing (version NUMBER(19) PRIMARY KEY)`, owner),
		fmt.Sprintf(`CREATE VIEW %s.schema_migrations AS SELECT version FROM %s.backing`, owner, owner),
		fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON %s.schema_migrations TO ptah`, owner),
	} {
		_, err := admin.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = admin.ExecContext(ctx, fmt.Sprintf(`DROP USER %s CASCADE`, owner))
	})
}
