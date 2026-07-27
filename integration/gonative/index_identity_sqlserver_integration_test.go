//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

const (
	sqlServerIndexIdentitySchema      = "ptah_770_sqlserver"
	sqlServerIndexIdentityUsersTable  = "users"
	sqlServerIndexIdentityOrdersTable = "orders"
	sqlServerIndexIdentityName        = "idx_ptah_770_shared"
)

func TestSQLServerTableQualifiedIndexIdentity_RoundTrip(t *testing.T) {
	dsn := requireReachableTestDSN(t, "SQLSERVER_TEST_DSN", "sqlserver", "SQL Server")
	c := qt.New(t)
	db, err := sql.Open("sqlserver", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	cleanupSQLServerIndexIdentity(db)
	defer cleanupSQLServerIndexIdentity(db)

	statements := []string{
		`CREATE SCHEMA [` + sqlServerIndexIdentitySchema + `]`,
		`CREATE TABLE [` + sqlServerIndexIdentitySchema + `].[users] (` +
			`[email] NVARCHAR(320) NOT NULL, [handle] NVARCHAR(320) NOT NULL)`,
		`CREATE TABLE [` + sqlServerIndexIdentitySchema + `].[orders] (` +
			`[reference] NVARCHAR(320) NOT NULL)`,
		`CREATE INDEX [` + sqlServerIndexIdentityName + `] ON [` +
			sqlServerIndexIdentitySchema + `].[users] ([email])`,
		`CREATE INDEX [` + sqlServerIndexIdentityName + `] ON [` +
			sqlServerIndexIdentitySchema + `].[orders] ([reference])`,
	}
	for _, statement := range statements {
		_, err = db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("initialize SQL Server index identity: %s", statement))
	}

	ordersTarget := sqlServerIndexIdentityOrdersTarget()
	live := readSQLServerIndexIdentitySchema(c, t, dsn)
	diff := schemadiff.CompareWithDialect(ordersTarget, live, platform.SQLServer)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: sqlServerIndexIdentityName, TableName: sqlServerIndexIdentitySchema + ".users"},
	})

	removeDiff := &difftypes.SchemaDiff{}
	removeDiff.SetIndexRemovals(diff.IndexRemovals())
	planned, err := planner.GenerateSchemaDiffSQLStatements(removeDiff, ordersTarget, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.DeepEquals, []string{
		"DROP INDEX [" + sqlServerIndexIdentityName + "] ON [" +
			sqlServerIndexIdentitySchema + "].[users]",
	})
	_, err = db.Exec(planned[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply SQL Server index removal: %s", planned[0]))

	live = readSQLServerIndexIdentitySchema(c, t, dsn)
	removedDiff := schemadiff.CompareWithDialect(ordersTarget, live, platform.SQLServer)
	c.Assert(removedDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(removedDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 1)

	bothTarget := sqlServerIndexIdentityBothTarget()
	addDiff := schemadiff.CompareWithDialect(bothTarget, live, platform.SQLServer)
	c.Assert(addDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: sqlServerIndexIdentityName, TableName: sqlServerIndexIdentitySchema + ".users"},
	})
	c.Assert(addDiff.IndexRemovals(), qt.HasLen, 0)

	createDiff := &difftypes.SchemaDiff{}
	createDiff.SetIndexAdditions(addDiff.IndexAdditions())
	planned, err = planner.GenerateSchemaDiffSQLStatements(createDiff, bothTarget, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.HasLen, 1)
	c.Assert(planned[0], qt.Contains,
		"CREATE INDEX ["+sqlServerIndexIdentityName+"] ON ["+
			sqlServerIndexIdentitySchema+"].[users] ([email])")
	_, err = db.Exec(planned[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply SQL Server index addition: %s", planned[0]))

	live = readSQLServerIndexIdentitySchema(c, t, dsn)
	finalDiff := schemadiff.CompareWithDialect(bothTarget, live, platform.SQLServer)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 2)
}

func cleanupSQLServerIndexIdentity(db *sql.DB) {
	_, _ = db.Exec(`DROP TABLE IF EXISTS [` + sqlServerIndexIdentitySchema + `].[orders]`)
	_, _ = db.Exec(`DROP TABLE IF EXISTS [` + sqlServerIndexIdentitySchema + `].[users]`)
	_, _ = db.Exec(
		`IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '` +
			sqlServerIndexIdentitySchema + `') EXEC('DROP SCHEMA [` +
			sqlServerIndexIdentitySchema + `]')`,
	)
}

func readSQLServerIndexIdentitySchema(c *qt.C, t *testing.T, dsn string) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{sqlServerIndexIdentitySchema})
	c.Assert(err, qt.IsNil)
	return live
}

func sqlServerIndexIdentityOrdersTarget() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{
				StructName: "SQLServerIndexUser",
				Schema:     sqlServerIndexIdentitySchema,
				Name:       sqlServerIndexIdentityUsersTable,
			},
			{
				StructName: "SQLServerIndexOrder",
				Schema:     sqlServerIndexIdentitySchema,
				Name:       sqlServerIndexIdentityOrdersTable,
			},
		},
		Indexes: []goschema.Index{
			{
				StructName: "SQLServerIndexOrder",
				Name:       sqlServerIndexIdentityName,
				Fields:     []string{"reference"},
			},
		},
	}
}

func sqlServerIndexIdentityBothTarget() *goschema.Database {
	target := sqlServerIndexIdentityOrdersTarget()
	target.Indexes = append(target.Indexes, goschema.Index{
		StructName: "SQLServerIndexUser",
		Name:       sqlServerIndexIdentityName,
		Fields:     []string{"email"},
	})
	return target
}
