//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

const (
	sqlServerIndexIdentitySchema      = "ptah_770_sqlserver"
	sqlServerIndexIdentityUsersTable  = "users"
	sqlServerIndexIdentityOrdersTable = "orders"
	sqlServerIndexIdentityName        = "idx_ptah_770_shared"
)

func TestSQLServerTableQualifiedIndexIdentity_RoundTrip(t *testing.T) {
	dsn := requireReachableEngine(t, dbtarget.SQLServer, "sqlserver", "SQL Server")
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
	_, diff := compareSQLServerIndexIdentitySchema(c, t, dsn, ordersTarget)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: sqlServerIndexIdentityName, TableName: sqlServerIndexIdentitySchema + ".users"},
	})

	removeDiff := &difftypes.SchemaDiff{
		IdentifierSemantics: diff.IdentifierSemantics,
	}
	removeDiff.SetIndexRemovals(diff.IndexRemovals())
	planned, err := planner.GenerateSchemaDiffSQLStatements(
		removeDiff,
		ordersTarget,
		platform.SQLServer,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.DeepEquals, []string{
		"DROP INDEX [" + sqlServerIndexIdentityName + "] ON [" +
			sqlServerIndexIdentitySchema + "].[users]",
	})
	_, err = db.Exec(planned[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply SQL Server index removal: %s", planned[0]))

	live, removedDiff := compareSQLServerIndexIdentitySchema(c, t, dsn, ordersTarget)
	c.Assert(removedDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(removedDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 1)

	bothTarget := sqlServerIndexIdentityBothTarget()
	_, addDiff := compareSQLServerIndexIdentitySchema(c, t, dsn, bothTarget)
	c.Assert(addDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: sqlServerIndexIdentityName, TableName: sqlServerIndexIdentitySchema + ".users"},
	})
	c.Assert(addDiff.IndexRemovals(), qt.HasLen, 0)

	createDiff := &difftypes.SchemaDiff{
		IdentifierSemantics: addDiff.IdentifierSemantics,
	}
	createDiff.SetIndexAdditions(addDiff.IndexAdditions())
	planned, err = planner.GenerateSchemaDiffSQLStatements(
		createDiff,
		bothTarget,
		platform.SQLServer,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.HasLen, 1)
	c.Assert(planned[0], qt.Contains,
		"CREATE INDEX ["+sqlServerIndexIdentityName+"] ON ["+
			sqlServerIndexIdentitySchema+"].[users] ([email])")
	_, err = db.Exec(planned[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply SQL Server index addition: %s", planned[0]))

	live, finalDiff := compareSQLServerIndexIdentitySchema(c, t, dsn, bothTarget)
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

func compareSQLServerIndexIdentitySchema(
	c *qt.C,
	t *testing.T,
	dsn string,
	target *goschema.Database,
) (*dbschematypes.DBSchema, *difftypes.SchemaDiff) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{sqlServerIndexIdentitySchema})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(t.Context(), conn, target, live, nil)
	c.Assert(err, qt.IsNil)
	return live, diff
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
