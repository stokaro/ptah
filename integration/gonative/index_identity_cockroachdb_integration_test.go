//go:build integration

package gonative_test

import (
	"database/sql"
	"slices"
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
	cockroachIndexIdentityUsersTable  = "ptah_770_crdb_users"
	cockroachIndexIdentityOrdersTable = "ptah_770_crdb_orders"
	cockroachIndexIdentityName        = "idx_ptah_770_crdb_shared"
)

func TestCockroachDBTableQualifiedIndexIdentity_RoundTrip(t *testing.T) {
	dsn := skipIfNoCockroachDB(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	cleanupCockroachIndexIdentity(db)
	defer cleanupCockroachIndexIdentity(db)

	_, err = db.Exec(
		`CREATE TABLE ` + cockroachIndexIdentityUsersTable +
			` (email STRING NOT NULL, handle STRING NOT NULL)`,
	)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(
		`CREATE TABLE ` + cockroachIndexIdentityOrdersTable +
			` (reference STRING NOT NULL)`,
	)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(
		`CREATE INDEX ` + cockroachIndexIdentityName +
			` ON ` + cockroachIndexIdentityUsersTable + ` (email)`,
	)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(
		`CREATE INDEX ` + cockroachIndexIdentityName +
			` ON ` + cockroachIndexIdentityOrdersTable + ` (reference)`,
	)
	c.Assert(err, qt.IsNil)

	ordersTarget := cockroachIndexIdentityOrdersTarget()
	live := readCockroachIndexIdentitySchema(c, t, dsn)
	diff := schemadiff.CompareWithDialect(ordersTarget, live, platform.CockroachDB)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: cockroachIndexIdentityName, TableName: cockroachIndexIdentityUsersTable},
	})

	removeDiff := &difftypes.SchemaDiff{}
	removeDiff.SetIndexRemovals(diff.IndexRemovals())
	statements, err := planner.GenerateSchemaDiffSQLStatements(removeDiff, ordersTarget, platform.CockroachDB)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`DROP INDEX IF EXISTS "` + cockroachIndexIdentityUsersTable + `"@"` +
			cockroachIndexIdentityName + `"`,
	})
	_, err = db.Exec(statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("execute CockroachDB index removal: %s", statements[0]))

	live = readCockroachIndexIdentitySchema(c, t, dsn)
	removedDiff := schemadiff.CompareWithDialect(ordersTarget, live, platform.CockroachDB)
	c.Assert(removedDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(removedDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 1)

	bothTarget := cockroachIndexIdentityBothTarget()
	addDiff := schemadiff.CompareWithDialect(bothTarget, live, platform.CockroachDB)
	c.Assert(addDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: cockroachIndexIdentityName, TableName: cockroachIndexIdentityUsersTable},
	})
	c.Assert(addDiff.IndexRemovals(), qt.HasLen, 0)

	createDiff := &difftypes.SchemaDiff{}
	createDiff.SetIndexAdditions(addDiff.IndexAdditions())
	statements, err = planner.GenerateSchemaDiffSQLStatements(createDiff, bothTarget, platform.CockroachDB)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains,
		`CREATE INDEX IF NOT EXISTS "`+cockroachIndexIdentityName+`" ON "`+
			cockroachIndexIdentityUsersTable+`" ("email")`)
	_, err = db.Exec(statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("execute CockroachDB index addition: %s", statements[0]))

	live = readCockroachIndexIdentitySchema(c, t, dsn)
	finalDiff := schemadiff.CompareWithDialect(bothTarget, live, platform.CockroachDB)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 2)
}

func skipIfNoCockroachDB(t *testing.T) string {
	t.Helper()
	return requireReachableEngine(t, dbtarget.CockroachDB, "pgx", "CockroachDB")
}

func cleanupCockroachIndexIdentity(db *sql.DB) {
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + cockroachIndexIdentityOrdersTable + ` CASCADE`)
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + cockroachIndexIdentityUsersTable + ` CASCADE`)
}

func readCockroachIndexIdentitySchema(c *qt.C, t *testing.T, dsn string) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	live.Tables = slices.DeleteFunc(live.Tables, func(table dbschematypes.DBTable) bool {
		return table.Name != cockroachIndexIdentityUsersTable &&
			table.Name != cockroachIndexIdentityOrdersTable
	})
	live.Indexes = slices.DeleteFunc(live.Indexes, func(index dbschematypes.DBIndex) bool {
		return index.IsPrimary ||
			index.TableName != cockroachIndexIdentityUsersTable &&
				index.TableName != cockroachIndexIdentityOrdersTable
	})
	live.Constraints = slices.DeleteFunc(live.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return constraint.TableName != cockroachIndexIdentityUsersTable &&
			constraint.TableName != cockroachIndexIdentityOrdersTable
	})
	return live
}

func cockroachIndexIdentityOrdersTarget() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{Name: cockroachIndexIdentityUsersTable, StructName: "CockroachIndexUser"},
			{Name: cockroachIndexIdentityOrdersTable, StructName: "CockroachIndexOrder"},
		},
		Indexes: []goschema.Index{
			{
				Name:      cockroachIndexIdentityName,
				TableName: cockroachIndexIdentityOrdersTable,
				Fields:    []string{"reference"},
			},
		},
	}
}

func cockroachIndexIdentityBothTarget() *goschema.Database {
	target := cockroachIndexIdentityOrdersTarget()
	target.Indexes = append(target.Indexes, goschema.Index{
		Name:      cockroachIndexIdentityName,
		TableName: cockroachIndexIdentityUsersTable,
		Fields:    []string{"email"},
	})
	return target
}
