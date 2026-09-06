//go:build integration

package gonative_test

import (
	"database/sql"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for database/sql

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

const (
	indexIdentityUsersTable  = "ptah_index_identity_users"
	indexIdentityOrdersTable = "ptah_index_identity_orders"
	indexIdentityName        = "idx_ptah_770_shared_tenant"
	indexIdentityUpperName   = "IDX_PTAH_770_SHARED_TENANT"
)

func TestMySQLTableQualifiedIndexIdentity_RoundTrip(t *testing.T) {
	testMySQLFamilyTableQualifiedIndexIdentityRoundTrip(t, skipIfNoMySQL(t), platform.MySQL)
}

func TestMariaDBTableQualifiedIndexIdentity_RoundTrip(t *testing.T) {
	testMySQLFamilyTableQualifiedIndexIdentityRoundTrip(t, skipIfNoMariaDB(t), platform.MariaDB)
}

func testMySQLFamilyTableQualifiedIndexIdentityRoundTrip(t *testing.T, dsn, dialect string) {
	t.Helper()
	c := qt.New(t)
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS " + indexIdentityOrdersTable)
	_, _ = db.Exec("DROP TABLE IF EXISTS " + indexIdentityUsersTable)
	defer func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS " + indexIdentityOrdersTable)
		_, _ = db.Exec("DROP TABLE IF EXISTS " + indexIdentityUsersTable)
	}()

	_, err = db.Exec(
		"CREATE TABLE " + indexIdentityUsersTable +
			" (id INT NOT NULL PRIMARY KEY, tenant_id INT NOT NULL, INDEX " +
			indexIdentityUpperName + " (tenant_id))",
	)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(
		"CREATE TABLE " + indexIdentityOrdersTable +
			" (id INT NOT NULL PRIMARY KEY, tenant_id INT NOT NULL, INDEX " +
			indexIdentityName + " (tenant_id))",
	)
	c.Assert(err, qt.IsNil)

	target := tableQualifiedIndexTarget()
	live := readMySQLFamilyIndexIdentitySchema(c, db)
	initialDiff := schemadiff.CompareWithDialect(target, live, dialect)
	c.Assert(initialDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(initialDiff.IndexRemovals(), qt.HasLen, 0)

	_, err = db.Exec("DROP INDEX " + indexIdentityName + " ON " + indexIdentityOrdersTable)
	c.Assert(err, qt.IsNil)
	live = readMySQLFamilyIndexIdentitySchema(c, db)
	additionDiff := schemadiff.CompareWithDialect(target, live, dialect)
	c.Assert(additionDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: indexIdentityName, TableName: indexIdentityOrdersTable},
	})
	c.Assert(additionDiff.IndexRemovals(), qt.HasLen, 0)

	indexOnlyAddition := &difftypes.SchemaDiff{}
	indexOnlyAddition.SetIndexAdditions(additionDiff.IndexesAdded)
	addStatements, err := planner.GenerateSchemaDiffSQLStatements(indexOnlyAddition, dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(addStatements, qt.HasLen, 1)
	_, err = db.Exec(addStatements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply exact index addition: %s", addStatements[0]))

	target = tableQualifiedIndexTargetWithoutOrdersIndex()
	live = readMySQLFamilyIndexIdentitySchema(c, db)
	removalDiff := schemadiff.CompareWithDialect(target, live, dialect)
	c.Assert(removalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(removalDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: indexIdentityName, TableName: indexIdentityOrdersTable},
	})

	indexOnlyRemoval := &difftypes.SchemaDiff{}
	indexOnlyRemoval.SetIndexRemovals(removalDiff.IndexRemovals())
	removeStatements, err := planner.GenerateSchemaDiffSQLStatements(indexOnlyRemoval, dialect)
	c.Assert(err, qt.IsNil)
	c.Assert(removeStatements, qt.HasLen, 1)
	_, err = db.Exec(removeStatements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply exact index removal: %s", removeStatements[0]))

	live = readMySQLFamilyIndexIdentitySchema(c, db)
	finalDiff := schemadiff.CompareWithDialect(target, live, dialect)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
}

func readMySQLFamilyIndexIdentitySchema(c *qt.C, db *sql.DB) *catalog.Database {
	c.Helper()
	live, err := mysql.NewMySQLReader(db, "").ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	live.Indexes = slices.DeleteFunc(live.Indexes, func(index catalog.Index) bool {
		return index.TableName != indexIdentityUsersTable && index.TableName != indexIdentityOrdersTable
	})
	return live
}

func tableQualifiedIndexTarget() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "IndexIdentityUser", Name: indexIdentityUsersTable},
			{StructName: "IndexIdentityOrder", Name: indexIdentityOrdersTable},
		},
		Indexes: []schemamodel.Index{
			{
				StructName: "IndexIdentityUser",
				Name:       indexIdentityName,
				Fields:     []string{"tenant_id"},
			},
			{
				StructName: "IndexIdentityOrder",
				Name:       indexIdentityName,
				Fields:     []string{"tenant_id"},
			},
		},
	}
}

func tableQualifiedIndexTargetWithoutOrdersIndex() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "IndexIdentityUser", Name: indexIdentityUsersTable},
			{StructName: "IndexIdentityOrder", Name: indexIdentityOrdersTable},
		},
		Indexes: []schemamodel.Index{
			{
				StructName: "IndexIdentityUser",
				Name:       indexIdentityName,
				Fields:     []string{"tenant_id"},
			},
		},
	}
}
