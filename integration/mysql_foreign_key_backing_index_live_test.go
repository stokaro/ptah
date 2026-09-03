//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver for database/sql

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestMySQLForeignKeyBackingIndexOwnershipLive is stokaro/ptah#2822, and it is
// the check this decision never had.
//
// Which index a foreign key owns has been decided twice from measurements taken
// by hand and written into unit-test expectations (#2782, #2818). A hand
// measurement can answer a different question than the one the code asks, and
// that is what happened: direction was measured at CREATE time -- which index
// the engine BUILDS -- and used to decide at DROP time which index the author
// may REMOVE. The two moments answer differently on MySQL.
//
// So this test does not assert a remembered answer. It plans against a live
// catalog and then hands the plan back to the server: a drop the comparison
// proposes has to be one the engine accepts, and an index the comparison
// protects has to be one the engine refuses to drop. Neither half can be
// satisfied by writing down the wrong rule.
func TestMySQLForeignKeyBackingIndexOwnershipLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()
			c.Assert(adminDB.PingContext(ctx), qt.IsNil)

			name := fmt.Sprintf("ptah_fk_own_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)
			dbURL := replaceMySQLDatabaseName(c, dbtarget.URL(t, test.engine), name)

			// Written by hand, and the cover is DESCENDING: that is the shape
			// the previous rule read as "the engine must have built f".
			for _, statement := range []string{
				"CREATE TABLE `parents` (`id` INT NOT NULL PRIMARY KEY)",
				"CREATE TABLE `children` (`a` INT, KEY `cover` (`a` DESC), KEY `f` (`a`), " +
					"CONSTRAINT `f` FOREIGN KEY (`a`) REFERENCES `parents` (`id`))",
			} {
				execInSchema(c, ctx, adminDB, name, statement)
			}

			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			current, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			// The author keeps the cover and the constraint, and drops `f`.
			difference := schemadiff.CompareWithDialect(
				childrenWithDescendingCover(), current, test.name)
			c.Assert(removedIndexNamesLive(difference), qt.DeepEquals, []string{"f"},
				qt.Commentf("the author removed f and the engine will accept it"))

			// The half no expectation can fake: the server takes the plan.
			execInSchema(c, ctx, adminDB, name, "DROP INDEX `f` ON `children`")

			// And the constraint survives on the descending cover, which is why
			// the drop was the author's to ask for.
			c.Assert(foreignKeyNames(c, ctx, adminDB, name), qt.DeepEquals, []string{"f"})
		})
	}
}

// TestMySQLTheEnginesOwnBackingIndexIsStillProtectedLive is the control.
//
// A rule that stopped claiming anything would satisfy the test above. Here the
// engine's index is the only one covering the key, the comparison has to plan
// nothing, and the engine's own refusal is what says the protection was needed:
// ERROR 1553, measured on both.
func TestMySQLTheEnginesOwnBackingIndexIsStillProtectedLive(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("mysql", dbtarget.DriverDSN(c, test.engine))
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()
			c.Assert(adminDB.PingContext(ctx), qt.IsNil)

			name := fmt.Sprintf("ptah_fk_own_solo_%d", time.Now().UnixNano())
			createMySQLDatabase(c, ctx, adminDB, name)
			defer dropMySQLDatabase(c, context.Background(), adminDB, name)
			dbURL := replaceMySQLDatabaseName(c, dbtarget.URL(t, test.engine), name)

			// No index of the author's at all: the engine builds its own and
			// names it for the constraint.
			for _, statement := range []string{
				"CREATE TABLE `parents` (`id` INT NOT NULL PRIMARY KEY)",
				"CREATE TABLE `children` (`a` INT, " +
					"CONSTRAINT `f` FOREIGN KEY (`a`) REFERENCES `parents` (`id`))",
			} {
				execInSchema(c, ctx, adminDB, name, statement)
			}

			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			current, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			difference := schemadiff.CompareWithDialect(
				childrenSchema(), current, test.name)
			c.Assert(removedIndexNamesLive(difference), qt.HasLen, 0,
				qt.Commentf("the engine's own backing index is not the author's to drop"))

			// What makes that the right answer rather than a preference.
			_, err = adminDB.ExecContext(ctx, fmt.Sprintf("USE `%s`", name))
			c.Assert(err, qt.IsNil)
			_, err = adminDB.ExecContext(ctx, "DROP INDEX `f` ON `children`")
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "1553")
		})
	}
}

// childrenWithDescendingCover is the desired schema for the first test: the
// constraint and the author's descending cover, and no index named for the
// constraint.
func childrenWithDescendingCover() *schemamodel.Database {
	database := childrenSchema()
	database.Indexes = []schemamodel.Index{{
		StructName: "Children", Name: "cover", Fields: []string{"a"},
		Parts: []schemamodel.IndexPart{{Name: "a", Desc: true}},
	}}
	return database
}

// childrenSchema is the two tables and the foreign key, with no index declared.
func childrenSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "parents", StructName: "Parents"},
			{Name: "children", StructName: "Children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parents", FieldName: "ID", Name: "id", Type: "INT", Primary: true},
			{StructName: "Children", FieldName: "A", Name: "a", Type: "INT", Nullable: true,
				Foreign: "parents(id)", ForeignKeyName: "f"},
		},
	}
}

// removedIndexNamesLive is the names of the indexes a comparison would drop.
func removedIndexNamesLive(difference *difftypes.SchemaDiff) []string {
	names := make([]string, 0, len(difference.IndexesRemoved))
	for _, removed := range difference.IndexesRemoved {
		names = append(names, removed.Name)
	}
	return names
}

// execInSchema runs one statement inside the named schema.
func execInSchema(c *qt.C, ctx context.Context, db *sql.DB, schema, statement string) {
	c.Helper()
	_, err := db.ExecContext(ctx, fmt.Sprintf("USE `%s`", schema))
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// foreignKeyNames is what the catalog says the constraints are.
func foreignKeyNames(c *qt.C, ctx context.Context, db *sql.DB, schema string) []string {
	c.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS
		 WHERE CONSTRAINT_SCHEMA = ? ORDER BY CONSTRAINT_NAME`, schema)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []string
	for rows.Next() {
		var constraintName string
		c.Assert(rows.Scan(&constraintName), qt.IsNil)
		found = append(found, constraintName)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}
