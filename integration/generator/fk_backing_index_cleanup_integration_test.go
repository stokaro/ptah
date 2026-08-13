//go:build integration

package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
)

func TestMySQLFamilyForeignKeyBackingIndexDownRoundTripIntegration(t *testing.T) {
	engines := []struct {
		name   string
		envKey string
	}{
		{name: "mysql", envKey: "MYSQL_URL"},
		{name: "mariadb", envKey: "MARIADB_URL"},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn := requireGeneratorDatabaseConnection(t, engine.envKey)
			tests := []struct {
				name      string
				indexName string
			}{
				{name: "generated backing index is removed"},
				{name: "pre-existing same-named index is preserved", indexName: "fk_parent"},
				{name: "pre-existing differently named covering index is preserved", indexName: "idx_parent"},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					runForeignKeyBackingIndexRoundTrip(c, conn, test.indexName)
				})
			}
		})
	}
}

func TestMySQLFamilySameNamedNonCoveringIndexRefusesForeignKeyIntegration(t *testing.T) {
	engines := []struct {
		name   string
		envKey string
	}{
		{name: "mysql", envKey: "MYSQL_URL"},
		{name: "mariadb", envKey: "MARIADB_URL"},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn := requireGeneratorDatabaseConnection(t, engine.envKey)
			assertSameNamedNonCoveringIndexRefusesForeignKey(c, conn)
		})
	}
}

func assertSameNamedNonCoveringIndexRefusesForeignKey(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
) {
	c.Helper()
	const (
		parentTable = "ptah_fk_collision_parents"
		childTable  = "ptah_fk_collision_children"
		foreignKey  = "fk_parent"
	)

	cleanup := func() {
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, childTable))
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, parentTable))
	}
	cleanup()
	c.Cleanup(cleanup)

	_, err := conn.Exec("CREATE TABLE " + parentTable + " (id VARCHAR(36) PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.Exec("CREATE TABLE " + childTable + " (" +
		"id VARCHAR(36) PRIMARY KEY, parent_id VARCHAR(36), other_id VARCHAR(36), " +
		"INDEX " + foreignKey + " (other_id))")
	c.Assert(err, qt.IsNil)
	_, err = conn.Exec("ALTER TABLE " + childTable + " ADD CONSTRAINT " + foreignKey +
		" FOREIGN KEY (parent_id) REFERENCES " + parentTable + " (id)")
	c.Assert(err, qt.Not(qt.IsNil))

	after, readErr := conn.Reader().ReadSchema()
	c.Assert(readErr, qt.IsNil)
	c.Assert(hasNamedForeignKey(after, childTable, foreignKey), qt.IsFalse)
	c.Assert(hasNamedIndex(after, childTable, foreignKey), qt.IsTrue)
}

func TestMySQLFamilyAddedForeignKeyColumnDownRoundTripIntegration(t *testing.T) {
	engines := []struct {
		name   string
		envKey string
	}{
		{name: "mysql", envKey: "MYSQL_URL"},
		{name: "mariadb", envKey: "MARIADB_URL"},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn := requireGeneratorDatabaseConnection(t, engine.envKey)
			runAddedForeignKeyColumnRoundTrip(c, conn)
		})
	}
}

func TestMySQLFamilyAddedReferencedColumnDownRoundTripIntegration(t *testing.T) {
	engines := []struct {
		name   string
		envKey string
	}{
		{name: "mysql", envKey: "MYSQL_URL"},
		{name: "mariadb", envKey: "MARIADB_URL"},
	}

	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			conn := requireGeneratorDatabaseConnection(t, engine.envKey)
			runAddedReferencedColumnRoundTrip(c, conn)
		})
	}
}

func runAddedReferencedColumnRoundTrip(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	const (
		parentTable = "ptah_fk_ref_column_parents"
		childTable  = "ptah_fk_ref_column_children"
		foreignKey  = "fk_parent_code"
	)

	cleanup := func() {
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, childTable))
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, parentTable))
	}
	cleanup()
	c.Cleanup(cleanup)

	prior := addedReferencedColumnSchema(false)
	setupSQL, _ := generateLiveMigrationSQL(c, conn, prior)
	execScript(c, conn, setupSQL, "SETUP")

	target := addedReferencedColumnSchema(true)
	upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
	execScript(c, conn, upSQL, "UP")

	afterUp, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterUp, childTable, foreignKey), qt.IsTrue)
	c.Assert(hasNamedColumn(afterUp, parentTable, "code"), qt.IsTrue)

	upperDown := strings.ToUpper(downSQL)
	dropForeignKey := strings.Index(upperDown, "DROP FOREIGN KEY")
	dropColumn := strings.Index(upperDown, "DROP COLUMN")
	c.Assert(dropForeignKey >= 0, qt.IsTrue)
	c.Assert(dropColumn >= 0, qt.IsTrue)
	c.Assert(dropForeignKey < dropColumn, qt.IsTrue)
	execScript(c, conn, downSQL, "DOWN")

	afterDown, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterDown, childTable, foreignKey), qt.IsFalse)
	c.Assert(hasNamedColumn(afterDown, parentTable, "code"), qt.IsFalse)
	c.Assert(hasNamedColumn(afterDown, childTable, "parent_code"), qt.IsTrue)
	c.Assert(hasNamedIndex(afterDown, childTable, "idx_parent_code"), qt.IsTrue)
}

func addedReferencedColumnSchema(withReference bool) *goschema.Database {
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKRefColumnParent", Name: "ptah_fk_ref_column_parents"},
			{StructName: "PtahFKRefColumnChild", Name: "ptah_fk_ref_column_children"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKRefColumnParent", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKRefColumnChild", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKRefColumnChild", Name: "parent_code", Type: "VARCHAR(36)", Nullable: true},
		},
		Indexes: []goschema.Index{{
			StructName: "PtahFKRefColumnChild", Name: "idx_parent_code", Fields: []string{"parent_code"},
		}},
	}
	if withReference {
		database.Fields = append(database.Fields, goschema.Field{
			StructName: "PtahFKRefColumnParent", Name: "code", Type: "VARCHAR(36)", Unique: true,
		})
		database.Fields[2].Foreign = "ptah_fk_ref_column_parents(code)"
		database.Fields[2].ForeignKeyName = "fk_parent_code"
	}
	goschema.Finalize(database)
	return database
}

func runAddedForeignKeyColumnRoundTrip(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	const (
		parentTable = "ptah_fk_column_parents"
		childTable  = "ptah_fk_column_children"
		foreignKey  = "fk_added_parent"
	)

	cleanup := func() {
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, childTable))
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, parentTable))
	}
	cleanup()
	c.Cleanup(cleanup)

	prior := addedForeignKeyColumnSchema(false)
	setupSQL, _ := generateLiveMigrationSQL(c, conn, prior)
	execScript(c, conn, setupSQL, "SETUP")

	target := addedForeignKeyColumnSchema(true)
	upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
	execScript(c, conn, upSQL, "UP")

	afterUp, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterUp, childTable, foreignKey), qt.IsTrue)
	c.Assert(hasNamedIndex(afterUp, childTable, foreignKey), qt.IsTrue)
	c.Assert(hasNamedColumn(afterUp, childTable, "parent_id"), qt.IsTrue)

	upperDown := strings.ToUpper(downSQL)
	dropForeignKey := strings.Index(upperDown, "DROP FOREIGN KEY")
	dropColumn := strings.Index(upperDown, "DROP COLUMN")
	c.Assert(dropForeignKey >= 0, qt.IsTrue)
	c.Assert(dropColumn >= 0, qt.IsTrue)
	c.Assert(dropForeignKey < dropColumn, qt.IsTrue)
	c.Assert(upperDown, qt.Not(qt.Contains), "DROP INDEX")
	execScript(c, conn, downSQL, "DOWN")

	afterDown, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterDown, childTable, foreignKey), qt.IsFalse)
	c.Assert(hasNamedIndex(afterDown, childTable, foreignKey), qt.IsFalse)
	c.Assert(hasNamedColumn(afterDown, childTable, "parent_id"), qt.IsFalse)
	c.Assert(hasNamedColumn(afterDown, childTable, "id"), qt.IsTrue)
}

func addedForeignKeyColumnSchema(withForeignKeyColumn bool) *goschema.Database {
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKColumnParent", Name: "ptah_fk_column_parents"},
			{StructName: "PtahFKColumnChild", Name: "ptah_fk_column_children"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKColumnParent", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKColumnChild", Name: "id", Type: "VARCHAR(36)", Primary: true},
		},
	}
	if withForeignKeyColumn {
		database.Fields = append(database.Fields, goschema.Field{
			StructName:     "PtahFKColumnChild",
			Name:           "parent_id",
			Type:           "VARCHAR(36)",
			Nullable:       true,
			Foreign:        "ptah_fk_column_parents(id)",
			ForeignKeyName: "fk_added_parent",
		})
	}
	goschema.Finalize(database)
	return database
}

func runForeignKeyBackingIndexRoundTrip(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	preexistingIndexName string,
) {
	c.Helper()
	const (
		parentTable = "ptah_fk_backing_parents"
		childTable  = "ptah_fk_backing_children"
		foreignKey  = "fk_parent"
	)

	cleanup := func() {
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, childTable))
		_, _ = conn.Exec(dropTableSQL(conn.Info().Dialect, parentTable))
	}
	cleanup()
	c.Cleanup(cleanup)

	prior := foreignKeyBackingSchema(false, preexistingIndexName)
	setupSQL, _ := generateLiveMigrationSQL(c, conn, prior)
	execScript(c, conn, setupSQL, "SETUP")

	target := foreignKeyBackingSchema(true, preexistingIndexName)
	upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
	c.Assert(upSQL, qt.Contains, foreignKey)
	execScript(c, conn, upSQL, "UP")

	afterUp, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterUp, childTable, foreignKey), qt.IsTrue)
	wantBackingIndexName := foreignKey
	if preexistingIndexName != "" {
		wantBackingIndexName = preexistingIndexName
	}
	c.Assert(hasNamedIndex(afterUp, childTable, wantBackingIndexName), qt.IsTrue)

	c.Assert(downSQL, qt.Contains, foreignKey)
	if preexistingIndexName != "" {
		c.Assert(strings.ToUpper(downSQL), qt.Not(qt.Contains), "DROP INDEX")
	} else {
		c.Assert(strings.ToUpper(downSQL), qt.Contains, "DROP INDEX")
	}
	execScript(c, conn, downSQL, "DOWN")

	afterDown, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(hasNamedForeignKey(afterDown, childTable, foreignKey), qt.IsFalse)
	c.Assert(
		hasNamedIndex(afterDown, childTable, wantBackingIndexName),
		qt.Equals,
		preexistingIndexName != "",
	)
}

func foreignKeyBackingSchema(withForeignKey bool, indexName string) *goschema.Database {
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKBackingParent", Name: "ptah_fk_backing_parents"},
			{StructName: "PtahFKBackingChild", Name: "ptah_fk_backing_children"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKBackingParent", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKBackingChild", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKBackingChild", Name: "parent_id", Type: "VARCHAR(36)", Nullable: true},
		},
	}
	if withForeignKey {
		database.Fields[2].Foreign = "ptah_fk_backing_parents(id)"
		database.Fields[2].ForeignKeyName = "fk_parent"
	}
	if indexName != "" {
		database.Indexes = []goschema.Index{{
			StructName: "PtahFKBackingChild",
			Name:       indexName,
			Fields:     []string{"parent_id"},
		}}
	}
	goschema.Finalize(database)
	return database
}

func hasNamedForeignKey(schema *dbschematypes.DBSchema, table, name string) bool {
	for _, constraint := range schema.Constraints {
		if constraint.TableName == table && constraint.Name == name && constraint.Type == "FOREIGN KEY" {
			return true
		}
	}
	return false
}

func hasNamedIndex(schema *dbschematypes.DBSchema, table, name string) bool {
	for _, index := range schema.Indexes {
		if index.TableName == table && index.Name == name {
			return true
		}
	}
	return false
}

func hasNamedColumn(schema *dbschematypes.DBSchema, table, name string) bool {
	for _, candidate := range schema.Tables {
		if candidate.Name != table {
			continue
		}
		for _, column := range candidate.Columns {
			if column.Name == name {
				return true
			}
		}
	}
	return false
}
