//go:build integration

package gonative_test

import (
	"database/sql"
	"slices"
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
	postgresIndexIdentitySchemaA = "ptah_770_a"
	postgresIndexIdentitySchemaB = "ptah_770_b"
	postgresIndexIdentityName    = "idx_ptah_770_shared"
)

func TestPostgreSQLSchemaScopedIndexIdentity_MoveAndRoundTrip(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	cleanupPostgresIndexIdentity(t, db)
	defer cleanupPostgresIndexIdentity(t, db)

	_, err = db.Exec(`CREATE SCHEMA ` + postgresIndexIdentitySchemaA)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE SCHEMA ` + postgresIndexIdentitySchemaB)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE ` + postgresIndexIdentitySchemaA + `.users (email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE ` + postgresIndexIdentitySchemaA + `.orders (reference TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE TABLE ` + postgresIndexIdentitySchemaB + `.users (email TEXT NOT NULL)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE INDEX ` + postgresIndexIdentityName + ` ON ` + postgresIndexIdentitySchemaA + `.users (email)`)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(`CREATE INDEX ` + postgresIndexIdentityName + ` ON ` + postgresIndexIdentitySchemaB + `.users (email)`)
	c.Assert(err, qt.IsNil)

	target := postgresSchemaScopedIndexTarget()
	live := readPostgresIndexIdentitySchema(c, t, dsn)
	diff := schemadiff.CompareWithDialect(target, live, platform.Postgres)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: postgresIndexIdentityName, TableName: postgresIndexIdentitySchemaA + ".orders"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: postgresIndexIdentityName, TableName: postgresIndexIdentitySchemaA + ".users"},
	})

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, target, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "DROP INDEX")
	c.Assert(statements[1], qt.Contains, "CREATE INDEX")
	_, err = db.Exec(statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("apply schema-scoped index drop: %s", statements[0]))
	_, err = db.Exec(statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("apply schema-scoped index create: %s", statements[1]))

	live = readPostgresIndexIdentitySchema(c, t, dsn)
	finalDiff := schemadiff.CompareWithDialect(target, live, platform.Postgres)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 2)
}

func cleanupPostgresIndexIdentity(t *testing.T, db *sql.DB) {
	t.Helper()
	_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + postgresIndexIdentitySchemaA + ` CASCADE`)
	_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + postgresIndexIdentitySchemaB + ` CASCADE`)
}

func readPostgresIndexIdentitySchema(c *qt.C, t *testing.T, dsn string) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{
		postgresIndexIdentitySchemaA,
		postgresIndexIdentitySchemaB,
	})
	c.Assert(err, qt.IsNil)
	tables := slices.DeleteFunc(live.Tables, func(table dbschematypes.DBTable) bool {
		return table.Schema != postgresIndexIdentitySchemaA && table.Schema != postgresIndexIdentitySchemaB
	})
	indexes := slices.DeleteFunc(live.Indexes, func(index dbschematypes.DBIndex) bool {
		return index.Schema != postgresIndexIdentitySchemaA && index.Schema != postgresIndexIdentitySchemaB
	})
	constraints := slices.DeleteFunc(live.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return constraint.Schema != postgresIndexIdentitySchemaA && constraint.Schema != postgresIndexIdentitySchemaB
	})
	return &dbschematypes.DBSchema{
		Tables:      tables,
		Indexes:     indexes,
		Constraints: constraints,
	}
}

func postgresSchemaScopedIndexTarget() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "SchemaAUser", Schema: postgresIndexIdentitySchemaA, Name: "users"},
			{StructName: "SchemaAOrder", Schema: postgresIndexIdentitySchemaA, Name: "orders"},
			{StructName: "SchemaBUser", Schema: postgresIndexIdentitySchemaB, Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "SchemaAUser", Name: "email", Type: "TEXT"},
			{StructName: "SchemaAOrder", Name: "reference", Type: "TEXT"},
			{StructName: "SchemaBUser", Name: "email", Type: "TEXT"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "SchemaAOrder",
				Name:       postgresIndexIdentityName,
				Fields:     []string{"reference"},
			},
			{
				StructName: "SchemaBUser",
				Name:       postgresIndexIdentityName,
				Fields:     []string{"email"},
			},
		},
	}
}
