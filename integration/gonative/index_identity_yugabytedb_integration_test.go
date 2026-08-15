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
	yugabyteIndexIdentitySchemaA = "ptah_770_yb_a"
	yugabyteIndexIdentitySchemaB = "ptah_770_yb_b"
	yugabyteIndexIdentityName    = "idx_ptah_770_yb_shared"
)

func TestYugabyteDBSchemaScopedIndexIdentity_RoundTrip(t *testing.T) {
	dsn := requireReachableEngine(t, dbtarget.YugabyteDB, "pgx", "YugabyteDB")
	c := qt.New(t)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	cleanupYugabyteIndexIdentity(db)
	defer cleanupYugabyteIndexIdentity(db)

	statements := []string{
		`CREATE SCHEMA ` + yugabyteIndexIdentitySchemaA,
		`CREATE SCHEMA ` + yugabyteIndexIdentitySchemaB,
		`CREATE TABLE ` + yugabyteIndexIdentitySchemaA + `.users (email TEXT NOT NULL)`,
		`CREATE TABLE ` + yugabyteIndexIdentitySchemaA + `.orders (reference TEXT NOT NULL)`,
		`CREATE TABLE ` + yugabyteIndexIdentitySchemaB + `.users (email TEXT NOT NULL)`,
		`CREATE INDEX ` + yugabyteIndexIdentityName + ` ON ` + yugabyteIndexIdentitySchemaA + `.users (email)`,
		`CREATE INDEX ` + yugabyteIndexIdentityName + ` ON ` + yugabyteIndexIdentitySchemaB + `.users (email)`,
	}
	for _, statement := range statements {
		_, err = db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("initialize YugabyteDB index identity: %s", statement))
	}

	target := yugabyteIndexIdentityTarget()
	live := readYugabyteIndexIdentitySchema(c, t, dsn)
	diff := schemadiff.CompareWithDialect(target, live, platform.YugabyteDB)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: yugabyteIndexIdentityName, TableName: yugabyteIndexIdentitySchemaA + ".orders"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: yugabyteIndexIdentityName, TableName: yugabyteIndexIdentitySchemaA + ".users"},
	})

	indexDiff := &difftypes.SchemaDiff{}
	indexDiff.SetIndexAdditions(diff.IndexAdditions())
	indexDiff.SetIndexRemovals(diff.IndexRemovals())
	planned, err := planner.GenerateSchemaDiffSQLStatements(indexDiff, target, platform.YugabyteDB)
	c.Assert(err, qt.IsNil)
	c.Assert(planned, qt.HasLen, 2)
	for _, statement := range planned {
		_, err = db.Exec(statement)
		c.Assert(err, qt.IsNil, qt.Commentf("apply YugabyteDB index identity plan: %s", statement))
	}

	live = readYugabyteIndexIdentitySchema(c, t, dsn)
	finalDiff := schemadiff.CompareWithDialect(target, live, platform.YugabyteDB)
	c.Assert(finalDiff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(finalDiff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(live.Indexes, qt.HasLen, 2)
}

func cleanupYugabyteIndexIdentity(db *sql.DB) {
	_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + yugabyteIndexIdentitySchemaA + ` CASCADE`)
	_, _ = db.Exec(`DROP SCHEMA IF EXISTS ` + yugabyteIndexIdentitySchemaB + ` CASCADE`)
}

func readYugabyteIndexIdentitySchema(c *qt.C, t *testing.T, dsn string) *dbschematypes.DBSchema {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), dsn)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{
		yugabyteIndexIdentitySchemaA,
		yugabyteIndexIdentitySchemaB,
	})
	c.Assert(err, qt.IsNil)
	live.Tables = slices.DeleteFunc(live.Tables, func(table dbschematypes.DBTable) bool {
		return table.Schema != yugabyteIndexIdentitySchemaA &&
			table.Schema != yugabyteIndexIdentitySchemaB
	})
	live.Indexes = slices.DeleteFunc(live.Indexes, func(index dbschematypes.DBIndex) bool {
		return index.Schema != yugabyteIndexIdentitySchemaA &&
			index.Schema != yugabyteIndexIdentitySchemaB
	})
	live.Constraints = slices.DeleteFunc(live.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return constraint.Schema != yugabyteIndexIdentitySchemaA &&
			constraint.Schema != yugabyteIndexIdentitySchemaB
	})
	return live
}

func yugabyteIndexIdentityTarget() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "YugabyteSchemaAUser", Schema: yugabyteIndexIdentitySchemaA, Name: "users"},
			{StructName: "YugabyteSchemaAOrder", Schema: yugabyteIndexIdentitySchemaA, Name: "orders"},
			{StructName: "YugabyteSchemaBUser", Schema: yugabyteIndexIdentitySchemaB, Name: "users"},
		},
		Indexes: []goschema.Index{
			{
				StructName: "YugabyteSchemaAOrder",
				Name:       yugabyteIndexIdentityName,
				Fields:     []string{"reference"},
			},
			{
				StructName: "YugabyteSchemaBUser",
				Name:       yugabyteIndexIdentityName,
				Fields:     []string{"email"},
			},
		},
	}
}
