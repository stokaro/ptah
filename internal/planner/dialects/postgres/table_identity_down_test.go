package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// downColumnTarget returns the desired schema for a `users` table that has
// dropped the `legacy_note` column, spelling the table's schema as given.
func downColumnTarget(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users", Schema: tableSchema}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}
}

// downColumnDatabase returns the introspected schema the migration runs
// against: the same table, still carrying `legacy_note`, with the schema spelled
// as given.
func downColumnDatabase(tableSchema string) *dbtypes.DBSchema {
	return &dbtypes.DBSchema{
		Tables: []dbtypes.DBTable{{
			Name: "users", Schema: tableSchema, Type: "BASE TABLE",
			Columns: []dbtypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "legacy_note", DataType: "text", IsNullable: "YES", OrdinalPosition: 3},
			},
		}},
	}
}

// planDownStatements runs the whole forward-then-reverse path a `.down.sql` file
// is produced by: compare, reverse, and plan the reversal against the pre-change
// database converted back to a schema.
func planDownStatements(c *qt.C, generated *goschema.Database, database *dbtypes.DBSchema) []string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: generated,
		CurrentSchema: database,
		Dialect:       "postgres",
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		plan.Reverse.Diff,
		dbschematogo.ConvertDBSchemaToGoSchema(database),
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	return statements
}

// containsStatement reports whether any statement contains fragment.
func containsStatement(statements []string, fragment string) bool {
	for _, statement := range statements {
		if strings.Contains(statement, fragment) {
			return true
		}
	}
	return false
}

// TestDownMigrationRestoresDroppedColumnAcrossSchemaSpellings is the regression
// for the silent half of this defect.
//
// The forward migration drops `legacy_note` with CASCADE. The rollback has to
// put it back, and it is planned against the pre-change database converted to a
// schema -- which spells the table's schema the way the CATALOG reported it,
// while the diff carries the way the DESIRED schema spells it. Table comparison
// keys through identifier semantics, so the two spellings are one modified
// table; the planner's own lookup did not, so it resolved nothing, found no
// field, and emitted no statement at all. The rollback said nothing was needed
// and the column stayed dropped.
func TestDownMigrationRestoresDroppedColumnAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name         string
		targetSchema string
		dbSchema     string
		wantTable    string
	}{
		{
			// Control. Both sides already agree, so this row passes with or
			// without the fix and proves the change does not disturb the
			// ordinary case.
			name:         "both sides spell the table the same way",
			targetSchema: "",
			dbSchema:     "",
			wantTable:    `"users"`,
		},
		{
			// The desired schema comes from a document that qualifies (HCL, or a
			// SQL file naming public explicitly) while the PostgreSQL reader
			// reports an empty schema for the connected one.
			name:         "the target qualifies public and the database reports it bare",
			targetSchema: "public",
			dbSchema:     "",
			wantTable:    `"public"."users"`,
		},
		{
			// The mirror image: Go annotations leave the schema out while the
			// database side carries it.
			name:         "the target is bare and the database reports public",
			targetSchema: "",
			dbSchema:     "public",
			wantTable:    `"users"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements := planDownStatements(
				c,
				downColumnTarget(test.targetSchema),
				downColumnDatabase(test.dbSchema),
			)
			c.Assert(
				containsStatement(statements, "ADD COLUMN "+`"legacy_note"`),
				qt.IsTrue,
				qt.Commentf("down plan:\n%s", strings.Join(statements, "\n")),
			)
			c.Assert(
				containsStatement(statements, "ALTER TABLE "+test.wantTable+" ADD COLUMN"),
				qt.IsTrue,
				qt.Commentf("down plan:\n%s", strings.Join(statements, "\n")),
			)
		})
	}
}

// TestPlannerColumnLookupDoesNotGuessBetweenSchemas is the control that a fix
// resolving too much cannot pass.
//
// A lookup that matched on the unqualified name alone would answer these rows,
// and answering them is wrong: `app.users` and `reporting.users` are two
// relations, and putting a column on the wrong one is the mistake the resolver
// exists to prevent.
func TestPlannerColumnLookupDoesNotGuessBetweenSchemas(t *testing.T) {
	t.Run("a named non-default schema selects its own table", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Tables: []goschema.Table{
				{StructName: "PublicUser", Name: "users", Schema: "public"},
				{StructName: "AppUser", Name: "users", Schema: "app"},
			},
			Fields: []goschema.Field{
				{StructName: "PublicUser", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "AppUser", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "AppUser", Name: "note", Type: "TEXT"},
			},
		}
		database := &dbtypes.DBSchema{
			Tables: []dbtypes.DBTable{
				{
					Name: "users", Schema: "public", Type: "BASE TABLE",
					Columns: []dbtypes.DBColumn{
						{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
					},
				},
				{
					Name: "users", Schema: "app", Type: "BASE TABLE",
					Columns: []dbtypes.DBColumn{
						{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
					},
				},
			},
		}
		diff := schemadiff.CompareWithDialect(generated, database, "postgres")
		statements, err := planner.GenerateSchemaDiffSQLStatements(diff, generated, "postgres")
		c.Assert(err, qt.IsNil)
		c.Assert(
			containsStatement(statements, `ALTER TABLE "app"."users" ADD COLUMN "note"`),
			qt.IsTrue,
			qt.Commentf("plan:\n%s", strings.Join(statements, "\n")),
		)
		c.Assert(
			containsStatement(statements, `ALTER TABLE "public"."users" ADD COLUMN "note"`),
			qt.IsFalse,
			qt.Commentf("plan:\n%s", strings.Join(statements, "\n")),
		)
	})

	t.Run("a table the schema does not declare gets no column DDL", func(t *testing.T) {
		c := qt.New(t)
		generated := &goschema.Database{
			Tables: []goschema.Table{{StructName: "Other", Name: "other", Schema: "public"}},
			Fields: []goschema.Field{{StructName: "Other", Name: "id", Type: "INTEGER", Primary: true}},
		}
		statements, err := planner.GenerateSchemaDiffSQLStatements(
			&types.SchemaDiff{TablesModified: []types.TableDiff{{
				TableName:    "reporting.users",
				ColumnsAdded: []string{"note"},
			}}},
			generated,
			"postgres",
		)
		c.Assert(err, qt.IsNil)
		c.Assert(
			containsStatement(statements, "ADD COLUMN"),
			qt.IsFalse,
			qt.Commentf("plan:\n%s", strings.Join(statements, "\n")),
		)
	})
}
