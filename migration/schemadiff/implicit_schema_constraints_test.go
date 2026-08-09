package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDialect_ImplicitDatabaseSchemaMatchesTheDesiredSchema pins that
// a table the database reports with no schema is the same table the desired
// schema names explicitly (stokaro/ptah#1232).
//
// The two sides genuinely disagree in spelling. A reader reports the schema as
// empty wherever the engine treats it as implicit, while a schema built from Go
// annotations or from HCL carries it. Table comparison has always resolved that
// through the dialect's default schema; constraint and trigger comparison did
// not, so every constraint the database had looked absent from the desired
// schema and was reported as removed.
//
// What that cost is worth stating precisely, because the two engines hid it
// differently. On SQLite the second `schema apply` of an unchanged file failed
// with `rebuilding table t requires the retained table definition`, which is at
// least loud. On PostgreSQL it emitted
// `ALTER TABLE "users" DROP CONSTRAINT IF EXISTS "users_pkey"`, applied it,
// exited 0 and printed "Schema apply completed successfully" -- the primary key
// was gone and the third apply then reported "Schema is synced". The pinned
// Atlas community binary v1.3.0 reports "Schema is synced, no changes to be
// made" on the second apply of both.
//
// The removal rows are the control and they carry the weight: a fix that simply
// stopped reporting constraint removals would satisfy every no-change row here
// and leave the tool unable to drop a constraint at all.
func TestCompareWithDialect_ImplicitDatabaseSchemaMatchesTheDesiredSchema(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		genSchema   string
		dbSchema    string
		dbTableName string
		wantRemoved []string
	}{
		{
			name:        "sqlite reports the schema as nothing where the desired side says main",
			dialect:     "sqlite",
			genSchema:   "main",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			name:        "postgres reports the schema as nothing where the desired side says public",
			dialect:     "postgres",
			genSchema:   "public",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			name:        "both sides naming the schema is unaffected",
			dialect:     "postgres",
			genSchema:   "public",
			dbSchema:    "public",
			dbTableName: "users",
		},
		{
			name:        "neither side naming it is unaffected",
			dialect:     "postgres",
			genSchema:   "",
			dbSchema:    "",
			dbTableName: "users",
		},
		{
			// The desired side omits the schema and the database supplies it.
			// The fill-in has to work in this direction too, or a schema read
			// from a server that does report `public` would drift against Go
			// annotations that do not name one.
			name:        "the database naming it and the desired side not is unaffected",
			dialect:     "postgres",
			genSchema:   "",
			dbSchema:    "public",
			dbTableName: "users",
		},
		{
			// The control. A constraint on a table that genuinely is not in the
			// desired schema must still be reported, or the fix has bought
			// idempotency by making removal impossible.
			name:        "a constraint on a table the desired schema does not have is still removed",
			dialect:     "postgres",
			genSchema:   "public",
			dbSchema:    "",
			dbTableName: "orphaned",
			wantRemoved: []string{"orphaned_pkey"},
		},
		{
			// The other control: a different schema is a different table, and
			// the fill-in must not collapse them.
			name:        "a table in another schema is not the desired one",
			dialect:     "postgres",
			genSchema:   "public",
			dbSchema:    "reporting",
			dbTableName: "users",
			wantRemoved: []string{"users_pkey"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				implicitSchemaDesired(test.genSchema),
				implicitSchemaDatabase(test.dbSchema, test.dbTableName),
				test.dialect,
			)

			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, test.wantRemoved,
				qt.Commentf("diff: %#v", diff))
		})
	}
}

// TestCompareWithDialect_ImplicitDatabaseSchemaPlansNothingAtAll is the
// assertion that a fix in the wrong place cannot satisfy.
//
// The SQLite planner reached `findTable` with the database's unqualified
// spelling while the generated table carried the qualified one, and returned an
// error. Teaching only `findTable` to match would have removed the error and
// replaced it with a real table rebuild -- CREATE, INSERT ... SELECT, DROP
// TABLE, RENAME -- on every apply of an unchanged file. So the assertion is
// that the comparison reports NO change, not merely that planning succeeds.
func TestCompareWithDialect_ImplicitDatabaseSchemaPlansNothingAtAll(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		genSchema string
	}{
		{name: "sqlite", dialect: "sqlite", genSchema: "main"},
		{name: "postgres", dialect: "postgres", genSchema: "public"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				implicitSchemaDesired(test.genSchema),
				implicitSchemaDatabase("", "users"),
				test.dialect,
			)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareWithDialect_ImplicitDatabaseSchemaMatchesTriggers covers the same
// mismatch on triggers, which key their owning table the same way.
//
// It is separate from the constraint run because a trigger reaching the diff is
// reported as removed AND added -- the desired one is unmatched too -- so the
// symptom is a pointless drop-and-recreate rather than a bare drop.
func TestCompareWithDialect_ImplicitDatabaseSchemaMatchesTriggers(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		genSchema string
		dbSchema  string
		wantSame  bool
	}{
		{name: "sqlite implicit database schema", dialect: "sqlite", genSchema: "main", dbSchema: "", wantSame: true},
		{name: "postgres implicit database schema", dialect: "postgres", genSchema: "public", dbSchema: "", wantSame: true},
		{name: "a genuinely different schema still differs", dialect: "postgres", genSchema: "public", dbSchema: "reporting", wantSame: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			generated := implicitSchemaDesired(test.genSchema)
			generated.Triggers = []goschema.Trigger{{
				StructName: "Users",
				Name:       "users_audit",
				Table:      qualify(test.genSchema, "users"),
				Timing:     "BEFORE",
				Event:      "INSERT",
				Body:       "SELECT 1",
			}}
			database := implicitSchemaDatabase(test.dbSchema, "users")
			database.Triggers = []types.DBTrigger{{
				Name:   "users_audit",
				Table:  "users",
				Schema: test.dbSchema,
				Timing: "BEFORE",
				Event:  "INSERT",
				Body:   "SELECT 1",
			}}

			diff := schemadiff.CompareWithDialect(generated, database, test.dialect)

			c.Assert(len(diff.TriggersRemoved) == 0, qt.Equals, test.wantSame,
				qt.Commentf("removed: %#v", diff.TriggersRemoved))
			c.Assert(len(diff.TriggersAdded) == 0, qt.Equals, test.wantSame,
				qt.Commentf("added: %#v", diff.TriggersAdded))
		})
	}
}

// implicitSchemaDesired builds the desired side: one table with a table-level
// primary key, in the named schema.
func implicitSchemaDesired(schema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "Users",
			Name:       "users",
			Schema:     schema,
			PrimaryKey: []string{"id"},
		}},
		Fields: []goschema.Field{
			{StructName: "Users", Name: "id", Type: "INTEGER", Nullable: false},
			{StructName: "Users", Name: "email", Type: "TEXT", Nullable: true},
		},
	}
}

// implicitSchemaDatabase builds the database side: the same table, as a reader
// reports it.
func implicitSchemaDatabase(schema, table string) *types.DBSchema {
	return &types.DBSchema{
		Tables: []types.DBTable{{
			Name:   table,
			Schema: schema,
			Type:   "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "INTEGER", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "TEXT", IsNullable: "YES"},
			},
		}},
		Constraints: []types.DBConstraint{{
			Name:        table + "_pkey",
			TableName:   table,
			Schema:      schema,
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"id"},
		}},
	}
}

// qualify writes a table name the way the desired schema carries it.
func qualify(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}
