package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// modifiedColumnOn is a diff that changes one column of table.
func modifiedColumnOn(table string, desired schemamodel.Field, changes map[string]string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: table,
			ColumnsModified: []difftypes.ColumnDiff{
				{ColumnName: desired.Name, Desired: desired, Changes: changes},
			},
		}},
	}
}

// A column that gains a column-level UNIQUE gets the constraint, under the
// name PostgreSQL gives the same UNIQUE inside CREATE TABLE. Without it the
// plan reports `unique: false -> true` and adds nothing, so every later
// comparison reports it again (stokaro/ptah#3649). Each statement is what
// Atlas CE v1.3.0 writes for the same change, spelled as Ptah spells it.
func TestGenerateSchemaDiffSQLStatements_ColumnGainsUnique(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		desired schemamodel.Field
		changes map[string]string
		want    []string
	}{
		{
			name:    "UNIQUE alone",
			table:   "flags",
			desired: schemamodel.Field{Name: "code", Type: "INTEGER", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"unique": "false -> true"},
			want: []string{"-- Add/modify columns for table: flags --\n" +
				"-- Modify column flags.code: unique: false -> true --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "flags" ADD CONSTRAINT "flags_code_key" UNIQUE ("code")`},
		},
		{
			name:    "a table in another schema is named without it",
			table:   "app.flags",
			desired: schemamodel.Field{Name: "code", Type: "INTEGER", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"unique": "false -> true"},
			want: []string{"-- Add/modify columns for table: app.flags --\n" +
				"-- Modify column app.flags.code: unique: false -> true --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "app"."flags" ADD CONSTRAINT "flags_code_key" UNIQUE ("code")`},
		},
		{
			name:  "a name past 63 bytes is cut as the server cuts it",
			table: "t3649_a_rather_long_table_name_for_truncation",
			desired: schemamodel.Field{
				Name: "a_rather_long_column_name_too", Type: "INTEGER", StructName: "Flag", Nullable: true, Unique: true,
			},
			changes: map[string]string{"unique": "false -> true"},
			want: []string{"-- Add/modify columns for table: t3649_a_rather_long_table_name_for_truncation --\n" +
				"-- Modify column t3649_a_rather_long_table_name_for_truncation.a_rather_long_column_name_too: unique: false -> true --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "t3649_a_rather_long_table_name_for_truncation" ADD CONSTRAINT ` +
				`"t3649_a_rather_long_table_nam_a_rather_long_column_name_too_key" UNIQUE ("a_rather_long_column_name_too")`},
		},
		{
			name:    "after the column's other clauses",
			table:   "flags",
			desired: schemamodel.Field{Name: "code", Type: "BIGINT", StructName: "Flag", Nullable: true, Unique: true},
			changes: map[string]string{"unique": "false -> true", "type": "int4 -> bigint"},
			want: []string{
				"-- Add/modify columns for table: flags --\n" +
					"-- Modify column flags.code: type: int4 -> bigint, unique: false -> true --\n" +
					"-- ALTER statements: --\n" +
					`ALTER TABLE "flags" ALTER COLUMN "code" TYPE BIGINT`,
				"-- ALTER statements: --\n" +
					`ALTER TABLE "flags" ADD CONSTRAINT "flags_code_key" UNIQUE ("code")`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(modifiedColumnOn(test.table, test.desired, test.changes), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The changes below add no constraint from the column. A UNIQUE the column
// loses is a live constraint the desired schema lacks, which the plan drops by
// its name as a removed constraint; unique_expr asks for uniqueness over an
// expression, which UNIQUE on the raw column would not enforce; and a PRIMARY
// KEY flag is not a UNIQUE.
func TestGenerateSchemaDiffSQLStatements_ColumnUniqueAddsNothing(t *testing.T) {
	tests := []struct {
		name    string
		desired schemamodel.Field
		changes map[string]string
		want    []string
	}{
		{
			name:    "UNIQUE lost",
			desired: schemamodel.Field{Name: "code", Type: "INTEGER", StructName: "Flag", Nullable: true},
			changes: map[string]string{"unique": "true -> false"},
			want:    []string{"-- Add/modify columns for table: flags --\n-- Modify column flags.code: unique: true -> false --"},
		},
		{
			name:    "unique_expr declared",
			desired: schemamodel.Field{Name: "code", Type: "TEXT", StructName: "Flag", Nullable: true, Unique: true, UniqueExpr: "lower(code)"},
			changes: map[string]string{"unique": "false -> true"},
			want:    []string{"-- Add/modify columns for table: flags --\n-- Modify column flags.code: unique: false -> true --"},
		},
		{
			name:    "PRIMARY KEY gained",
			desired: schemamodel.Field{Name: "code", Type: "INTEGER", StructName: "Flag", Primary: true},
			changes: map[string]string{"primary_key": "false -> true"},
			want:    []string{"-- Add/modify columns for table: flags --\n-- Modify column flags.code: primary_key: false -> true --"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(modifiedColumnOn("flags", test.desired, test.changes), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
