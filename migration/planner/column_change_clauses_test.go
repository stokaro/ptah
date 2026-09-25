package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// oneModifiedColumn is a diff that changes one column of "flags".
func oneModifiedColumn(desired schemamodel.Field, changes map[string]string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName: "flags",
			ColumnsModified: []difftypes.ColumnDiff{
				{ColumnName: desired.Name, Desired: desired, Changes: changes},
			},
		}},
	}
}

// A column change is planned as the clauses for what changed and nothing
// else, one statement per clause, each preceded by the comment saying what it
// changes.
//
// The first row is stokaro/ptah#3645 as measured. Restating the column for a
// default-only change writes `ALTER COLUMN TYPE boolean` for a boolean column,
// a NULL backfill in a DO block, `SET NOT NULL` for a column that is NOT NULL,
// and then the `SET DEFAULT` that was asked for; Atlas CE v1.3.0 writes the
// last statement alone. With the comment after the statements, a plan that
// ends on a column change ends on a comment, and every writer terminates it
// as `-- Modify column ... --;`.
func TestGenerateSchemaDiffSQLStatements_ColumnChangeClauses_HappyPath(t *testing.T) {
	const header = "-- Add/modify columns for table: flags --\n"
	tests := []struct {
		name    string
		desired schemamodel.Field
		changes map[string]string
		want    []string
	}{
		{
			name:    "a default set",
			desired: schemamodel.Field{Name: "fresh", Type: "BOOLEAN", StructName: "Flag", Default: "true"},
			changes: map[string]string{"default_expr": " -> true"},
			want: []string{header +
				"-- Modify column flags.fresh: default_expr:  -> true --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "flags" ALTER COLUMN "fresh" SET DEFAULT true`},
		},
		{
			name:    "a default dropped",
			desired: schemamodel.Field{Name: "fresh", Type: "BOOLEAN", StructName: "Flag"},
			changes: map[string]string{"default": "false -> "},
			want: []string{header +
				"-- Modify column flags.fresh: default: false ->  --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "flags" ALTER COLUMN "fresh" DROP DEFAULT`},
		},
		{
			name:    "NOT NULL dropped",
			desired: schemamodel.Field{Name: "fresh", Type: "BOOLEAN", StructName: "Flag", Nullable: true},
			changes: map[string]string{"nullable": "false -> true"},
			want: []string{header +
				"-- Modify column flags.fresh: nullable: false -> true --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "flags" ALTER COLUMN "fresh" DROP NOT NULL`},
		},
		{
			name:    "a type changed",
			desired: schemamodel.Field{Name: "hits", Type: "BIGINT", StructName: "Flag"},
			changes: map[string]string{"type": "int4 -> bigint"},
			want: []string{header +
				"-- Modify column flags.hits: type: int4 -> bigint --\n" +
				"-- ALTER statements: --\n" +
				`ALTER TABLE "flags" ALTER COLUMN "hits" TYPE BIGINT`},
		},
		{
			name:    "a type and a default changed together",
			desired: schemamodel.Field{Name: "hits", Type: "BIGINT", StructName: "Flag", Default: "1"},
			changes: map[string]string{"type": "int4 -> bigint", "default_expr": " -> 1"},
			want: []string{
				header +
					"-- Modify column flags.hits: default_expr:  -> 1, type: int4 -> bigint --\n" +
					"-- ALTER statements: --\n" +
					`ALTER TABLE "flags" ALTER COLUMN "hits" TYPE BIGINT`,
				`ALTER TABLE "flags" ALTER COLUMN "hits" SET DEFAULT 1`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := planner.GenerateSchemaDiffSQLStatements(oneModifiedColumn(test.desired, test.changes), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// A change no ALTER COLUMN clause carries is reported by the comment alone.
// Restated for a UNIQUE flag, the column gets a TYPE, a NOT NULL and a DEFAULT
// clause, none of which adds the constraint. The plan adding it is
// stokaro/ptah#3649, and this test changes with it.
func TestGenerateSchemaDiffSQLStatements_UniqueOnlyChangeRestatesNothing(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Field{Name: "code", Type: "TEXT", StructName: "Flag", Nullable: true, Unique: true}

	got, err := planner.GenerateSchemaDiffSQLStatements(
		oneModifiedColumn(desired, map[string]string{"unique": "false -> true"}), platform.Postgres,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{
		"-- Add/modify columns for table: flags --\n-- Modify column flags.code: unique: false -> true --",
	})
}

// The MySQL family writes its column comment before the MODIFY it describes,
// for the reason the PostgreSQL plan does: after it, a plan that ends on a
// column change ends on a comment the writers terminate as `--;`. MODIFY
// COLUMN restates the whole definition by design; MySQL has no other form.
func TestGenerateSchemaDiffSQLStatements_MySQLColumnCommentPrecedesTheStatement(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Field{Name: "fresh", Type: "BOOLEAN", StructName: "Flag", Default: "true"}

	got, err := planner.GenerateSchemaDiffSQLStatements(
		oneModifiedColumn(desired, map[string]string{"default_expr": " -> true"}), platform.MySQL,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"-- Modify table: flags --\n" +
		"-- Modify column flags.fresh: default_expr:  -> true --\n" +
		"-- ALTER statements: --\n" +
		"ALTER TABLE `flags` MODIFY COLUMN `fresh` BOOLEAN NOT NULL DEFAULT 1"})
}
