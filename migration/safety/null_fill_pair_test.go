package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/migration/safety"
)

// judged is one assessment the tests compare: what the report says about one
// statement.
type judged struct {
	Severity  safety.Severity
	Reason    string
	Statement string
}

func judgedOf(assessments []safety.StatementAssessment) []judged {
	out := make([]judged, 0, len(assessments))
	for _, assessment := range assessments {
		out = append(out, judged{Severity: assessment.Severity, Reason: assessment.Reason, Statement: assessment.Statement})
	}
	return out
}

func notNullModification(column *ast.ColumnNode, omit bool) *ast.ModifyColumnOperation {
	return &ast.ModifyColumnOperation{
		Column:           column,
		Changed:          ast.ColumnProperties{Nullability: true},
		HasChanged:       true,
		OmitNullBackfill: omit,
	}
}

const (
	fillReason       = "UPDATE rewrites the column's NULL rows with its declared default before SET NOT NULL"
	afterFillReason  = "SET NOT NULL follows the UPDATE that fills the column's NULL rows"
	setNotNullReason = "SET NOT NULL can fail when existing rows contain NULL"
	// modifyNotNullReason is what a MySQL-family MODIFY ... NOT NULL is
	// reported with (stokaro/ptah#3669).
	modifyNotNullReason = "MODIFY or CHANGE ... NOT NULL fails when a row holds NULL; " +
		"outside strict SQL mode MySQL and MariaDB rewrite the NULL to the type's zero value instead"
)

func fillStatement(column string) string {
	return "DO $$\nBEGIN\n" +
		"    IF EXISTS (SELECT 1 FROM \"flags\" WHERE \"" + column + "\" IS NULL LIMIT 1) THEN\n" +
		"        UPDATE \"flags\" SET \"" + column + "\" = '9' WHERE \"" + column + "\" IS NULL;\n" +
		"    END IF;\nEND\n$$"
}

// The PostgreSQL plan for a column made NOT NULL with a declared default is a
// fill and a SET NOT NULL, and the report judges each for what it does. Read by
// its words alone, the fill is a DO block that matches no rule and reads safe,
// though it rewrites every NULL row of the column, and the SET NOT NULL reads
// as a statement that can fail on a NULL row the fill has just removed
// (stokaro/ptah#3660).
//
// The other rows are the controls. With the fill omitted, as ptah-compat plans
// under PTAH_ATLAS_STRICT_COMPAT=1, the SET NOT NULL can fail and says so. In
// one ALTER TABLE holding a filled column and one without a default, each
// SET NOT NULL is judged with its own column.
func TestAssessRendered_NullFillPair(t *testing.T) {
	tests := []struct {
		name       string
		operations []ast.AlterOperation
		want       []judged
	}{
		{
			name:       "a declared default fills the NULL rows",
			operations: []ast.AlterOperation{notNullModification(ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), false)},
			want: []judged{
				{Severity: safety.Warning, Reason: fillReason, Statement: "-- ALTER statements: --\n" + fillStatement("c")},
				{Severity: safety.Safe, Reason: afterFillReason, Statement: `ALTER TABLE "flags" ALTER COLUMN "c" SET NOT NULL`},
			},
		},
		{
			name:       "the fill omitted",
			operations: []ast.AlterOperation{notNullModification(ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), true)},
			want: []judged{{
				Severity: safety.Warning,
				Reason:   setNotNullReason,
				Statement: "-- ALTER statements: --\n" +
					"-- POSTGRES: SET NOT NULL fails if any row of \"flags\" holds NULL in \"c\"; this plan does not fill it with the column's default.\n" +
					`ALTER TABLE "flags" ALTER COLUMN "c" SET NOT NULL`,
			}},
		},
		{
			name: "a filled column beside one without a default",
			operations: []ast.AlterOperation{
				notNullModification(ast.NewColumn("a", "INTEGER").SetNotNull().SetDefault("9"), false),
				notNullModification(ast.NewColumn("b", "INTEGER").SetNotNull(), false),
			},
			want: []judged{
				{Severity: safety.Warning, Reason: fillReason, Statement: "-- ALTER statements: --\n" + fillStatement("a")},
				{Severity: safety.Safe, Reason: afterFillReason, Statement: `ALTER TABLE "flags" ALTER COLUMN "a" SET NOT NULL`},
				{
					Severity: safety.Warning,
					Reason:   setNotNullReason,
					Statement: "-- ALTER statements: --\n" +
						"-- POSTGRES: SET NOT NULL fails if any row of \"flags\" holds NULL in \"b\"; the column declares no default to fill it with.\n" +
						`ALTER TABLE "flags" ALTER COLUMN "b" SET NOT NULL`,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "flags", Operations: test.operations}

			assessments, err := safety.AssessRendered([]ast.Node{node}, platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(judgedOf(assessments), qt.DeepEquals, test.want)
		})
	}
}

// MySQL's MODIFY carries the default and fills nothing, so the pair rule does
// not reach it, and an ALTER TABLE carrying several modifications is assessed
// as rendered, not one operation at a time: the second statement carries no
// header comment of its own.
func TestAssessRendered_NullFillPairIsPostgreSQLOnly(t *testing.T) {
	tests := []struct {
		name       string
		operations []ast.AlterOperation
		want       []judged
	}{
		{
			name:       "one modification",
			operations: []ast.AlterOperation{notNullModification(ast.NewColumn("c", "INTEGER").SetNotNull().SetDefault("9"), false)},
			want: []judged{{
				Severity:  safety.Warning,
				Reason:    modifyNotNullReason,
				Statement: "-- ALTER statements: --\nALTER TABLE `flags` MODIFY COLUMN `c` INTEGER NOT NULL DEFAULT 9",
			}},
		},
		{
			name: "two modifications",
			operations: []ast.AlterOperation{
				notNullModification(ast.NewColumn("a", "INTEGER").SetNotNull().SetDefault("9"), false),
				notNullModification(ast.NewColumn("b", "INTEGER").SetNotNull(), false),
			},
			want: []judged{
				{Severity: safety.Warning, Reason: modifyNotNullReason, Statement: "-- ALTER statements: --\nALTER TABLE `flags` MODIFY COLUMN `a` INTEGER NOT NULL DEFAULT 9"},
				{Severity: safety.Warning, Reason: modifyNotNullReason, Statement: "ALTER TABLE `flags` MODIFY COLUMN `b` INTEGER NOT NULL"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{Name: "flags", Operations: test.operations}

			assessments, err := safety.AssessRendered([]ast.Node{node}, platform.MySQL)

			c.Assert(err, qt.IsNil)
			c.Assert(judgedOf(assessments), qt.DeepEquals, test.want)
		})
	}
}
