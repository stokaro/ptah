package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/migration/safety"
)

// A column modification that names what it changes is judged by that. A
// default-only change on a NOT NULL column renders as one SET DEFAULT; judged
// by the column alone it is reported as "SET NOT NULL can fail when existing
// rows contain NULL", a statement the plan does not contain
// (stokaro/ptah#3645). The last row is the control: a modification that does
// not say what it changes is judged by the column.
func TestAssessRendered_ModifyColumnJudgedByChangedProperties(t *testing.T) {
	tests := []struct {
		name         string
		column       *ast.ColumnNode
		changed      ast.ColumnProperties
		hasChanged   bool
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name:         "a default set",
			column:       ast.NewColumn("fresh", "BOOLEAN").SetNotNull().SetDefault("true"),
			changed:      ast.ColumnProperties{Default: true},
			hasChanged:   true,
			wantSeverity: safety.Safe,
			wantReason:   "does not remove data or tighten constraints",
		},
		{
			name:         "a default dropped",
			column:       ast.NewColumn("fresh", "BOOLEAN").SetNotNull(),
			changed:      ast.ColumnProperties{Default: true},
			hasChanged:   true,
			wantSeverity: safety.Warning,
			wantReason:   "DROP DEFAULT can break writers that leave the column out",
		},
		{
			name:         "NOT NULL set",
			column:       ast.NewColumn("fresh", "JSONB").SetNotNull(),
			changed:      ast.ColumnProperties{Nullability: true},
			hasChanged:   true,
			wantSeverity: safety.Warning,
			wantReason:   "SET NOT NULL can fail when existing rows contain NULL",
		},
		{
			name:         "changes not stated",
			column:       ast.NewColumn("fresh", "BOOLEAN").SetNotNull().SetDefault("true"),
			wantSeverity: safety.Warning,
			wantReason:   "SET NOT NULL can fail when existing rows contain NULL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := &ast.AlterTableNode{
				Name: "flags",
				Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
					Column:     test.column,
					Changed:    test.changed,
					HasChanged: test.hasChanged,
				}},
			}

			got := safety.Classify(node)
			assessments := safety.Assess([]ast.Node{node})

			c.Assert(got, qt.Equals, test.wantSeverity)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// Rendered, a default-only change is one statement, and the assessment it
// takes from the modification is a default change's, not a SET NOT NULL's.
func TestAssessRendered_DefaultOnlyChangeIsOneSafeStatement(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{
		Name: "flags",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{
			Column:     ast.NewColumn("fresh", "BOOLEAN").SetNotNull().SetDefault("true"),
			Changed:    ast.ColumnProperties{Default: true},
			HasChanged: true,
		}},
	}

	assessments, err := safety.AssessRendered([]ast.Node{node}, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Statement, qt.Equals, "-- ALTER statements: --\nALTER TABLE \"flags\" ALTER COLUMN \"fresh\" SET DEFAULT true")
	c.Assert(assessments[0].Severity, qt.Equals, safety.Safe)
}
