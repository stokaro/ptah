package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/migration/safety"
)

// rebuiltIndex is `ALTER TABLE users DROP INDEX k, ADD INDEX k (email)`, the
// one statement the MySQL family rebuilds an index with; unique makes the
// index added a unique one, and dropsUnique says the index dropped enforced a
// UNIQUE constraint.
func rebuiltIndex(unique, dropsUnique bool) ast.Node {
	index := ast.NewIndex("k", "users", "email")
	index.Unique = unique
	return &ast.AlterTableNode{Name: "users", Operations: []ast.AlterOperation{
		&ast.ReplaceIndexOperation{Index: index, DropsUniqueConstraint: dropsUnique},
	}}
}

// TestClassify_IndexRebuild judges the one-statement rebuild as the DROP INDEX
// and the CREATE INDEX it stands for are judged: a rebuild that turns a UNIQUE
// key into a plain index removes a uniqueness guarantee (stokaro/ptah#3853).
func TestClassify_IndexRebuild(t *testing.T) {
	tests := []struct {
		name         string
		node         ast.Node
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name: "a UNIQUE key rebuilt as a plain index", node: rebuiltIndex(false, true),
			wantSeverity: safety.Destructive,
			wantReason:   "the index rebuild removes the uniqueness a UNIQUE constraint enforces",
		},
		{
			name: "a UNIQUE key rebuilt unique", node: rebuiltIndex(true, true),
			wantSeverity: safety.Warning,
			wantReason:   "rebuilding a UNIQUE index can fail on existing duplicate values",
		},
		{
			name: "a plain index rebuilt", node: rebuiltIndex(false, false),
			wantSeverity: safety.Warning,
			wantReason:   "rebuilding an index can affect query plans and constraints",
		},
		{
			name: "an index hidden from the optimizer",
			node: &ast.AlterTableNode{Name: "users", Operations: []ast.AlterOperation{
				&ast.AlterIndexVisibilityOperation{IndexName: "k", Invisible: true},
			}},
			wantSeverity: safety.Warning,
			wantReason:   "ALTER INDEX changes which index the optimizer can use, and so query plans",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
		})
	}
}
