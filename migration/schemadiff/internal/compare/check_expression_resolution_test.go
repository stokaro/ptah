package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestConstraints_ACheckIsComparedThroughTheServerWhenOneAnswered pins which
// of the two rules decides a CHECK, and that the old one still stands where no
// server answered.
//
// PostgreSQL stores a parse tree rather than the text it was given. Measured on
// 17.11, a declared `score BETWEEN 1 AND 10` is stored as
// `((score >= 1) AND (score <= 10))` and `price >= 0` as
// `(price >= (0)::numeric)`, so a textual comparison planned a DROP and an ADD
// for a constraint nobody had changed -- on every run, at severity destructive
// (stokaro/ptah#2044).
func TestConstraints_ACheckIsComparedThroughTheServerWhenOneAnswered(t *testing.T) {
	const stored = "((score >= 1) AND (score <= 10))"
	tests := []struct {
		name       string
		declared   string
		checks     map[string]config.CheckExpression
		wantAdded  []string
		wantRemove []string
	}{
		{
			// The row the issue is about: the two texts share almost nothing
			// and are the same constraint.
			name:     "the server says the declaration is what the catalog holds",
			declared: "score BETWEEN 1 AND 10",
			checks: map[string]config.CheckExpression{
				"t.ck_score": {Expression: stored, Resolved: true},
			},
		},
		{
			// The control that keeps the fix from being a blanket silence.
			name:     "the server says it is something else",
			declared: "score BETWEEN 1 AND 20",
			checks: map[string]config.CheckExpression{
				"t.ck_score": {Expression: "((score >= 1) AND (score <= 20))", Resolved: true},
			},
			wantAdded:  []string{"ck_score"},
			wantRemove: []string{"ck_score"},
		},
		{
			// A refusal says nothing about whether the two agree, so it falls
			// back rather than being read as a difference. The old textual
			// rule then reports this pair, which is the behavior that shipped.
			name:     "the server refused the declaration",
			declared: "score BETWEEN 1 AND 10",
			checks: map[string]config.CheckExpression{
				"t.ck_score": {},
			},
			wantAdded:  []string{"ck_score"},
			wantRemove: []string{"ck_score"},
		},
		{
			// No connection at all: every offline comparison.
			name:       "nobody asked a server",
			declared:   "score BETWEEN 1 AND 10",
			wantAdded:  []string{"ck_score"},
			wantRemove: []string{"ck_score"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			declared := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "score", Type: "INTEGER"}},
				Constraints: []schemamodel.Constraint{{
					StructName: "T", Name: "ck_score", Table: "t",
					Type: "CHECK", CheckExpression: test.declared,
				}},
			}
			live := &catalog.Database{
				Tables: []catalog.Table{{Name: "t", Schema: "public"}},
				Constraints: []catalog.Constraint{{
					Name: "ck_score", TableName: "t", Schema: "public",
					Type: "CHECK", CheckClause: new(stored),
				}},
			}

			compare.ConstraintsWithSemantics(declared, live, diff,
				&config.CompareOptions{Dialect: platform.Postgres, CheckExpressions: test.checks},
				checkSemantics())

			c.Assert(diff.ConstraintsAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.ConstraintsRemoved, qt.DeepEquals, test.wantRemove)
		})
	}
}

// checkSemantics is the identifier rule a live PostgreSQL connection supplies.
func checkSemantics() identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	return semantics
}
