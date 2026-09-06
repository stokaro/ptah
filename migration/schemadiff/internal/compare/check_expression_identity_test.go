package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestConstraints_TwoTablesDifferingOnlyByCaseKeepTheirOwnCheck pins that a
// resolved CHECK belongs to the table it was resolved for.
//
// On PostgreSQL `t` and `T` are two tables -- the catalog reports both names
// verbatim -- and each may carry a constraint of the same name, because a
// constraint name is unique within its table and not across the schema. A key
// that folds the table with strings.ToLower spells one key for both, so the
// expression the server resolved for one table answers the lookup for the
// other, and a genuine difference on the second is reported as no change.
func TestConstraints_TwoTablesDifferingOnlyByCaseKeepTheirOwnCheck(t *testing.T) {
	c := qt.New(t)

	const storedLower = "((score >= 1) AND (score <= 10))"

	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Lower", Name: "t"},
			{StructName: "Upper", Name: "T"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Lower", Name: "score", Type: "INTEGER"},
			{StructName: "Upper", Name: "score", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Lower", Name: "ck_score", Table: "t",
				Type: "CHECK", CheckExpression: "score BETWEEN 1 AND 10",
			},
			{
				// Genuinely different from what the catalog holds for T, so
				// this pair has to be reported.
				StructName: "Upper", Name: "ck_score", Table: "T",
				Type: "CHECK", CheckExpression: "score BETWEEN 1 AND 20",
			},
		},
	}
	live := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "t", Schema: "public"},
			{Name: "T", Schema: "public"},
		},
		Constraints: []catalog.Constraint{
			{
				Name: "ck_score", TableName: "t", Schema: "public",
				Type: "CHECK", CheckClause: new(storedLower),
			},
			{
				Name: "ck_score", TableName: "T", Schema: "public",
				Type: "CHECK", CheckClause: new(storedLower),
			},
		},
	}

	// Only `t` was resolved, and it agrees with the catalog. Nothing was
	// resolved for `T`, whose declaration differs from what the catalog holds.
	checks := map[string]config.CheckExpression{
		checkKey("t", "ck_score"): {Expression: storedLower, Resolved: true},
	}

	diff := &difftypes.SchemaDiff{}
	compare.ConstraintsWithSemantics(declared, live, diff,
		&config.CompareOptions{Dialect: platform.Postgres, CheckExpressions: checks},
		checkSemantics())

	c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"ck_score"},
		qt.Commentf("T's declaration differs from its catalog CHECK and must be reported"))
	c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, []string{"ck_score"})
}
