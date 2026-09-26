package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/exprkey"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestConstraints_AnExcludeIsComparedThroughTheServerWhenOneAnswered pins which
// of the two rules decides an EXCLUDE, and that the texts still decide where no
// server answered.
//
// PostgreSQL stores an exclusion constraint parsed. Measured on 18.6, a
// declared `WHERE (n >= 0)` over a numeric column is printed back as
// `WHERE ((n >= (0)::numeric))`, which the reader cuts to `(n >= (0)::numeric)`,
// so a text comparison planned a DROP and an ADD for a constraint nobody had
// changed, on every run (stokaro/ptah#3767).
func TestConstraints_AnExcludeIsComparedThroughTheServerWhenOneAnswered(t *testing.T) {
	const storedWhere = "(n >= (0)::numeric)"
	tests := []struct {
		name       string
		where      string
		excludes   map[string]config.ExcludeExpression
		wantAdded  []string
		wantRemove []string
	}{
		{
			name:  "the server says the declaration is what the catalog holds",
			where: "n >= 0",
			excludes: map[string]config.ExcludeExpression{
				excludeKey("t", "t_r_excl"): {Elements: "r WITH =", Where: storedWhere, Resolved: true},
			},
		},
		{
			name:  "the server says the predicate is something else",
			where: "n >= 1",
			excludes: map[string]config.ExcludeExpression{
				excludeKey("t", "t_r_excl"): {Elements: "r WITH =", Where: "(n >= (1)::numeric)", Resolved: true},
			},
			wantAdded:  []string{"t_r_excl"},
			wantRemove: []string{"t_r_excl"},
		},
		{
			name:  "the server says the elements are something else",
			where: "n >= 0",
			excludes: map[string]config.ExcludeExpression{
				excludeKey("t", "t_r_excl"): {Elements: "r WITH <>", Where: storedWhere, Resolved: true},
			},
			wantAdded:  []string{"t_r_excl"},
			wantRemove: []string{"t_r_excl"},
		},
		{
			// A refusal says nothing about whether the two agree, so the texts
			// decide. They agree here, so an empty answer read as the server's
			// would plan a change the texts do not show.
			name:  "the server refused the declaration",
			where: storedWhere,
			excludes: map[string]config.ExcludeExpression{
				excludeKey("t", "t_r_excl"): {},
			},
		},
		{
			name:       "nobody asked a server",
			where:      "n >= 0",
			wantAdded:  []string{"t_r_excl"},
			wantRemove: []string{"t_r_excl"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ConstraintsWithSemantics(excludeDeclared(test.where), excludeLive(new(storedWhere)), diff,
				&config.CompareOptions{Dialect: platform.Postgres, ExcludeExpressions: test.excludes},
				checkSemantics())

			c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, test.wantAdded)
			c.Assert(diff.ConstraintsRemoved.Names(), qt.DeepEquals, test.wantRemove)
		})
	}
}

// TestConstraints_AnExcludeMethodIsComparedWithoutCase compares an EXCLUDE
// declared `USING GIST` with the catalog's `gist`. An access method is an
// identifier, folded to lower case like any other written unquoted, so the two
// are one method.
func TestConstraints_AnExcludeMethodIsComparedWithoutCase(t *testing.T) {
	c := qt.New(t)
	declared := excludeDeclared("")
	declared.Constraints[0].UsingMethod = "GIST"
	diff := &difftypes.SchemaDiff{}

	compare.ConstraintsWithSemantics(declared, excludeLive(nil), diff,
		&config.CompareOptions{Dialect: platform.Postgres}, checkSemantics())

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}

// excludeDeclared declares table t with EXCLUDE USING gist (r WITH =) and the
// WHERE clause given, none for an empty one.
func excludeDeclared(where string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "r", Type: "INTEGER"},
			{StructName: "T", Name: "n", Type: "NUMERIC"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Name: "t_r_excl", Table: "t", Type: "EXCLUDE",
			UsingMethod: "gist", ExcludeElements: "r WITH =", WhereCondition: where,
		}},
	}
}

// excludeLive is table t as the reader reports it, holding the constraint with
// the WHERE clause given, none for nil.
func excludeLive(where *string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "public"}},
		Constraints: []catalog.Constraint{{
			Name: "t_r_excl", TableName: "t", Schema: "public", Type: "EXCLUDE",
			UsingMethod: new("gist"), ExcludeElements: new("r WITH ="), WhereCondition: where,
		}},
	}
}

// excludeKey builds a [config.CompareOptions.ExcludeExpressions] key the way
// the resolver that fills that map does.
func excludeKey(table, constraint string) string {
	return exprkey.Exclude(checkSemantics(), table, constraint)
}
