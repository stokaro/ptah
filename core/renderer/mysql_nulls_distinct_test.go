package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// A model reaches the MySQL renderer from more sources than MySQL SQL -- Go
// annotations, a YAML schema, a PostgreSQL database read and re-rendered. The
// parser refuses the clause when it reads MySQL-family SQL; this file covers
// the other boundary, where the clause is already in the model. Measured
// 2026-09-03: MySQL 8.4.11 and MariaDB 11.8.9 both answer error 1064 to every
// spelling of it, so there is no output for these inputs -- rendering the
// constraint without the clause would invert its null-equality semantics.
// See stokaro/ptah#2788.

func nullsDistinctSchema(nullsDistinct bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "email", Type: "VARCHAR(255)"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName:    "Account",
			Name:          "uq_accounts_email",
			Type:          "UNIQUE",
			Table:         "accounts",
			Columns:       []string{"email"},
			NullsDistinct: &nullsDistinct,
		}},
	}
}

func nullsDistinctTableNode(nullsDistinct bool) *ast.CreateTableNode {
	constraint := ast.NewUniqueConstraint("uq_accounts_email", "email")
	constraint.NullsDistinct = &nullsDistinct
	return ast.NewCreateTable("accounts").
		AddColumn(ast.NewColumn("email", "VARCHAR(255)")).
		AddConstraint(constraint)
}

func nullsDistinctIndexNode(nullsDistinct bool) *ast.IndexNode {
	index := ast.NewIndex("uq_accounts_email", "accounts", "email").SetUnique()
	index.NullsDistinct = &nullsDistinct
	return index
}

func TestRenderNullsDistinct_MySQLFamilyFailurePath(t *testing.T) {
	tests := []struct {
		name          string
		nullsDistinct bool
		wantClause    string
	}{
		{name: "not distinct", nullsDistinct: false, wantClause: "NULLS NOT DISTINCT"},
		{name: "distinct", nullsDistinct: true, wantClause: "NULLS DISTINCT"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/whole model/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := renderer.GetOrderedCreateStatements(nullsDistinctSchema(test.nullsDistinct), dialect)

				c.Assert(statements, qt.IsNil)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err.Error(), qt.Contains, dialect+" does not support the "+test.wantClause+" clause")
				var capabilityErr *ptaherr.CapabilityError
				c.Assert(err, qt.ErrorAs, &capabilityErr)
				c.Assert(capabilityErr.Dialect, qt.Equals, dialect)
				c.Assert(capabilityErr.Feature, qt.Equals, "NULLS DISTINCT unique-constraint semantics")
			})

			t.Run(dialect+"/table constraint/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql, err := renderer.RenderSQL(dialect, nullsDistinctTableNode(test.nullsDistinct))

				c.Assert(sql, qt.Equals, "")
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err.Error(), qt.Contains, dialect+" does not support the "+test.wantClause+" clause")
			})

			t.Run(dialect+"/standalone index/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql, err := renderer.RenderSQL(dialect, nullsDistinctIndexNode(test.nullsDistinct))

				c.Assert(sql, qt.Equals, "")
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err.Error(), qt.Contains, dialect+" does not support the "+test.wantClause+" clause")
			})
		}
	}
}

// The control that keeps the refusal keyed on the clause rather than on the
// dialect: an ordinary unique constraint and an ordinary unique index still
// render on both engines.
func TestRenderUniqueWithoutNullsDistinct_MySQLFamilyHappyPath(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			table := nullsDistinctTableNode(false)
			table.Constraints[0].NullsDistinct = nil
			index := nullsDistinctIndexNode(false)
			index.NullsDistinct = nil

			tableSQL, err := renderer.RenderSQL(dialect, table)

			c.Assert(err, qt.IsNil)
			c.Assert(tableSQL, qt.Contains, "UNIQUE (`email`)")

			indexSQL, err := renderer.RenderSQL(dialect, index)

			c.Assert(err, qt.IsNil)
			c.Assert(indexSQL, qt.Contains, "CREATE UNIQUE INDEX")
		})
	}
}

// PostgreSQL is the control that keeps the refusal dialect-scoped: the same
// model still renders the clause it carries.
func TestRenderNullsDistinct_PostgresHappyPath(t *testing.T) {
	tests := []struct {
		name          string
		nullsDistinct bool
		want          string
	}{
		{name: "not distinct", nullsDistinct: false, want: `UNIQUE NULLS NOT DISTINCT ("email")`},
		{name: "distinct", nullsDistinct: true, want: `UNIQUE NULLS DISTINCT ("email")`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.Postgres, nullsDistinctTableNode(test.nullsDistinct))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
