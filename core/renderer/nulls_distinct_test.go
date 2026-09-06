package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// A model reaches a renderer from more sources than that dialect's own SQL --
// Go annotations, a YAML schema, a PostgreSQL database read and re-rendered.
// The parser refuses the clause when it reads SQL for a dialect that has no
// spelling for it; this file covers the other boundary, where the clause is
// already in the model.
//
// The refusal began as a MySQL-family one (stokaro/ptah#2788) and is now keyed
// on capability.UniqueNullsDistinctClause, because the dialects outside that
// family were not silent about the clause in one way. Measured 2026-09-03:
// MySQL 8.4.11 and MariaDB 11.8.9 answer error 1064; SQLite, SQL Server and
// Oracle have no such clause; ClickHouse and Spanner refuse the unique
// constraint before reaching it; and CockroachDB v26.3.1 answered `ERROR: at
// or near "nulls": syntax error` to Ptah's own output, which made it an
// unappliable migration rather than a silent drop. SQL Server is the reason
// both spellings are refused rather than only the one that inverts the
// default: its plain UNIQUE already treats nulls as equal, so there the pair
// runs the other way round. See stokaro/ptah#2820.

// refusingDialects are the targets whose default capability set cannot spell
// the clause. YugabyteDB is deliberately absent: its 2025.2 and 2026.1 lines
// accept, honor and read the clause back, and only the 2024.2 line refuses
// it, which is a release-line answer rather than a dialect one.
func refusingDialects() []string {
	return []string{
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.SQLServer,
		platform.Oracle,
		platform.ClickHouse,
		platform.Spanner,
		platform.CockroachDB,
	}
}

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

func TestRenderNullsDistinct_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		nullsDistinct bool
		wantClause    string
	}{
		{name: "not distinct", nullsDistinct: false, wantClause: "NULLS NOT DISTINCT"},
		{name: "distinct", nullsDistinct: true, wantClause: "NULLS DISTINCT"},
	}

	for _, dialect := range refusingDialects() {
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
// dialect: an ordinary unique index still renders on every engine that refuses
// the clause. Without it, deleting the feature outright would read as a fix.
func TestRenderUniqueWithoutNullsDistinct_HappyPath(t *testing.T) {
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

// PostgreSQL and YugabyteDB are the control that keeps the refusal scoped to
// the capability: the same model still renders the clause it carries on a
// target whose default set can spell it.
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

			yugabyteSQL, err := renderer.RenderSQL(platform.YugabyteDB, nullsDistinctTableNode(test.nullsDistinct))

			c.Assert(err, qt.IsNil)
			c.Assert(yugabyteSQL, qt.Contains, test.want)
		})
	}
}
