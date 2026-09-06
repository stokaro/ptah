package renderer_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// The expectations in this file are one live measurement per cell, taken
// 2026-08-30 and tabulated at constraintIncludeTargets in renderer.go. A row
// here restates a server's answer; when a row changes, the server is what
// settles it.

func TestUniqueConstraintIncludeSupportedDialects(t *testing.T) {
	dialects := []string{platform.Postgres, platform.YugabyteDB, platform.CockroachDB}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(uniqueIncludeSchema(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(
				strings.Join(statements, "\n"),
				qt.Contains,
				`CONSTRAINT "uq_accounts_email" UNIQUE ("email") INCLUDE ("display_name")`,
			)

			sql, err := renderer.RenderSQL(dialect, uniqueIncludeTableNode())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `CONSTRAINT "uq_accounts_email" UNIQUE ("email") INCLUDE ("display_name")`)
		})
	}
}

func TestPrimaryKeyIncludeSupportedDialects(t *testing.T) {
	dialects := []string{platform.Postgres, platform.YugabyteDB}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(primaryKeyIncludeSchema(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(
				strings.Join(statements, "\n"),
				qt.Contains,
				`CONSTRAINT "pk_accounts" PRIMARY KEY ("email") INCLUDE ("display_name")`,
			)
		})
	}
}

func TestUniqueConstraintIncludeUnsupportedDialectsFailClosed(t *testing.T) {
	dialects := []string{
		platform.Spanner,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLServer,
		platform.Oracle,
		platform.SQLite,
		platform.ClickHouse,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			want := fmt.Sprintf(
				`%s does not support INCLUDE columns on UNIQUE constraint "uq_accounts_email"; `+
					`target postgres, yugabytedb, or cockroachdb`,
				dialect,
			)

			statements, err := renderer.GetOrderedCreateStatements(uniqueIncludeSchema(), dialect)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, want)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, "constraint INCLUDE columns")

			sql, directErr := renderer.RenderSQL(dialect, uniqueIncludeTableNode())

			c.Assert(sql, qt.Equals, "")
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(directErr.Error(), qt.Equals, err.Error())
		})
	}
}

// CockroachDB is the row that makes this test worth having on its own. It takes
// INCLUDE on a UNIQUE constraint and refuses it on a primary key -- measured, not
// inferred -- so a single allow-set shared by both kinds would be wrong whichever
// way it were written.
func TestPrimaryKeyIncludeUnsupportedDialectsFailClosed(t *testing.T) {
	dialects := []string{
		platform.CockroachDB,
		platform.Spanner,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLServer,
		platform.Oracle,
		platform.SQLite,
		platform.ClickHouse,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			want := fmt.Sprintf(
				`%s does not support INCLUDE columns on PRIMARY KEY constraint "pk_accounts"; `+
					`target postgres or yugabytedb`,
				dialect,
			)

			statements, err := renderer.GetOrderedCreateStatements(primaryKeyIncludeSchema(), dialect)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, want)
		})
	}
}

func TestTablePrimaryKeyIncludeFailsClosed(t *testing.T) {
	c := qt.New(t)
	database := uniqueIncludeSchema()
	database.Constraints = nil
	database.Tables[0].PrimaryKey = []string{"email"}
	database.Tables[0].PrimaryKeyName = "pk_accounts"
	database.Tables[0].PrimaryKeyInclude = []string{"display_name"}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.MySQL)

	c.Assert(statements, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(
		err,
		qt.ErrorMatches,
		`mysql does not support INCLUDE columns on PRIMARY KEY constraint "pk_accounts"; `+
			`target postgres or yugabytedb`,
	)
}

// An unnamed primary key is reported by its table, because "" in the middle of a
// sentence names nothing and the reader still has to find the declaration.
//
// This is one of the two things the declared gate covers that the AST gate does
// not: a key with no name reaches prepareConstraintNode carrying "", so removing
// validateDeclaredConstraintIncludes leaves the refusal firing and the sentence
// naming nothing. Measured by deleting that call and watching this test, and
// only this one of the primary-key tests, redden.
func TestUnnamedTablePrimaryKeyIncludeIsReportedByItsTable(t *testing.T) {
	c := qt.New(t)
	database := uniqueIncludeSchema()
	database.Constraints = nil
	database.Tables[0].PrimaryKey = []string{"email"}
	database.Tables[0].PrimaryKeyInclude = []string{"display_name"}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.SQLite)

	c.Assert(statements, qt.IsNil)
	c.Assert(
		err,
		qt.ErrorMatches,
		`sqlite does not support INCLUDE columns on PRIMARY KEY constraint "accounts"; `+
			`target postgres or yugabytedb`,
	)
}

func TestConstraintIncludeEmptyColumnFailsClosed(t *testing.T) {
	c := qt.New(t)
	database := uniqueIncludeSchema()
	database.Constraints[0].IncludeColumns = []string{"display_name", " "}

	statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(statements, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`UNIQUE constraint "uq_accounts_email" has an empty INCLUDE column at position 2`,
	)
}

// Only a PRIMARY KEY and a UNIQUE constraint render an INCLUDE payload, so a
// payload on any other kind is dropped on every dialect, PostgreSQL included.
// That is a declaration error rather than a capability one, and it is refused as
// one.
//
// This is the other thing only the declared gate sees. FromConstraint builds a
// CHECK, FOREIGN KEY or EXCLUDE node without copying IncludeColumns at all, so
// the payload is gone before prepareConstraintNode is reached and the AST gate
// has nothing left to refuse.
func TestConstraintIncludeOnAKindThatCannotCarryItFailsClosed(t *testing.T) {
	tests := []struct {
		name           string
		constraintType string
	}{
		{name: "check", constraintType: "CHECK"},
		{name: "foreign key", constraintType: "FOREIGN KEY"},
		{name: "exclude", constraintType: "EXCLUDE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := uniqueIncludeSchema()
			database.Constraints[0].Type = test.constraintType

			statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(
				err,
				qt.ErrorMatches,
				fmt.Sprintf(
					`%s constraint "uq_accounts_email" carries INCLUDE columns; `+
						`only a PRIMARY KEY or UNIQUE constraint can`,
					test.constraintType,
				),
			)
		})
	}
}

// The ALTER TABLE route reaches prepareConstraintNode through a different
// operation, and a covering constraint added to an existing table is the shape
// `ptah migrations generate` emits. Refusing one and not the other would leave
// the drop reachable by the path migrations actually take.
func TestAddConstraintIncludeFailsClosed(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "accounts",
		Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: uniqueIncludeConstraintNode()},
		},
	}

	sql, err := renderer.RenderSQL(platform.MySQL, alter)

	c.Assert(sql, qt.Equals, "")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(
		err,
		qt.ErrorMatches,
		`mysql does not support INCLUDE columns on UNIQUE constraint "uq_accounts_email"; `+
			`target postgres, yugabytedb, or cockroachdb`,
	)
}

// A refusal must not leave half a statement in the buffer for the next caller to
// pick up, the same property TestIndexIncludeVisitorPathFailsClosedAndResetsOutput
// asserts for an index.
func TestConstraintIncludeVisitorPathFailsClosedAndResetsOutput(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer(platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(ast.NewIndex("idx_seed", "accounts", "email").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Not(qt.Equals), "")

	err = uniqueIncludeTableNode().Accept(r)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(r.Output(), qt.Equals, "")
}

// The control for every refusal above: the same schema without the payload
// renders on every dialect. Without it, a check that refused the whole
// constraint -- or the whole document -- would pass each failure-path test.
func TestConstraintWithoutIncludeRendersOnEveryDialect(t *testing.T) {
	dialects := []string{
		platform.Postgres,
		platform.YugabyteDB,
		platform.CockroachDB,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLServer,
		platform.Oracle,
		platform.SQLite,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			database := uniqueIncludeSchema()
			database.Constraints[0].IncludeColumns = nil

			statements, err := renderer.GetOrderedCreateStatements(database, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "uq_accounts_email")
			c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "INCLUDE")
		})
	}
}

func uniqueIncludeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "email", Type: "TEXT", Nullable: false},
			{StructName: "Account", Name: "display_name", Type: "TEXT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName:     "Account",
			Name:           "uq_accounts_email",
			Type:           "UNIQUE",
			Table:          "accounts",
			Columns:        []string{"email"},
			IncludeColumns: []string{"display_name"},
		}},
	}
}

func primaryKeyIncludeSchema() *schemamodel.Database {
	database := uniqueIncludeSchema()
	database.Constraints[0].Type = "PRIMARY KEY"
	database.Constraints[0].Name = "pk_accounts"
	return database
}

func uniqueIncludeConstraintNode() *ast.ConstraintNode {
	node := ast.NewUniqueConstraint("uq_accounts_email", "email")
	node.IncludeColumns = []string{"display_name"}
	return node
}

func uniqueIncludeTableNode() *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name: "accounts",
		Columns: []*ast.ColumnNode{
			{Name: "email", Type: "TEXT", Nullable: false},
			{Name: "display_name", Type: "TEXT", Nullable: true},
		},
		Constraints: []*ast.ConstraintNode{uniqueIncludeConstraintNode()},
	}
}
