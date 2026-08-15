package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/parser"
)

// dropIndexShape is the whole of what a parsed DROP INDEX has to say. Comparing
// the struct rather than field by field is what makes a row fail when the parser
// records a name in the wrong field, which is the defect this file guards:
// asserting only on Name would pass with the schema silently misfiled on Table.
type dropIndexShape struct {
	Name         string
	Table        string
	IfExists     bool
	Concurrently bool
	Cascade      bool
}

func shapeOfDropIndex(node *ast.DropIndexNode) dropIndexShape {
	return dropIndexShape{
		Name:         node.Name,
		Table:        node.Table,
		IfExists:     node.IfExists,
		Concurrently: node.Concurrently,
		Cascade:      node.Cascade,
	}
}

// TestParser_ParseDropIndex pins that a DROP INDEX parses into an
// [ast.DropIndexNode] on every dialect Ptah renders one for, and that the schema
// qualifier ends up where [ast.DropIndexNode.Name] says it does
// (stokaro/ptah#1296).
//
// Before this, `parseDropStatement` refused every target but TABLE, so
// `DROP INDEX app.idx` came back as `unsupported DROP target: INDEX at position
// 5` and contributed no schema change to `migration/lint` in any schema -- the
// community binary v1.3.0 counts one for the reviewed schema.
//
// The two grammars are what the rows are really about. PostgreSQL and SQLite
// name the index in its own namespace and no table; MySQL, MariaDB, SQL Server
// and CockroachDB name the table and leave the index bare. A row that put
// `app` on Table for the first shape would read as "the index in schema app" to
// a renderer and as "the table named app" to the reviewed-schema filter, which
// treats an unqualified name as in-scope -- so it would put a drop in `app`
// under review from a run reviewing `public`.
func TestParser_ParseDropIndex(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    dropIndexShape
	}{
		{
			name:    "postgres schema-qualified index",
			dialect: platform.Postgres,
			sql:     "DROP INDEX app.idx;",
			want:    dropIndexShape{Name: "app.idx"},
		},
		{
			name:    "postgres unqualified index",
			dialect: platform.Postgres,
			sql:     "DROP INDEX idx;",
			want:    dropIndexShape{Name: "idx"},
		},
		{
			name:    "postgres concurrently",
			dialect: platform.Postgres,
			sql:     "DROP INDEX CONCURRENTLY idx;",
			want:    dropIndexShape{Name: "idx", Concurrently: true},
		},
		{
			name:    "postgres concurrently if exists qualified",
			dialect: platform.Postgres,
			sql:     "DROP INDEX CONCURRENTLY IF EXISTS public.idx;",
			want:    dropIndexShape{Name: "public.idx", IfExists: true, Concurrently: true},
		},
		{
			name:    "postgres cascade",
			dialect: platform.Postgres,
			sql:     "DROP INDEX app.idx CASCADE;",
			want:    dropIndexShape{Name: "app.idx", Cascade: true},
		},
		{
			name:    "postgres restrict is the default and records nothing",
			dialect: platform.Postgres,
			sql:     "DROP INDEX app.idx RESTRICT;",
			want:    dropIndexShape{Name: "app.idx"},
		},
		{
			name:    "postgres quoted identifiers",
			dialect: platform.Postgres,
			sql:     `DROP INDEX "App"."Idx";`,
			want:    dropIndexShape{Name: `"App"."Idx"`},
		},
		{
			name:    "lowercase keywords",
			dialect: platform.Postgres,
			sql:     "drop index concurrently if exists app.idx cascade;",
			want:    dropIndexShape{Name: "app.idx", IfExists: true, Concurrently: true, Cascade: true},
		},
		{
			name:    "sqlite schema-qualified index",
			dialect: platform.SQLite,
			sql:     "DROP INDEX IF EXISTS tenant.idx;",
			want:    dropIndexShape{Name: "tenant.idx", IfExists: true},
		},
		{
			name:    "mysql names the table",
			dialect: platform.MySQL,
			sql:     "DROP INDEX idx ON app.t;",
			want:    dropIndexShape{Name: "idx", Table: "app.t"},
		},
		{
			name:    "mariadb guards the drop and names the table",
			dialect: platform.MariaDB,
			sql:     "DROP INDEX IF EXISTS idx ON app.t;",
			want:    dropIndexShape{Name: "idx", Table: "app.t", IfExists: true},
		},
		{
			name:    "sqlserver bracketed identifiers",
			dialect: platform.SQLServer,
			sql:     "DROP INDEX [idx] ON [dbo].[t];",
			// Bracketed identifiers keep their brackets through the parser on
			// every statement; the renderers strip them. Recording them
			// unbracketed here would make this the one place they do not.
			want: dropIndexShape{Name: "[idx]", Table: "[dbo].[t]"},
		},
		{
			name:    "cockroachdb joins table and index with @",
			dialect: platform.CockroachDB,
			sql:     "DROP INDEX app.t@idx;",
			want:    dropIndexShape{Name: "idx", Table: "app.t"},
		},
		{
			name:    "cockroachdb if exists and cascade around the @ form",
			dialect: platform.CockroachDB,
			sql:     "DROP INDEX IF EXISTS app.t@idx CASCADE;",
			want:    dropIndexShape{Name: "idx", Table: "app.t", IfExists: true, Cascade: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			dropIndex, ok := statements.Statements[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(shapeOfDropIndex(dropIndex), qt.Equals, test.want)
		})
	}
}

// TestParser_ParseDropIndexWithoutDialect pins that the compatibility mode --
// no dialect selected, which is what `migration/lint` falls back to when a run
// names none -- parses the same statements. A grammar wired only under an
// explicit dialect would leave the change count zero for exactly the callers
// that have the least information.
func TestParser_ParseDropIndexWithoutDialect(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want dropIndexShape
	}{
		{
			name: "qualified index",
			sql:  "DROP INDEX app.idx;",
			want: dropIndexShape{Name: "app.idx"},
		},
		{
			name: "index on table",
			sql:  "DROP INDEX idx ON app.t;",
			want: dropIndexShape{Name: "idx", Table: "app.t"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			dropIndex, ok := statements.Statements[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(shapeOfDropIndex(dropIndex), qt.Equals, test.want)
		})
	}
}

// TestParser_ParseDropIndexRoundTrip pins that a parsed DROP INDEX renders back
// to the statement it came from.
//
// The qualified-index rows are the ones that need saying. With the schema on
// [ast.DropIndexNode.Name] and no table to take a namespace from, a renderer
// that escapes the name whole emits `DROP INDEX "app.idx";` -- one identifier,
// naming an index nobody created, and a statement that would fail against a
// database rather than drop the wrong thing. Both PostgreSQL and SQLite reach
// that shape, so both are here.
func TestParser_ParseDropIndexRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    string
	}{
		{
			name:    "postgres keeps the schema on the index name",
			dialect: platform.Postgres,
			sql:     "DROP INDEX app.idx;",
			want:    "DROP INDEX \"app\".\"idx\";\n",
		},
		{
			name:    "postgres unqualified index",
			dialect: platform.Postgres,
			sql:     "DROP INDEX idx;",
			want:    "DROP INDEX \"idx\";\n",
		},
		{
			name:    "postgres concurrently if exists cascade",
			dialect: platform.Postgres,
			sql:     "DROP INDEX CONCURRENTLY IF EXISTS app.idx CASCADE;",
			want:    "DROP INDEX CONCURRENTLY IF EXISTS \"app\".\"idx\" CASCADE;\n",
		},
		{
			name:    "sqlite keeps the schema on the index name",
			dialect: platform.SQLite,
			sql:     "DROP INDEX IF EXISTS tenant.idx;",
			want:    "DROP INDEX IF EXISTS \"tenant\".\"idx\";\n",
		},
		{
			name:    "mysql keeps the table",
			dialect: platform.MySQL,
			sql:     "DROP INDEX idx ON app.t;",
			want:    "DROP INDEX `idx` ON `app`.`t`;\n",
		},
		{
			name:    "cockroachdb keeps the @ form",
			dialect: platform.CockroachDB,
			sql:     "DROP INDEX app.t@idx;",
			want:    "DROP INDEX \"app\".\"t\"@\"idx\";\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)

			rendered, err := renderer.RenderSQL(test.dialect, statements.Statements[0])

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

// TestParser_ParseDropIndexRefusesShapesItCannotRecord pins that the forms
// [ast.DropIndexNode] cannot hold are refused out loud rather than truncated.
//
// A silent partial parse is the worse failure here: `DROP INDEX a, b` recorded
// as one node would report one schema change where two happened, and a reader
// judging a version by its change count would be told the smaller number with
// nothing to indicate it. An error costs the count entirely, which is the
// failure that is at least visible in the parse.
func TestParser_ParseDropIndexRefusesShapesItCannotRecord(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "several index names",
			sql:  "DROP INDEX a, b;",
			want: "unsupported DROP INDEX with multiple index names at position 12",
		},
		{
			name: "several index names after IF EXISTS",
			sql:  "DROP INDEX IF EXISTS a, b;",
			want: "unsupported DROP INDEX with multiple index names at position 22",
		},
		{
			name: "no index name",
			sql:  "DROP INDEX;",
			want: "expected index name: expected identifier, got Semicolon at position 10",
		},
		{
			name: "ON without a table",
			sql:  "DROP INDEX idx ON;",
			want: "expected table name: expected identifier, got Semicolon at position 17",
		},
		{
			name: "@ without an index name",
			sql:  "DROP INDEX t@;",
			want: "expected index name after '@': expected identifier, got Semicolon at position 13",
		},
		{
			name: "IF without EXISTS",
			sql:  "DROP INDEX IF idx;",
			want: "expected EXISTS after DROP INDEX IF: expected 'EXISTS', got 'idx' at position 14",
		},
		{
			name: "a DROP target that is still unmodeled",
			sql:  "DROP VIEW users;",
			want: "unsupported DROP target: VIEW at position 5",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.want)
		})
	}
}

// TestParser_ParseDropIndexRefusesSQLServerTableQualifiedName pins the one
// dialect where a qualified name with no ON means something else.
//
// SQL Server has no schema-qualified index names, so `DROP INDEX t.idx` is its
// backward-compatible spelling of `DROP INDEX idx ON t`. Reading `t` as a schema
// there -- which is what every other dialect's grammar means -- would measure
// the drop against a schema nobody wrote and render the name back as the single
// identifier "t.idx". Ptah never renders that spelling, so refusing it costs
// only the schema change count of a statement it does not emit.
//
// TestParser_ParseDropIndexAcceptsWhatTheSQLServerRefusalMustNotReach is the
// control that keeps these rows honest: it proves the refusal is scoped to the
// dialect rather than to the shape.
func TestParser_ParseDropIndexRefusesSQLServerTableQualifiedName(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "sqlserver two-part name without ON",
			sql:  "DROP INDEX t.idx;",
			want: `unsupported SQL Server DROP INDEX "t.idx" without ON: a qualified name there is table.index, not schema.index`,
		},
		{
			name: "sqlserver three-part name without ON",
			sql:  "DROP INDEX dbo.t.idx;",
			want: `unsupported SQL Server DROP INDEX "dbo.t.idx" without ON: a qualified name there is table.index, not schema.index`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(platform.SQLServer)).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, test.want)
		})
	}
}

// TestParser_ParseDropIndexAcceptsWhatTheSQLServerRefusalMustNotReach is the
// control for the refusal above. Both boundaries have to hold: an unqualified
// SQL Server name is still an ordinary drop, and the very text SQL Server
// refuses is a schema-qualified index everywhere else.
func TestParser_ParseDropIndexAcceptsWhatTheSQLServerRefusalMustNotReach(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{
			name:    "sqlserver bare name without ON is accepted",
			dialect: platform.SQLServer,
			sql:     "DROP INDEX idx;",
		},
		{
			name:    "postgres reads the same text as a schema-qualified index",
			dialect: platform.Postgres,
			sql:     "DROP INDEX t.idx;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
		})
	}
}
