package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// sqliteOptionsSource declares table events STRICT and WITHOUT ROWID as
// platform properties of the sqlite target.
const sqliteOptionsSource = `package entities

//ptah:schema:table name="events" platform.sqlite.strict="true" platform.sqlite.without_rowid="true"
type Event struct {
	//ptah:schema:field name="id" type="TEXT" primary="true"
	ID string
}
`

// TestSQLiteTableOptions_FollowTheTarget pins how the options are written:
// the SQLite owner reads its properties on the sqlite target and writes them
// after the column list, and another target leaves them out.
func TestSQLiteTableOptions_FollowTheTarget(t *testing.T) {
	database := must.Must(goschema.ParseSource(builtintest.Annotations(), "events.go", sqliteOptionsSource))
	tests := []struct {
		dialect string
		want    string
	}{
		{dialect: platform.SQLite, want: ") STRICT, WITHOUT ROWID;"},
		{dialect: platform.Postgres, want: ");"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestSQLiteTableOptions_ASQLDocumentKeepsThem reads STRICT and WITHOUT ROWID
// from a SQL document into the owner's platform properties and writes them
// back when the document is rendered for SQLite.
func TestSQLiteTableOptions_ASQLDocumentKeepsThem(t *testing.T) {
	c := qt.New(t)
	desired, _, err := sqlschema.Read([]byte("CREATE TABLE events (id TEXT PRIMARY KEY) STRICT, WITHOUT ROWID;"), platform.SQLite)
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&desired, platform.SQLite, capability.ForDialect(platform.SQLite))

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, ") STRICT, WITHOUT ROWID;")
}

// TestRenderSQL_WritesTheSQLiteOwnersTableOptions writes the options a CREATE
// TABLE node's facet declares, as SQLite spells them.
func TestRenderSQL_WritesTheSQLiteOwnersTableOptions(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("events").AddColumn(ast.NewColumn("id", "TEXT").SetPrimary())
	table.Facets = must.Must(schemaext.NewFacets(&sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true, WithoutRowID: true}}))

	rendered, err := builtin.RenderSQL(platform.SQLite, table)

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, ") STRICT, WITHOUT ROWID;")
}
