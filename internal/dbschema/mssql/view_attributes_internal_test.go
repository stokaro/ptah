package mssql

// White-box testing required: viewAttributes is unexported and reads the half
// of a definition viewBody discards. Reaching it from outside the package would
// mean a live SQL Server for every shape a header can take.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestViewAttributes_ReadsTheHeadersWithClause pins what a view carries besides
// its body.
//
// SQL Server writes `CREATE VIEW name [(columns)] [WITH attribute [,...]] AS`.
// The attributes are part of what the view IS -- SCHEMABINDING binds it to the
// tables it names, so they cannot be altered under it, and an indexed view
// requires it -- and viewBody cuts the header off. Nothing kept them, so a
// replayed view came back unbound. Measured on SQL Server 2025:
//
//	v_bound  IsSchemaBound=1   ->   v_bound  IsSchemaBound=0
//
// with the replay reporting success and --dry-run reporting `Schema is synced`
// (stokaro/ptah#2125).
func TestViewAttributes_ReadsTheHeadersWithClause(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       []string
	}{
		{
			name:       "one attribute",
			definition: "CREATE VIEW dbo.v WITH SCHEMABINDING AS SELECT id FROM dbo.t",
			want:       []string{"SCHEMABINDING"},
		},
		{
			name:       "two attributes",
			definition: "CREATE VIEW dbo.v WITH SCHEMABINDING, VIEW_METADATA AS SELECT id FROM dbo.t",
			want:       []string{"SCHEMABINDING", "VIEW_METADATA"},
		},
		{
			// The catalog echoes the text its author wrote, so the case is
			// theirs. Reading it back uppercased is what lets a comparison
			// treat two spellings of one clause as one clause.
			name:       "lower case, as an author may have written it",
			definition: "create view dbo.v with schemabinding as select id from dbo.t",
			want:       []string{"SCHEMABINDING"},
		},
		{
			name:       "an attribute list with a declared column list before it",
			definition: "CREATE VIEW dbo.v (a, b) WITH SCHEMABINDING AS SELECT id, n FROM dbo.t",
			want:       []string{"SCHEMABINDING"},
		},
		{
			// THE control. A view with no WITH clause must come back with
			// nothing, or every plain view in every database grows an attribute
			// it never had.
			name:       "no attributes at all",
			definition: "CREATE VIEW dbo.v AS SELECT id FROM dbo.t",
			want:       nil,
		},
		{
			// A WITH inside the BODY is not the header's. Without the header
			// split this would read a common table expression as an attribute.
			name:       "a WITH that opens a CTE in the body",
			definition: "CREATE VIEW dbo.v AS WITH c AS (SELECT id FROM dbo.t) SELECT id FROM c",
			want:       nil,
		},
		{
			// A bracketed name may contain anything, including the keywords
			// this walk looks for.
			name:       "a bracketed name containing the keywords",
			definition: "CREATE VIEW [dbo].[v WITH AS] WITH SCHEMABINDING AS SELECT 1 AS one",
			want:       []string{"SCHEMABINDING"},
		},
		{
			// The comment rule viewBody already follows, on the other half of
			// the same header (stokaro/ptah#2115).
			name:       "a comment in the header holding the keyword",
			definition: "CREATE VIEW dbo.v /* WITH ENCRYPTION was considered */ WITH SCHEMABINDING AS SELECT 1 AS one",
			want:       []string{"SCHEMABINDING"},
		},
		{
			// Nothing to split on. Carrying no attributes is right: the reader
			// cannot say where the header ended.
			name:       "no header-level AS",
			definition: "SELECT id FROM dbo.t",
			want:       nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(viewAttributes(test.definition), qt.DeepEquals, test.want)
		})
	}
}

// TestViewAttributes_AndViewBodySplitAtTheSamePlace is why there is one scan
// rather than two.
//
// One takes what precedes the header's AS and the other what follows it. If
// they disagreed about where it is, the halves would overlap or drop text
// between them, and neither function's own table could show it.
func TestViewAttributes_AndViewBodySplitAtTheSamePlace(t *testing.T) {
	c := qt.New(t)
	definition := "CREATE VIEW dbo.v (a) WITH SCHEMABINDING, VIEW_METADATA AS SELECT id AS a FROM dbo.t"

	attributes := viewAttributes(definition)
	body := viewBody(definition)

	c.Assert(attributes, qt.DeepEquals, []string{"SCHEMABINDING", "VIEW_METADATA"})
	c.Assert(body, qt.Equals, "SELECT id AS a FROM dbo.t")
	// The body must not contain the header, and the header must not contain the
	// body: an `AS` inside the body is what makes that worth asserting.
	c.Assert(body, qt.Not(qt.Contains), "SCHEMABINDING")
}

// TestReadViews_CarriesTheAttributes is the half the helper table cannot reach:
// that readViews actually uses what viewAttributes returns.
//
// Written after the mutation that deletes the call from the reader survived
// every other assertion in this file -- each of them hands viewAttributes a
// definition directly, so none of them can tell whether anything calls it
// (stokaro/ptah#2125).
func TestReadViews_CarriesTheAttributes(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringABoundView)
	reader := NewSQLServerReader(db.SQL, "shop")

	views, err := reader.readViews()

	c.Assert(err, qt.IsNil)
	c.Assert(views, qt.HasLen, 2)
	c.Assert(views[0].Name, qt.Equals, "bound_orders")
	c.Assert(views[0].Attributes, qt.DeepEquals, []string{"SCHEMABINDING"})
	// The body still comes out whole, so a reader that took the attributes by
	// cutting the definition in the wrong place would fail here.
	c.Assert(views[0].Body, qt.Equals, "SELECT id, total FROM shop.orders")
	// And the plain view beside it carries none, which is what keeps this from
	// passing on a reader that attaches the same clause to everything.
	c.Assert(views[1].Name, qt.Equals, "plain_orders")
	c.Assert(views[1].Attributes, qt.IsNil)
}

// answeringABoundView is a server holding one schema-bound view and one plain
// one, each with the whole CREATE statement SQL Server keeps.
func answeringABoundView(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "FROM sys.views") {
		return dbtest.QueryResult{}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{"schema_name", "view_name", "definition"},
		Rows: [][]driver.Value{
			{
				"shop", "bound_orders",
				"CREATE VIEW shop.bound_orders WITH SCHEMABINDING AS SELECT id, total FROM shop.orders;",
			},
			{
				"shop", "plain_orders",
				"CREATE VIEW shop.plain_orders AS SELECT id FROM shop.orders;",
			},
		},
	}, nil
}
