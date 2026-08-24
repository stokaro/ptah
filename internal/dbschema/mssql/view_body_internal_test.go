package mssql

// White-box testing required: viewBody is unexported and the shapes that break
// a naive split -- a column alias spelled AS, a bracketed name containing AS --
// cannot be reached through the reader without a live server that has such a
// view.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestViewBody_TakesTheHeaderOff pins where a view's body begins.
//
// SQL Server stores the object's whole creation text, unlike PostgreSQL, whose
// pg_get_viewdef returns the body alone. Storing the statement where the body
// belongs made the renderer write it inside another `CREATE VIEW ... AS`, and
// the server refused the pair with `'CREATE VIEW' must be the first statement
// in a query batch` -- so a document describing any SQL Server view could not be
// applied (stokaro/ptah#2115).
func TestViewBody_TakesTheHeaderOff(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       string
	}{
		{
			name:       "the ordinary shape",
			definition: "CREATE VIEW shop.paid_orders AS SELECT id, total FROM shop.orders WHERE total > 0;",
			want:       "SELECT id, total FROM shop.orders WHERE total > 0",
		},
		{
			// THE case that separates the first AS from the last one. A view's
			// body is full of column aliases, and cutting at the last AS would
			// leave `total_amount` as the whole body.
			name:       "a body full of column aliases",
			definition: "CREATE VIEW v AS SELECT id AS order_id, total AS total_amount FROM orders",
			want:       "SELECT id AS order_id, total AS total_amount FROM orders",
		},
		{
			name:       "a bracketed name containing the word",
			definition: "CREATE VIEW [shop].[orders AS shipped] AS SELECT 1 AS one",
			want:       "SELECT 1 AS one",
		},
		{
			name:       "a declared column list",
			definition: "CREATE VIEW v (a, b) AS SELECT 1, 2",
			want:       "SELECT 1, 2",
		},
		{
			name:       "with schemabinding",
			definition: "CREATE VIEW dbo.v WITH SCHEMABINDING AS SELECT id FROM dbo.t",
			want:       "SELECT id FROM dbo.t",
		},
		{
			name:       "a word that merely starts with the letters",
			definition: "CREATE VIEW v AS SELECT asset FROM assets",
			want:       "SELECT asset FROM assets",
		},
		{
			// Nothing this reader can split, so it is carried whole rather than
			// cut at a guess.
			name:       "no header-level AS",
			definition: "SELECT id FROM orders",
			want:       "SELECT id FROM orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(viewBody(tt.definition), qt.Equals, tt.want)
		})
	}
}

// TestReadViews_CarriesTheBodyRatherThanTheStatement is the half the helper
// test cannot reach: that readViews actually calls it.
//
// Without this the mutation that deletes the call from the reader survives,
// because every other assertion here builds the definition by hand.
func TestReadViews_CarriesTheBodyRatherThanTheStatement(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringOneView)
	reader := NewSQLServerReader(db.SQL, "shop")

	views, err := reader.readViews()

	c.Assert(err, qt.IsNil)
	c.Assert(views, qt.HasLen, 1)
	c.Assert(views[0].Name, qt.Equals, "paid_orders")
	c.Assert(views[0].Body, qt.Equals, "SELECT id, total FROM shop.orders WHERE total > 0")
}

// answeringOneView is a server holding one view, whose OBJECT_DEFINITION is the
// whole CREATE statement the way SQL Server keeps it.
func answeringOneView(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "FROM sys.views") {
		return dbtest.QueryResult{}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{"schema_name", "view_name", "definition"},
		Rows: [][]driver.Value{{
			"shop", "paid_orders",
			"CREATE VIEW shop.paid_orders AS SELECT id, total FROM shop.orders WHERE total > 0;",
		}},
	}, nil
}
