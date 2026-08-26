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
			// Measured on SQL Server 2025. The word inside the comment is a
			// standalone word by every test standaloneWord applies, so a blind
			// scan read the body as `lives here */ AS SELECT id AS ident FROM
			// dbo.orders` and the replay was refused with `Incorrect syntax
			// near 'lives'` (stokaro/ptah#2115).
			name:       "a block comment in the header holding the word",
			definition: "CREATE VIEW dbo.v /* the header AS lives here */ AS SELECT id AS ident FROM dbo.orders",
			want:       "SELECT id AS ident FROM dbo.orders",
		},
		{
			name:       "a line comment in the header holding the word",
			definition: "CREATE VIEW dbo.v -- written AS a report\nAS SELECT id FROM dbo.t",
			want:       "SELECT id FROM dbo.t",
		},
		{
			// A nested block comment, which T-SQL has and most scanners do not:
			// the first */ closes the inner one only, so a scanner that stops
			// there resumes inside a comment and cuts at the next word it
			// recognizes.
			name:       "a nested block comment",
			definition: "CREATE VIEW dbo.v /* outer /* AS inner */ still AS comment */ AS SELECT 1 AS one",
			want:       "SELECT 1 AS one",
		},
		{
			// The bracket half. An unbalanced bracket inside a comment used to
			// leave the scan at a depth it never returned from, so the real AS
			// was skipped and the whole statement came back as the body.
			name:       "a comment holding an unbalanced bracket",
			definition: "CREATE VIEW dbo.v /* see [orders */ AS SELECT 1 AS one",
			want:       "SELECT 1 AS one",
		},
		{
			// The control for all four: a comment is not a place where the
			// header ends. A rule that skipped to the end of every comment
			// AND treated what follows as the body would pass the rows above
			// and break this one.
			name:       "a comment inside the body, which stays",
			definition: "CREATE VIEW dbo.v AS SELECT /* keep me */ id FROM dbo.t",
			want:       "SELECT /* keep me */ id FROM dbo.t",
		},
		{
			// The other control: an apostrophe inside a comment must not open
			// a string. Reading it as one swallows the rest of the header,
			// including the AS.
			name:       "a comment holding an apostrophe",
			definition: "CREATE VIEW dbo.v /* the buyer's name AS shown */ AS SELECT 1 AS one",
			want:       "SELECT 1 AS one",
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

	views, err := reader.readViews(t.Context())

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
