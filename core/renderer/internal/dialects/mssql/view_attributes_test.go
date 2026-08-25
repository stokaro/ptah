package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mssql"
)

// TestCreateView_CarriesTheViewsOwnWithClause pins where a view's attributes go.
//
// SQL Server's grammar is `CREATE VIEW name [(columns)] [WITH attribute [,...]]
// AS`, and the attributes belong to the view rather than to its body:
// SCHEMABINDING binds it to the tables it names, and an indexed view requires
// it. Nothing wrote them, so a replayed view came back unbound -- measured on
// SQL Server 2025, source against replay:
//
//	v_bound  IsSchemaBound=1   ->   v_bound  IsSchemaBound=0
//
// with the replay reporting success (stokaro/ptah#2125).
func TestCreateView_CarriesTheViewsOwnWithClause(t *testing.T) {
	tests := []struct {
		name       string
		attributes []string
		withCheck  bool
		want       string
	}{
		{
			name:       "one attribute",
			attributes: []string{"SCHEMABINDING"},
			want:       "CREATE VIEW [dbo].[v] WITH SCHEMABINDING AS",
		},
		{
			name:       "two attributes keep the server's order",
			attributes: []string{"SCHEMABINDING", "VIEW_METADATA"},
			want:       "CREATE VIEW [dbo].[v] WITH SCHEMABINDING, VIEW_METADATA AS",
		},
		{
			// The control. A view with no attributes must render exactly what
			// it rendered before, or every view in every document grows a
			// clause nobody asked for.
			name: "no attributes",
			want: "CREATE VIEW [dbo].[v] AS",
		},
		{
			// The two WITH clauses are different clauses in different places:
			// the attributes precede the body and CHECK OPTION follows it.
			// Rendering either where the other belongs is refused by the server.
			name:       "attributes and a check option are not the same clause",
			attributes: []string{"SCHEMABINDING"},
			withCheck:  true,
			want:       "CREATE VIEW [dbo].[v] WITH SCHEMABINDING AS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(&ast.CreateViewNode{
				Name:       "dbo.v",
				Body:       "SELECT id FROM dbo.t",
				Attributes: test.attributes,
				WithCheck:  test.withCheck,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestCreateView_ACheckOptionStillFollowsTheBody is the other half of the last
// row above: the clause that belongs after the body is still written there.
func TestCreateView_ACheckOptionStillFollowsTheBody(t *testing.T) {
	c := qt.New(t)

	sql, err := mssql.New().Render(&ast.CreateViewNode{
		Name:       "dbo.v",
		Body:       "SELECT id FROM dbo.t",
		Attributes: []string{"SCHEMABINDING"},
		WithCheck:  true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "SELECT id FROM dbo.t\nWITH CHECK OPTION")
}
