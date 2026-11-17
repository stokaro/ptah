package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

const ownerReason = "OWNER TO gives the object to another role: the role that owned it loses an owner's privileges, " +
	"and a view then reads its tables with the new owner's privileges"

// TestAssess_RatesAnOwnerChange pins that giving a view to a role is a
// security-relevant change, rated as a REVOKE is: the role that owned it loses
// what an owner holds, and a view runs with its owner's privileges. A new view
// that declares an owner carries the same change, and one that declares none
// stays safe.
func TestAssess_RatesAnOwnerChange(t *testing.T) {
	owned := ast.NewCreateView("active_users").SetBody("SELECT 1")
	owned.Owner = "reporting"
	ownedMaterialized := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	ownedMaterialized.Owner = "reporting"
	tests := []struct {
		name string
		node ast.Node
		want verdict
	}{
		{name: "ALTER VIEW OWNER TO", node: ast.NewAlterOwner("VIEW", "active_users", "reporting"), want: verdict{safety.Warning, ownerReason}},
		{name: "ALTER MATERIALIZED VIEW OWNER TO", node: ast.NewAlterOwner("MATERIALIZED VIEW", "daily", "reporting"),
			want: verdict{safety.Warning, ownerReason}},
		{name: "a new view with an owner", node: owned, want: verdict{safety.Warning, ownerReason}},
		{name: "a new materialized view with an owner", node: ownedMaterialized, want: verdict{safety.Warning, ownerReason}},
		{name: "a new view without one", node: ast.NewCreateView("active_users").SetBody("SELECT 1"),
			want: verdict{safety.Safe, "does not remove data or tighten constraints"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{test.node})

			c.Assert(verdicts(assessments), qt.DeepEquals, []verdict{test.want})
		})
	}
}

// TestAssessSQL_RatesAnOwnerChange pins the SQL-text path: an ALTER of any
// object to OWNER TO a role reads as the same warning, so a saved plan and a
// migration file rate it as the plan that made it did.
func TestAssessSQL_RatesAnOwnerChange(t *testing.T) {
	tests := []struct {
		statement string
		want      verdict
	}{
		{statement: `ALTER VIEW "active_users" OWNER TO "reporting"`, want: verdict{safety.Warning, ownerReason}},
		{statement: `alter materialized view "public"."daily" owner to "Report Owner"`, want: verdict{safety.Warning, ownerReason}},
		{statement: `ALTER TABLE "users" OWNER TO "app"`, want: verdict{safety.Warning, ownerReason}},
		{statement: `CREATE VIEW "owner_to" AS SELECT 1`, want: verdict{safety.Safe, "does not remove data or tighten constraints"}},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessSQL(t.Context(), runtime, "postgres", []string{test.statement})

			c.Assert(err, qt.IsNil)
			c.Assert(verdicts(assessments), qt.DeepEquals, []verdict{test.want})
		})
	}
}

// TestAssessRendered_RatesAnOwnerChange pins the rendered path: a new view
// with an owner is two statements, and the second carries the warning.
func TestAssessRendered_RatesAnOwnerChange(t *testing.T) {
	c := qt.New(t)
	owned := ast.NewCreateView("active_users").SetBody("SELECT 1")
	owned.Owner = "reporting"

	assessments, err := safety.AssessRendered(t.Context(), must.Must(builtin.New()), []ast.Node{owned}, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(verdicts(assessments), qt.DeepEquals, []verdict{
		{safety.Safe, "does not remove data or tighten constraints"},
		{safety.Warning, ownerReason},
	})
}
