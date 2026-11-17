package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

const ownerReason = "OWNER TO gives the object to another role: the role that owned it loses an owner's privileges, " +
	"and a view then reads its tables with the new owner's privileges"

const restoreOwnerReason = "OWNER TO gives the view the plan creates again back to the role that owned it before the drop, " +
	"which the declaration does not name: the view reads its tables with that role's privileges, as it did before"

// TestAssess_RatesAnOwnerChange pins that giving a view to a role is a
// security-relevant change, rated as a REVOKE is: the role that owned it loses
// what an owner holds, and a view runs with its owner's privileges. A new view
// that declares an owner carries the same change, and one that declares none
// stays safe. A view a rebuild creates again and gives back to the role that
// owned it is rated alike, with a reason that says so: no role loses the view.
func TestAssess_RatesAnOwnerChange(t *testing.T) {
	owned := ast.NewCreateView("active_users").SetBody("SELECT 1")
	owned.Owner = "reporting"
	ownedMaterialized := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	ownedMaterialized.Owner = "reporting"
	restored := ast.NewCreateView("active_users").SetBody("SELECT 1")
	restored.Owner, restored.OwnerRestored = "reporting", true
	restoredMaterialized := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	restoredMaterialized.Owner, restoredMaterialized.OwnerRestored = "reporting", true
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
		{name: "a view given back to its owner", node: restored, want: verdict{safety.Warning, restoreOwnerReason}},
		{name: "a materialized view given back to its owner", node: restoredMaterialized, want: verdict{safety.Warning, restoreOwnerReason}},
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

// TestAssessRendered_RatesAnOwnerChange pins the rendered path: a view with an
// owner is two statements, and the second carries the warning. Its words are
// the same whether the view's owner changes or a rebuild gives the view back
// to the role that owned it, so the reason comes from the node that rendered
// it.
func TestAssessRendered_RatesAnOwnerChange(t *testing.T) {
	owned := ast.NewCreateView("active_users").SetBody("SELECT 1")
	owned.Owner = "reporting"
	ownedMaterialized := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	ownedMaterialized.Owner = "reporting"
	restored := ast.NewCreateView("active_users").SetBody("SELECT 1")
	restored.Owner, restored.OwnerRestored = "reporting", true
	restoredMaterialized := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	restoredMaterialized.Owner, restoredMaterialized.OwnerRestored = "reporting", true
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a view with a declared owner", node: owned, want: ownerReason},
		{name: "a materialized view with a declared owner", node: ownedMaterialized, want: ownerReason},
		{name: "a view given back to its owner", node: restored, want: restoreOwnerReason},
		{name: "a materialized view given back to its owner", node: restoredMaterialized, want: restoreOwnerReason},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessRendered(t.Context(), must.Must(builtin.New()), []ast.Node{test.node}, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(verdicts(assessments), qt.DeepEquals, []verdict{
				{safety.Safe, "does not remove data or tighten constraints"},
				{safety.Warning, test.want},
			})
		})
	}
}

// TestOwnerRestores_HappyPath pins which statements a view node that gives the
// view back to its owner rendered, read from the provenance alone: every
// statement of that node, and no statement of another node or of none.
func TestOwnerRestores_HappyPath(t *testing.T) {
	c := qt.New(t)
	declared := ast.NewCreateView("orders").SetBody("SELECT 1")
	declared.Owner = "reporting"
	restored := ast.NewCreateMaterializedView("daily").SetBody("SELECT 1")
	restored.Owner, restored.OwnerRestored = "reporting", true
	nodes := []ast.Node{ast.NewAlterOwner("VIEW", "orders", "reporting"), declared, restored}

	got, err := safety.OwnerRestores(nodes, []int{0, 1, 1, -1, 2, 2})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []bool{false, false, false, false, true, true})
}

// TestOwnerRestores_FailurePath pins that provenance outside the planned nodes
// is refused rather than read as no node.
func TestOwnerRestores_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := safety.OwnerRestores([]ast.Node{ast.NewCreateView("orders")}, []int{0, 1})

	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(got, qt.IsNil)
}

// TestRestoredOwnerReason pins that only the reason of a change of owner
// becomes the reason of an owner given back: the CREATE and the GRANTs a view
// node renders keep theirs.
func TestRestoredOwnerReason(t *testing.T) {
	tests := []struct {
		name, reason, want string
	}{
		{name: "a change of owner", reason: ownerReason, want: restoreOwnerReason},
		{name: "an owner given back", reason: restoreOwnerReason, want: restoreOwnerReason},
		{name: "another reason", reason: "does not remove data or tighten constraints", want: "does not remove data or tighten constraints"},
		{name: "no reason", reason: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(safety.RestoredOwnerReason(test.reason), qt.Equals, test.want)
		})
	}
}
