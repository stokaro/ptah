package featureplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

func TestCommonStepCloneIsolatesColumnOperands(t *testing.T) {
	c := qt.New(t)
	original := featureplan.CommonStep{
		Effects: []plangraph.Effect{{Action: plangraph.Create}},
		AddedColumn: &ast.ColumnNode{Name: "account", Default: &ast.DefaultValue{Value: "0"},
			ForeignKey: &ast.ForeignKeyRef{Columns: []string{"id"}, OnDeleteColumns: []string{"account"}}},
	}
	snapshot := original.Clone()
	c.Assert(snapshot, qt.DeepEquals, original)
	snapshot.Effects[0].Action = plangraph.Drop
	snapshot.AddedColumn.Name = "changed"
	snapshot.AddedColumn.Default.Value = "1"
	snapshot.AddedColumn.ForeignKey.Columns[0] = "changed"
	snapshot.AddedColumn.ForeignKey.OnDeleteColumns[0] = "changed"
	c.Assert(original.Effects[0].Action, qt.Equals, plangraph.Create)
	c.Assert(original.AddedColumn.Name, qt.Equals, "account")
	c.Assert(original.AddedColumn.Default.Value, qt.Equals, "0")
	c.Assert(original.AddedColumn.ForeignKey.Columns, qt.DeepEquals, []string{"id"})
	c.Assert(original.AddedColumn.ForeignKey.OnDeleteColumns, qt.DeepEquals, []string{"account"})
}

func TestPlanningRefusalCannotRetainCommonRewriteClaims(t *testing.T) {
	c := qt.New(t)
	result := featureplan.Result{Complete: true, Rewrites: []plangraph.Rewrite{{}},
		Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: "example.org/storage", Message: "cannot combine operations",
		}}},
	}
	c.Assert(result.Err(featureplan.Request{}), qt.ErrorIs, schemaext.ErrInvalidValue)
}

func TestPhaseValid(t *testing.T) {
	tests := []struct {
		phase featureplan.Phase
		want  bool
	}{
		{phase: featureplan.PhaseDefault, want: true},
		{phase: featureplan.PhaseDependent, want: true},
		{phase: "late", want: false},
		{phase: "Dependent", want: false},
	}
	for _, test := range tests {
		t.Run(string(test.phase), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.phase.Valid(), qt.Equals, test.want)
		})
	}
}
