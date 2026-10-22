package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/planner"
)

type nativeUnit struct{ Value string }

func (*nativeUnit) Kind() schemaext.Kind                   { return "example.org/native/operation" }
func (p *nativeUnit) CloneExtension() ast.ExtensionPayload { return &nativeUnit{Value: p.Value} }

func TestPlannedUnitsPreserveNativeDataAndInputProvenance(t *testing.T) {
	c := qt.New(t)
	payload := &nativeUnit{Value: "native"}
	first := "OPAQUE 'unbalanced; owner syntax;"
	plan := planner.RenderedPlan{
		Request: renderer.Request{Nodes: []ast.Node{ast.NewRawSQL(first), &ast.ExtensionStatement{Payload: payload}, &ast.StatementList{}, ast.NewRawSQL("last;")}},
		Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: first}, {Native: payload}, {}, {SQL: "last;"}}},
	}
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units, qt.DeepEquals, []planner.PlannedUnit{{Fragment: renderer.Fragment{SQL: first}, Node: 0}, {Fragment: renderer.Fragment{Native: &nativeUnit{Value: "native"}}, Node: 1}, {Fragment: renderer.Fragment{SQL: "last;"}, Node: 3}})
	payload.Value = "changed"
	c.Assert(units[1].Fragment.Native.(*nativeUnit).Value, qt.Equals, "native")
	statements, err := plan.PlannedStatements()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(statements, qt.IsNil)
	sql, err := plan.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(sql, qt.Equals, "")
}

func TestPlannedUnitsRefuseMalformedSuffixWithoutPrefix(t *testing.T) {
	c := qt.New(t)
	plan := planner.RenderedPlan{Request: renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("first"), ast.NewRawSQL("last")}}, Result: renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: "SELECT 1;"}, {SQL: "SELECT 2;", Native: &nativeUnit{}}}}}
	units, err := plan.Units()
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(units, qt.IsNil)
}
