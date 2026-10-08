package renderer_test

import (
	"context"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemavalidation"
)

func TestRenderCompletedRefusalPreservesDataAndInputProvenance(t *testing.T) {
	c := qt.New(t)
	first, second := ast.NewRawSQL("first"), ast.NewRawSQL("second")
	diagnostics := []renderer.Diagnostic{{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.UnsupportedFeature, Kind: "table", Object: "items", Feature: "storage", Message: "storage is unavailable",
	}, Input: new(1)}}
	service := renderFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		request.Nodes[1] = nil
		return renderer.Result{Complete: true, Diagnostics: diagnostics}, nil
	})
	result, err := renderer.Render(t.Context(), service, renderer.Request{Target: "custom", Nodes: []ast.Node{first, second}})
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	var refused *renderer.BatchRefusalError
	c.Assert(err, qt.ErrorAs, &refused)
	var rendering *ptaherr.RenderError
	c.Assert(err, qt.ErrorAs, &rendering)
	c.Assert(rendering.Node, qt.Equals, second)
	var capability *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &capability)
	c.Assert(capability.Feature, qt.Equals, "storage")
	*diagnostics[0].Input = 0
	diagnostics[0].Problem.Message = "changed"
	c.Assert(*refused.Diagnostics[0].Input, qt.Equals, 1)
	c.Assert(refused.Error(), qt.Equals, "storage is unavailable")
}

func TestRenderRefusalCrossesADataBoundary(t *testing.T) {
	c := qt.New(t)
	response := renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: "column", Object: "items.id", Message: "invalid column",
	}, Input: new(0)}}}
	data, err := json.Marshal(response)
	c.Assert(err, qt.IsNil)
	var decoded renderer.Result
	c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)
	service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) { return decoded, nil })
	result, err := renderer.Render(t.Context(), service, renderer.Request{Target: "custom", Nodes: []ast.Node{ast.NewRawSQL("unused")}})
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, "invalid column")
}

func TestRenderRequiresCompletionForAnEmptyBatch(t *testing.T) {
	c := qt.New(t)
	service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) { return renderer.Result{}, nil })
	result, err := renderer.Render(t.Context(), service, renderer.Request{})
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	completed := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true}, nil
	})
	result, err = renderer.Render(t.Context(), completed, renderer.Request{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
}

func TestRenderRejectsMalformedRefusalReplies(t *testing.T) {
	problem := schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: "table", Message: "invalid table"}
	refusal := []renderer.Diagnostic{{Problem: problem}}
	for _, test := range []struct {
		name   string
		result renderer.Result
	}{
		{name: "no completion", result: renderer.Result{Diagnostics: refusal}},
		{name: "SQL prefix", result: renderer.Result{Complete: true, Diagnostics: refusal, Fragments: []string{"prefix"}}},
		{name: "empty fragment", result: renderer.Result{Complete: true, Diagnostics: refusal, Fragments: []string{""}}},
		{name: "omissions", result: renderer.Result{Complete: true, Diagnostics: refusal, Omissions: []renderer.Omission{{Dialect: "custom", Kind: "table", Reason: "unsupported"}}}},
		{name: "negative input", result: renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Problem: problem, Input: new(-1)}}}},
		{name: "absent input", result: renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Problem: problem, Input: new(1)}}}},
		{name: "invalid diagnostic", result: renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) { return test.result, nil })
			result, err := renderer.Render(t.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("unused")}})
			c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
		})
	}
}
