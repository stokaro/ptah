package genexprprobe_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/genexprprobe"
)

type renderFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f renderFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func probeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "sizes", StructName: "Size"}, {Schema: "other", Name: "weights", StructName: "Weight"}},
		Fields: []schemamodel.Field{
			{StructName: "Size", Name: "size", Type: "NUMBER"},
			{StructName: "Size", Name: "doubled", Type: "NUMBER", GeneratedExpression: "size * 2"},
			{StructName: "Weight", Name: "weight", Type: "NUMBER"},
			{StructName: "Weight", Name: "doubled", Type: "NUMBER", GeneratedExpression: "weight * 2"},
		},
	}
}

func TestGeneratedExpressionProbesUseOneSelectedBatch(t *testing.T) {
	c := qt.New(t)
	declared := probeSchema()
	caps := capability.ForDialect("oracle")
	calls := 0
	service := renderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "oracle")
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Nodes, qt.HasLen, 2)
		result := renderer.Result{Complete: true}
		for index, node := range request.Nodes {
			table, ok := node.(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Name, qt.Equals, dbexprprobe.GeneratedExpressionProbeTable(index))
			c.Assert(table.Columns, qt.HasLen, 2)
			result.Fragments = append(result.Fragments, fmt.Sprintf("  selected probe %d; \n", index))
		}
		return result, nil
	})
	probes, err := genexprprobe.For(t.Context(), service, "oracle", caps, declared)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(probes, qt.HasLen, 2)
	c.Assert(probes[0].Create, qt.Equals, "selected probe 0")
	c.Assert(probes[1].Create, qt.Equals, "selected probe 1")
	c.Assert(probes[0].Table, qt.Equals, "sizes")
	c.Assert(probes[1].Schema, qt.Equals, "other")
	c.Assert(probes[1].Table, qt.Equals, "weights")
	c.Assert(probes[0].Generated, qt.DeepEquals, []string{"doubled"})
	c.Assert(probes[1].Generated, qt.DeepEquals, []string{"doubled"})
	c.Assert(declared, qt.DeepEquals, probeSchema())
}

type renderFailure struct {
	name   string
	result renderer.Result
	err    error
	want   error
	cancel bool
}

func (tc renderFailure) service(cancel context.CancelFunc) renderer.Service {
	return renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		if tc.cancel {
			cancel()
		}
		return tc.result, tc.err
	})
}

func TestGeneratedExpressionProbesRefuseIncompleteOrLossyRendering(t *testing.T) {
	failure := errors.New("provider disconnected")
	for _, test := range []renderFailure{
		{name: "service failure", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, err: failure, want: failure},
		{name: "missing completion", result: renderer.Result{Fragments: []string{"prefix", "second"}}, want: renderer.ErrInvalidResult},
		{name: "missing fragment", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, want: renderer.ErrInvalidResult},
		{name: "empty fragment", result: renderer.Result{Complete: true, Fragments: []string{"prefix", " ; \n"}}, want: renderer.ErrInvalidResult},
		{name: "omission", result: renderer.Result{Complete: true, Fragments: []string{"prefix", "second"},
			Omissions: []renderer.Omission{{Dialect: "oracle", Kind: "column", Reason: "unsupported", Property: "expression"}}}, want: ptaherr.ErrUnsupportedFeature},
		{name: "refusal", result: renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.InvalidSchema, Kind: "column", Message: "invalid expression",
		}, Input: new(1)}}}, want: ptaherr.ErrInvalidSchemaDiff},
		{name: "cancellation", result: renderer.Result{Complete: true, Fragments: []string{"prefix", "second"}}, cancel: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			probes, err := genexprprobe.For(ctx, test.service(cancel), "oracle", capability.ForDialect("oracle"), probeSchema())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(probes, qt.IsNil)
		})
	}
}

func TestGeneratedExpressionProbesRequireExplicitInputs(t *testing.T) {
	c := qt.New(t)
	probes, err := genexprprobe.For(t.Context(), nil, "oracle", nil, probeSchema())
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(probes, qt.IsNil)
	service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		c.Fatal("renderer must not run with a canceled context")
		return renderer.Result{}, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	probes, err = genexprprobe.For(ctx, service, "oracle", nil, probeSchema())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(probes, qt.IsNil)
}

func TestGeneratedExpressionProbesSkipUnneededRendering(t *testing.T) {
	for _, test := range []struct {
		name   string
		target string
		schema *schemamodel.Database
	}{
		{name: "target retains expressions", target: "postgres", schema: probeSchema()},
		{name: "no tables", target: "oracle", schema: &schemamodel.Database{}},
		{name: "no generated columns", target: "oracle", schema: &schemamodel.Database{Tables: []schemamodel.Table{{Name: "ordinary", StructName: "Ordinary"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
				calls++
				return renderer.Result{}, nil
			})
			probes, err := genexprprobe.For(t.Context(), service, test.target, nil, test.schema)
			c.Assert(err, qt.IsNil)
			c.Assert(probes, qt.IsNil)
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
