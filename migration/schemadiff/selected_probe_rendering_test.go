package schemadiff_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/schemadiff"
)

type selectedProbeRuntime struct {
	schemadiff.TargetRuntime
	schemaext.NormalizationService
	service     renderer.Service
	validations int
	stop        error
}

func (s *selectedProbeRuntime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return s.service.Render(ctx, request)
}

func (s *selectedProbeRuntime) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	s.validations++
	return schemavalidation.Result{}, s.stop
}

type probeRenderFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f probeRenderFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func probeComparisonSchemas() (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Tables:    []schemamodel.Table{{Name: "items", StructName: "Item"}},
		Fields:    []schemamodel.Field{{StructName: "Item", Name: "id", Type: "INTEGER"}, {StructName: "Item", Name: "label", Type: "VARCHAR(8)"}},
		Functions: []schemamodel.Function{{Name: "fn", Parameters: "arg INTEGER", Returns: "INTEGER", Language: "sql", Body: "SELECT 1"}},
	}, &catalog.Database{
		Tables:    []catalog.Table{{Name: "items", Columns: []catalog.Column{{Name: "id", DataType: "int"}, {Name: "label", DataType: "text"}}}},
		Functions: []catalog.Function{{Name: "fn"}},
	}
}

func probeComparisonConnection(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	conn := must.Must(dbschema.ConnectToDatabase(t.Context(), atlasurl.SQLiteURLFromPath(t.TempDir()+"/probes.db")))
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func TestDatabaseComparisonSelectsContextualProbeBatches(t *testing.T) {
	c := qt.New(t)
	conn := probeComparisonConnection(t)
	desired, current := probeComparisonSchemas()
	var batches [][]ast.Node
	service := probeRenderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, conn.Info().Dialect)
		c.Assert(request.Capabilities, qt.DeepEquals, conn.Info().Capabilities)
		batches = append(batches, request.Nodes)
		result := renderer.Result{Complete: true}
		for index := range request.Nodes {
			result.Fragments = append(result.Fragments, fmt.Sprintf("SELECT %d;", index))
		}
		return result, nil
	})
	stop := errors.New("stop after probes")
	runtime := &selectedProbeRuntime{TargetRuntime: must.Must(builtin.New()), NormalizationService: must.Must(builtin.New()), service: service, stop: stop}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), conn, desired, current, nil, runtime)
	c.Assert(err, qt.ErrorIs, stop)
	c.Assert(diff, qt.IsNil)
	c.Assert(runtime.validations, qt.Equals, 1)
	c.Assert(batches, qt.HasLen, 2)
	c.Assert(batches[0], qt.HasLen, 3)
	c.Assert(batches[1], qt.HasLen, 2)
	table, ok := batches[0][0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Columns, qt.HasLen, 2)
	c.Assert(table.Name, qt.Equals, "pg_temp.ptah_column_probe_0")
	column, ok := batches[0][2].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(column.Columns, qt.HasLen, 1)
	c.Assert(column.Columns[0].Name, qt.Equals, "label")
	create, ok := batches[1][0].(*ast.CreateFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "pg_temp.ptah_routine_probe_0")
	drop, ok := batches[1][1].(*ast.DropFunctionNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, create.Name)
}

type probeFailureCase struct {
	name        string
	result      renderer.Result
	err         error
	want        error
	cancel      bool
	routineOnly bool
}

func (tc probeFailureCase) service(cancel context.CancelFunc) renderer.Service {
	return probeRenderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		if tc.cancel {
			cancel()
		}
		return tc.result, tc.err
	})
}

func (tc probeFailureCase) schemas() (*schemamodel.Database, *catalog.Database) {
	desired, current := probeComparisonSchemas()
	if tc.routineOnly {
		desired.Tables, desired.Fields, current.Tables = nil, nil, nil
	}
	return desired, current
}

func TestDatabaseComparisonDoesNotHideProbeServiceFailure(t *testing.T) {
	failure := errors.New("renderer disconnected")
	for _, test := range []probeFailureCase{
		{name: "provider", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, err: failure, want: failure},
		{name: "incomplete", want: renderer.ErrInvalidResult},
		{name: "missing fragments", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, want: renderer.ErrInvalidResult},
		{name: "unavailable capability", err: ptaherr.ErrUnsupportedFeature, want: ptaherr.ErrUnsupportedFeature},
		{name: "canceled", cancel: true, want: context.Canceled},
		{name: "routine provider", routineOnly: true, err: failure, want: failure},
		{name: "routine incomplete", routineOnly: true, want: renderer.ErrInvalidResult},
		{name: "routine unavailable capability", routineOnly: true, err: ptaherr.ErrUnsupportedFeature, want: ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := probeComparisonConnection(t)
			desired, current := test.schemas()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runtime := &selectedProbeRuntime{TargetRuntime: must.Must(builtin.New()), NormalizationService: must.Must(builtin.New()), service: test.service(cancel)}
			diff, err := schemadiff.CompareWithDatabase(ctx, conn, desired, current, nil, runtime)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(diff, qt.IsNil)
			c.Assert(runtime.validations, qt.Equals, 0)
		})
	}
}

type unresolvedProbeCase string

func (tc unresolvedProbeCase) render(_ context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	if tc == "refused" {
		result.Diagnostics = []renderer.Diagnostic{{Problem: schemavalidation.Diagnostic{
			Code: schemavalidation.UnsupportedFeature, Kind: "probe", Feature: "normalization", Message: "probe is unsupported",
		}, Input: new(0)}}
		return result, nil
	}
	for range request.Nodes {
		result.Fragments = append(result.Fragments, "SELECT 1;")
	}
	if tc == "omitted" {
		result.Omissions = []renderer.Omission{{Dialect: request.Target, Kind: "probe", Reason: "unsupported", Property: "normalization"}}
	} else {
		result.Fragments[len(result.Fragments)-1] = " \n"
	}
	return result, nil
}

func TestDatabaseComparisonLeavesCompletedUnusableProbesUnresolved(t *testing.T) {
	for _, test := range []unresolvedProbeCase{"refused", "omitted", "empty fragment"} {
		t.Run(string(test), func(t *testing.T) {
			c := qt.New(t)
			conn := probeComparisonConnection(t)
			desired, current := probeComparisonSchemas()
			stop := errors.New("stop after unresolved probes")
			calls := 0
			service := probeRenderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
				calls++
				return test.render(ctx, request)
			})
			runtime := &selectedProbeRuntime{TargetRuntime: must.Must(builtin.New()), NormalizationService: must.Must(builtin.New()), service: service, stop: stop}
			diff, err := schemadiff.CompareWithDatabase(t.Context(), conn, desired, current, nil, runtime)
			c.Assert(err, qt.ErrorIs, stop)
			c.Assert(diff, qt.IsNil)
			c.Assert(runtime.validations, qt.Equals, 1)
			c.Assert(calls, qt.Equals, 2)
		})
	}
}

func TestDatabaseComparisonDoesNotHideMissingSelectedRenderer(t *testing.T) {
	c := qt.New(t)
	conn := probeComparisonConnection(t)
	desired, current := probeComparisonSchemas()
	missing := must.Must(engine.New(engine.Provider{ID: "example.org/missing", Targets: []engine.Target{{Name: "sqlite"}}}))
	runtime := &selectedProbeRuntime{TargetRuntime: must.Must(builtin.New()), service: missing}
	diff, err := schemadiff.CompareWithDatabase(t.Context(), conn, desired, current, nil, runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(diff, qt.IsNil)
	c.Assert(runtime.validations, qt.Equals, 0)
}
