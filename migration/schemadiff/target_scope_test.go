package schemadiff_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestRegisteredCustomTargetScopesBothComparisonInputs(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}}}}))
	desired := scopedFunctionDeclaredFor("postgres")
	desired.Functions = append(desired.Functions, schemamodel.Function{Name: "local", Dialects: []string{" ALTERNATE "}})
	current := databaseHoldingTheScopedFunction()
	current.Functions = append(current.Functions, catalog.Function{Name: "undeclared"})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, "alternate", runtime))
	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.FunctionsAdded[0].Name, qt.Equals, "local")
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 1)
	c.Assert(diff.FunctionsRemoved[0].Name, qt.Equals, "undeclared")
	c.Assert(desired.Functions, qt.HasLen, 2)
	c.Assert(current.Functions, qt.HasLen, 2)
	refused, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "unregistered", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(refused, qt.IsNil)
}

func TestComparisonPreservesRegisteredCanonicalTargetName(t *testing.T) {
	c := qt.New(t)
	// A name belongs to this registration. Built-in aliases cannot redirect
	// dispatch to a target this runtime never selected.
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "pgx", Aliases: []string{"alternate"}}}}))
	diff, err := schemadiff.CompareWithDialect(t.Context(), scopedFunctionDeclaredFor("alternate"), &catalog.Database{}, "alternate", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
}

func TestTargetInfoComparisonPreservesScopedOmissionsBeforeValidation(t *testing.T) {
	c := qt.New(t)
	calls := 0
	runtime := selectedValidator{ComparisonRuntime: must.Must(builtin.New()), validate: func(_ context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(request.Target, qt.Equals, "sqlite")
		c.Assert(request.Schema.Functions, qt.HasLen, 0)
		return schemavalidation.Result{Complete: true}, nil
	}}
	current := databaseHoldingTheScopedFunction()
	current.Functions = append(current.Functions, catalog.Function{Name: "undeclared"})
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(t.Context(), scopedFunctionDeclaredFor("postgres"), current, catalog.ServerInfo{Dialect: "libsql+ws"}, nil, runtime))
	c.Assert(calls, qt.Equals, 1)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 1)
	c.Assert(diff.FunctionsRemoved[0].Name, qt.Equals, "undeclared")
}
