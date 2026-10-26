package inputapproval_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/inputapproval"
)

func TestInputApprovalBindsExactTargetAndSQLWithIndependentCopies(t *testing.T) {
	c := qt.New(t)
	unit := renderer.Fragment{SQL: "CREATE OBJECT x;", Inputs: []renderer.InputReference{{Source: renderer.InputEnvironment, Name: "APPROVED_INPUT"}}}
	ctx, err := inputapproval.With(c.Context(), "fixture", []renderer.Fragment{unit})
	c.Assert(err, qt.IsNil)
	unit.Inputs[0].Name = "MUTATED"
	refs := inputapproval.References(ctx, "fixture", unit.SQL)
	c.Assert(refs[0].Name, qt.Equals, "APPROVED_INPUT")
	refs[0].Name = "MUTATED"
	c.Assert(inputapproval.References(ctx, "fixture", unit.SQL)[0].Name, qt.Equals, "APPROVED_INPUT")
	c.Assert(inputapproval.References(ctx, "other", unit.SQL), qt.IsNil)
	c.Assert(inputapproval.References(ctx, "fixture", unit.SQL+" "), qt.IsNil)
	_, err = inputapproval.With(ctx, "fixture", []renderer.Fragment{unit})
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
}

func TestInputApprovalRefusesCanceledCapture(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	prepared, err := inputapproval.With(ctx, "fixture", nil)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(prepared, qt.IsNil)
}
