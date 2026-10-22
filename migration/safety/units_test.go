package safety_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

func TestAssessUnitsPreservesMixedOrderAndAccessEffects(t *testing.T) {
	c := qt.New(t)
	narrow := policy(narrowsRows).(*ast.ExtensionStatement).Payload
	widen := policy(widensRows).(*ast.ExtensionStatement).Payload
	units := []renderer.Fragment{{SQL: "SELECT 1;"}, {Native: narrow}, {SQL: "DROP TABLE old;"}, {Native: widen}}
	assessments, err := safety.AssessUnits(c.Context(), must.Must(builtin.New()), "sqlite", units, make([]safety.StatementAssessment, len(units)))
	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 4)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Safe)
	c.Assert(assessments[0].Statement, qt.Equals, "SELECT 1;")
	c.Assert(assessments[1].Index, qt.Equals, 2)
	c.Assert(assessments[1].Statement, qt.Equals, "")
	c.Assert(assessments[1].NativeKind, qt.Equals, narrow.Kind())
	c.Assert(assessments[1].Severity, qt.Equals, safety.Warning)
	c.Assert(assessments[1].Access, qt.Equals, schemaext.AccessNarrows)
	c.Assert(assessments[2].Index, qt.Equals, 3)
	c.Assert(assessments[2].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[3].Index, qt.Equals, 4)
	c.Assert(assessments[3].Statement, qt.Equals, "")
	c.Assert(assessments[3].NativeKind, qt.Equals, widen.Kind())
	c.Assert(assessments[3].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[3].Access, qt.Equals, schemaext.AccessWidens)
}

type unassessedNative struct{}

func (unassessedNative) Kind() schemaext.Kind                 { return "example.org/unassessed-operation" }
func (unassessedNative) CloneExtension() ast.ExtensionPayload { return unassessedNative{} }

func TestAssessUnitsKeepsUnknownNativeEffectDestructive(t *testing.T) {
	c := qt.New(t)
	assessments, err := safety.AssessUnits(c.Context(), must.Must(builtin.New()), "sqlite", []renderer.Fragment{{Native: unassessedNative{}}}, []safety.StatementAssessment{{}})
	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].NativeKind, qt.Equals, unassessedNative{}.Kind())
}

func TestAssessUnitsRetainsManagedDataSeverity(t *testing.T) {
	c := qt.New(t)
	assessments, err := safety.AssessUnits(c.Context(), must.Must(builtin.New()), "sqlite", []renderer.Fragment{{SQL: "DELETE FROM reference_rows;"}}, []safety.StatementAssessment{{NodeType: "managed-data", Severity: safety.Destructive, Reason: "removes declared rows"}})
	c.Assert(err, qt.IsNil)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Reason, qt.Equals, "removes declared rows")
}

func TestAssessUnitsRetainsOwnerRiskWithoutNodeType(t *testing.T) {
	c := qt.New(t)
	assessments, err := safety.AssessUnits(c.Context(), must.Must(builtin.New()), "sqlite", []renderer.Fragment{{SQL: "SELECT 1;"}}, []safety.StatementAssessment{{Severity: safety.Destructive, Reason: "owner risk", Access: schemaext.AccessWidens, AccessReason: "owner access"}})
	c.Assert(err, qt.IsNil)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Reason, qt.Equals, "owner risk")
	c.Assert(assessments[0].Access, qt.Equals, schemaext.AccessWidens)
	c.Assert(assessments[0].AccessReason, qt.Equals, "owner access")
}

func TestAssessUnitsRejectsInvalidSuffixWithoutPrefix(t *testing.T) {
	c := qt.New(t)
	assessments, err := safety.AssessUnits(c.Context(), must.Must(builtin.New()), "sqlite", []renderer.Fragment{{SQL: "SELECT 1;"}, {SQL: "SELECT 2;", Native: unassessedNative{}}}, make([]safety.StatementAssessment, 2))
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(assessments, qt.IsNil)
}

func TestAssessUnitsRejectsCancellationWithoutPrefix(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	assessments, err := safety.AssessUnits(ctx, must.Must(builtin.New()), "sqlite", []renderer.Fragment{{Native: unassessedNative{}}}, []safety.StatementAssessment{{}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(assessments, qt.IsNil)
}
