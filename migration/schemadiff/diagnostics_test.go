package schemadiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/migration/schemadiff"
)

func TestIncompleteComparisonErrorRetainsIndependentEvidence(t *testing.T) {
	c := qt.New(t)
	limits := schemadiff.Diagnostics{
		Common: []coverage.Object{{Kind: coverage.Role, Name: "reader", Reason: coverage.NotInspected}},
		Features: []schemaext.UndecidedChange{{Kind: "test/stream", Reason: "unobserved namespace",
			Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events"),
		}},
	}
	err := limits.Err()
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	var incomplete *schemadiff.IncompleteComparisonError
	c.Assert(err, qt.ErrorAs, &incomplete)
	c.Assert(incomplete.Diagnostics, qt.DeepEquals, limits)
	limits.Common[0].Name = "mutated"
	limits.Features[0].Reason = "mutated"
	c.Assert(incomplete.Diagnostics.Common[0].Name, qt.Equals, "reader")
	c.Assert(incomplete.Diagnostics.Features[0].Reason, qt.Equals, "unobserved namespace")
	c.Assert(err.Error(), qt.Contains, "unobserved namespace")
	c.Assert(err.Error(), qt.Contains, `table "events"`)
	c.Assert((schemadiff.Diagnostics{}).Err(), qt.IsNil)
}

func TestDiagnosticJSONPreservesQualifiedAndLiteralIdentities(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	limits := schemadiff.Diagnostics{Features: []schemaext.UndecidedChange{
		{Kind: "test/stream", Subject: builder.TableParts("", "audit.events"), Reason: "not inspected"},
		{Kind: "test/stream", Subject: builder.TableParts("audit", "events"), Reason: "not represented"},
	}}
	document, err := json.Marshal(limits)
	c.Assert(err, qt.IsNil)
	var restored schemadiff.Diagnostics
	c.Assert(json.Unmarshal(document, &restored), qt.IsNil)
	c.Assert(restored, qt.DeepEquals, limits)
	c.Assert(restored.Features[0].Subject.Key(), qt.Not(qt.Equals), restored.Features[1].Subject.Key())
	c.Assert(restored.Empty(), qt.IsFalse)
	c.Assert(restored.Count(), qt.Equals, 2)
}
