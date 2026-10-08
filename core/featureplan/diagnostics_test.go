package featureplan_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

func TestPlanningRefusalCrossesADataBoundary(t *testing.T) {
	c := qt.New(t)
	request := featureplan.Request{Target: "custom", Changes: make([]schemaext.ChangeRecord, 2)}
	result := featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: "example.org/retention", Object: "events", Feature: "retained-rows", Message: "removed rows cannot be restored"},
		Change:  new(1),
	}}}
	data, err := json.Marshal(result)
	c.Assert(err, qt.IsNil)
	var decoded featureplan.Result
	c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, result)
	err = decoded.Err(request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	var refused *featureplan.RefusalError
	c.Assert(err, qt.ErrorAs, &refused)
	c.Assert(refused.Target(), qt.Equals, "custom")
	c.Assert(refused.Diagnostics(), qt.DeepEquals, result.Diagnostics)
	decoded.Diagnostics[0].Problem.Message = "changed response"
	*decoded.Diagnostics[0].Change = 0
	copyOfDiagnostics := refused.Diagnostics()
	*copyOfDiagnostics[0].Change = 0
	c.Assert(refused.Diagnostics()[0].Problem.Object, qt.Equals, "events")
	c.Assert(refused.Diagnostics()[0].Problem.Message, qt.Equals, "removed rows cannot be restored")
	c.Assert(*refused.Diagnostics()[0].Change, qt.Equals, 1)
}

func TestPlanningOutcomesRetainSchemaAndCompletionErrors(t *testing.T) {
	c := qt.New(t)
	request := featureplan.Request{Target: "custom"}
	c.Assert((featureplan.Result{}).Err(request), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert((featureplan.Result{Complete: true}).Err(request), qt.IsNil)
	result := featureplan.Result{Complete: true, Diagnostics: []featureplan.Diagnostic{{Problem: schemavalidation.Diagnostic{
		Code: schemavalidation.InvalidSchema, Kind: "table", Message: "contradictory captured state",
	}}}}
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
}
