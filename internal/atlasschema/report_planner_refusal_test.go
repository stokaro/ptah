package atlasschema_test

import (
	"encoding/json"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/planrefusal"
	"ptah.run/migration/schemadiff"
)

// plannerRefusals are the refusals a planner raises, wrapped the way the plan
// computation wraps them, each with the refusal a report names for it.
func plannerRefusals() []struct {
	name string
	err  error
	want *atlasschema.Refusal
} {
	cause := fmt.Errorf("%w: what the plan would do", ptaherr.ErrUnsupportedFeature)
	options := []planrefusal.ViewOption{{View: "v", Option: "security_invoker"}}
	return []struct {
		name string
		err  error
		want *atlasschema.Refusal
	}{
		{
			name: "a view security option the schema leaves out",
			err:  fmt.Errorf("generate schema apply operations: %w", &planrefusal.LostViewSecurityError{Options: options, Err: cause}),
			want: &atlasschema.Refusal{Code: atlasschema.RefusalViewSecurityOption, ViewOptions: options},
		},
		{
			name: "an object something else depends on",
			err:  fmt.Errorf("generate schema apply operations: %w", &planrefusal.DependentObjectError{Err: cause}),
			want: &atlasschema.Refusal{Code: atlasschema.RefusalDependentObject},
		},
		{
			name: "a declaration the comparison refused",
			err:  fmt.Errorf("compare database schema: %w", &planrefusal.DeclarationError{Err: cause}),
			want: &atlasschema.Refusal{Code: atlasschema.RefusalDeclarationRefused},
		},
	}
}

// TestNewPlanReport_NamesAPlannerRefusal holds the plan document to the code of a
// refusal the planner raised, with the sentence the command prints beside it.
func TestNewPlanReport_NamesAPlannerRefusal(t *testing.T) {
	for _, test := range plannerRefusals() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: test.err})

			c.Assert(got, qt.DeepEquals, atlasschema.PlanReport{
				ContractVersion: atlasschema.PlanReportContractVersion,
				Command:         atlasschema.PlanReportCommand,
				Outcome:         atlasschema.PlanOutcomeRefused,
				Refusal:         test.want,
				Error:           &errorkind.Report{Message: test.err.Error()},
			})
		})
	}
}

// TestNewApplyReport_NamesAPlannerRefusal holds the apply document to the same
// code, and to none once the statements were sent, because a refusal promises
// that nothing reached the database.
func TestNewApplyReport_NamesAPlannerRefusal(t *testing.T) {
	for _, test := range plannerRefusals() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			refused := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Err: test.err})
			dispatched := atlasschema.NewApplyReport(atlasschema.ApplyEvidence{Dispatched: true, Err: test.err})

			c.Assert(refused.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(refused.Refusal, qt.DeepEquals, test.want)
			c.Assert(refused.Error, qt.DeepEquals, &errorkind.Report{Message: test.err.Error()})
			c.Assert(dispatched.Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
			c.Assert(dispatched.Refusal, qt.IsNil)
		})
	}
}

// TestNewPlanReport_ReadsTheTypeOfAPlannerRefusal is the control for the two
// above: the same sentinel and the same sentence without the type name no
// refusal, because a planner's refusal is told apart by its type and the
// sentinel also marks failures raised before the target was read.
func TestNewPlanReport_ReadsTheTypeOfAPlannerRefusal(t *testing.T) {
	c := qt.New(t)
	err := fmt.Errorf("generate schema apply operations: %w: what the plan would do", ptaherr.ErrUnsupportedFeature)

	got := atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: err})

	c.Assert(got.Outcome, qt.Equals, atlasschema.PlanOutcomeFailed)
	c.Assert(got.Refusal, qt.IsNil)
}

// TestNewPlanReport_ReadsTheDeclarationMarkNotTheRefusalsUnderIt is the control
// for the declaration refusal: a completed refusal of a schema, of either type,
// names no code until the plan computation marks it, because `schema diff` raises
// both over two documents with no target at all.
func TestNewPlanReport_ReadsTheDeclarationMarkNotTheRefusalsUnderIt(t *testing.T) {
	validation := schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{
		{Code: schemavalidation.UnsupportedFeature, Kind: "schema", Feature: "f", Message: "the target has no f"},
	}}.Err("custom")
	tests := []struct {
		name string
		err  error
	}{
		{name: "the selected validator's refusal", err: fmt.Errorf("render: %w", validation)},
		{name: "the comparison's own refusal", err: fmt.Errorf("compare: %w", &schemadiff.RefusalError{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.NewPlanReport(atlasschema.PlanEvidence{Err: test.err})

			c.Assert(got.Outcome, qt.Equals, atlasschema.PlanOutcomeFailed)
			c.Assert(got.Refusal, qt.IsNil)
		})
	}
}

// TestRefusal_ViewOptionsWireNames pins the field names a consumer parses, as
// TestReports_WireNames does for the fields of the other codes.
func TestRefusal_ViewOptionsWireNames(t *testing.T) {
	c := qt.New(t)

	got, err := json.Marshal(atlasschema.Refusal{
		Code:        atlasschema.RefusalViewSecurityOption,
		ViewOptions: []planrefusal.ViewOption{{View: "v", Option: "security_invoker"}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(string(got), qt.Equals,
		`{"code":"view-security-option","view_options":[{"view":"v","option":"security_invoker"}]}`)
}
