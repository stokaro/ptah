package pgpolicyprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyreport"
)

// TestReportFeatures_CountsPoliciesAndSwitches pins the report of each model
// in each representation: a policy counts once, and a table's switches count
// where they are on.
func TestReportFeatures_CountsPoliciesAndSwitches(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		values         []schemaext.Value
		want           []schemaext.ValueReport
	}{
		{name: "declared", representation: schemaext.Desired,
			values: []schemaext.Value{&permissiveDeclared, &pgpolicy.DesiredTableState{Enabled: true}},
			want: []schemaext.ValueReport{
				{Kind: pgpolicy.PolicyKind, Counts: []schemaext.MetricCount{{Name: "row_security_policies", Value: 1}}},
				{Kind: pgpolicy.TableStateKind, Counts: []schemaext.MetricCount{{Name: "row_security_enabled_tables", Value: 1}, {Name: "row_security_forced_tables", Value: 0}}},
			}},
		{name: "observed", representation: schemaext.Observed,
			values: []schemaext.Value{&permissiveObserved, &pgpolicy.ObservedTableState{Forced: true}},
			want: []schemaext.ValueReport{
				{Kind: pgpolicy.PolicyKind, Counts: []schemaext.MetricCount{{Name: "row_security_policies", Value: 1}}},
				{Kind: pgpolicy.TableStateKind, Counts: []schemaext.MetricCount{{Name: "row_security_enabled_tables", Value: 0}, {Name: "row_security_forced_tables", Value: 1}}},
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, err := newRuntime(c).ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "postgres", Representation: test.representation, Values: test.values})

			c.Assert(err, qt.IsNil)
			c.Assert(report.Values, qt.DeepEquals, test.want)
			c.Assert(report.Definitions, qt.DeepEquals, policyreport.Definitions())
		})
	}
}

// TestReportFeatures_FailurePath pins what the service refuses when a host
// hands it a value directly: one of the other representation, and an invalid
// one.
func TestReportFeatures_FailurePath(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "an observed policy reported as declared", representation: schemaext.Desired, value: &permissiveObserved},
		{name: "a declared policy reported as observed", representation: schemaext.Observed, value: &permissiveDeclared},
		{name: "observed switches reported as declared", representation: schemaext.Desired, value: &pgpolicy.ObservedTableState{}},
		{name: "declared switches reported as observed", representation: schemaext.Observed, value: &pgpolicy.DesiredTableState{}},
		{name: "an invalid policy", representation: schemaext.Desired, value: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandInsert, Using: new("x")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reports, err := policyreport.Service{}.ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(reports, qt.IsNil)
		})
	}
}
