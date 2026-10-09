package ydbreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreport"
	"ptah.run/dialect/ydb/ydbsecret"
)

// TestSecretReport_CountsEachSecretOnce counts a declared and an observed
// secret under one metric, and refuses a value of the other representation.
func TestSecretReport_CountsEachSecretOnce(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{name: "desired", representation: schemaext.Desired, value: &ydbsecret.Desired{ValueEnv: "PTAH_SECRET_PW"}},
		{name: "observed", representation: schemaext.Observed, value: &ydbsecret.Observed{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbreport.SecretService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []schemaext.ValueReport{{Kind: ydbsecret.Kind, Counts: []schemaext.MetricCount{{Name: "secrets", Value: 1}}}})
		})
	}
	c := qt.New(t)
	result, err := (ydbreport.SecretService{}).ReportValues(t.Context(), schemaext.ReportingRequest{Representation: schemaext.Observed,
		Values: []schemaext.Value{&ydbsecret.Observed{}, &ydbsecret.Desired{}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
