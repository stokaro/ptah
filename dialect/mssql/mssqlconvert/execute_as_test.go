package mssqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlconvert"
	"ptah.run/dialect/mssql/mssqlschema"
)

// A principal converts across representations unchanged, in both directions.
func TestService_ConvertsExecuteAs(t *testing.T) {
	for _, test := range []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "declared to observed", from: schemaext.Desired, to: schemaext.Observed,
			value: &mssqlschema.DesiredExecuteAs{Principal: "auditor"}, want: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}},
		{name: "observed to declared", from: schemaext.Observed, to: schemaext.Desired,
			value: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, want: &mssqlschema.DesiredExecuteAs{Principal: "auditor"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := mssqlconvert.Service{}.ConvertFeatures(c.Context(), schemaext.ConversionRequest{
				Target: "sqlserver", From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}
