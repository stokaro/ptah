package pgdefaultprivilegeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
)

func TestDefaultPrivileges_ReportsCapturedACLObjects(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		value          schemaext.Value
	}{
		{"declared mixed privileges", schemaext.Desired, &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "INSERT"}}, Revoked: []string{"DELETE"}}},
		{"observed revoked default", schemaext.Observed, &pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			report, err := runtime(c).ReportFeatures(t.Context(), schemaext.ReportingRequest{Target: "postgres", Representation: test.representation, Values: []schemaext.Value{test.value}})
			c.Assert(err, qt.IsNil)
			c.Assert(report.Values, qt.DeepEquals, []schemaext.ValueReport{{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Counts: []schemaext.MetricCount{{Name: "default_privileges", Value: 1}}}})
			c.Assert(report.Definitions, qt.HasLen, 1)
			c.Assert(report.Definitions[0].DisplayName, qt.Equals, "default privileges")
		})
	}
}
