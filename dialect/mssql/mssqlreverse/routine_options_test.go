package mssqlreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlreverse"
	"ptah.run/dialect/mssql/mssqlschema"
)

// The rollback of a routine options change replaces the routine again with
// the options it held, and predicts the ones the forward change leaves.
func TestRoutineOptionsService_ReversesTheChange(t *testing.T) {
	c := qt.New(t)
	subject := objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Function("dbo.f", "")

	reversals, err := mssqlreverse.RoutineOptionsService{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: "sqlserver",
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &mssqldiff.RoutineOptions{
			Before: &mssqlschema.ObservedRoutineOptions{Principal: "auditor", Recompile: true},
			After:  &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reversals, qt.HasLen, 1)
	c.Assert(reversals[0].Change.Value, qt.DeepEquals, &mssqldiff.RoutineOptions{
		Before: &mssqlschema.ObservedRoutineOptions{Principal: "reviewer"},
		After:  &mssqlschema.DesiredRoutineOptions{Principal: "auditor", Recompile: true}})
	c.Assert(reversals[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement,
		Kind: mssqlschema.RoutineOptionsKind, Value: &mssqlschema.ObservedRoutineOptions{Principal: "reviewer"}}})
	c.Assert(reversals[0].Strategy, qt.Equals, "replace the routine with the options it held")
}

// The service reverses routine options changes of routines on SQL Server
// alone.
func TestRoutineOptionsService_FailurePath(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))
	change := &mssqldiff.RoutineOptions{After: &mssqlschema.DesiredRoutineOptions{Principal: "reviewer"}}
	for _, test := range []struct {
		name    string
		target  string
		subject objectidentity.ID
		wantErr string
	}{
		{name: "another target", target: "postgres", subject: builder.Function("dbo.f", ""), wantErr: `.*SQL Server routine options reversal on "postgres".*`},
		{name: "a table", target: "sqlserver", subject: builder.Table("dbo.t"), wantErr: `.*reversal requires a routine options change on a routine.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversals, err := mssqlreverse.RoutineOptionsService{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: test.target,
				Changes: []schemaext.ChangeRecord{{Subject: test.subject, Value: change}}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(reversals, qt.IsNil)
		})
	}
}
