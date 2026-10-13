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

// The rollback of a principal change replaces the routine again with the
// principal it ran as, and predicts the one the forward change leaves.
func TestExecuteAsService_ReversesTheChange(t *testing.T) {
	c := qt.New(t)
	subject := objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Function("dbo.f", "")

	reversals, err := mssqlreverse.ExecuteAsService{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: "sqlserver",
		Changes: []schemaext.ChangeRecord{{Subject: subject, Value: &mssqldiff.ExecuteAs{
			Before: &mssqlschema.ObservedExecuteAs{Principal: "auditor"}, After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}}}})

	c.Assert(err, qt.IsNil)
	c.Assert(reversals, qt.HasLen, 1)
	c.Assert(reversals[0].Change.Value, qt.DeepEquals, &mssqldiff.ExecuteAs{
		Before: &mssqlschema.ObservedExecuteAs{Principal: "reviewer"}, After: &mssqlschema.DesiredExecuteAs{Principal: "auditor"}})
	c.Assert(reversals[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement,
		Kind: mssqlschema.ExecuteAsKind, Value: &mssqlschema.ObservedExecuteAs{Principal: "reviewer"}}})
	c.Assert(reversals[0].Strategy, qt.Equals, "replace the routine with the principal it ran as")
}

// The service reverses principal changes of routines on SQL Server alone.
func TestExecuteAsService_FailurePath(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))
	change := &mssqldiff.ExecuteAs{After: &mssqlschema.DesiredExecuteAs{Principal: "reviewer"}}
	for _, test := range []struct {
		name    string
		target  string
		subject objectidentity.ID
		wantErr string
	}{
		{name: "another target", target: "postgres", subject: builder.Function("dbo.f", ""), wantErr: `.*SQL Server EXECUTE AS reversal on "postgres".*`},
		{name: "a table", target: "sqlserver", subject: builder.Table("dbo.t"), wantErr: `.*reversal requires an EXECUTE AS change on a routine.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			reversals, err := mssqlreverse.ExecuteAsService{}.ReverseChanges(c.Context(), schemaext.ReversalRequest{Target: test.target,
				Changes: []schemaext.ChangeRecord{{Subject: test.subject, Value: change}}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(reversals, qt.IsNil)
		})
	}
}
