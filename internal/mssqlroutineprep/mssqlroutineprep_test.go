package mssqlroutineprep_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/mssqlroutineprep"
	"ptah.run/internal/routineargs"
)

func TestService_LowerDesired(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))
	before := routineargs.Subject(builder, "procedure", "dbo.p", "@a AS INT")
	after := routineargs.Subject(builder, "procedure", "dbo.p", "@a INT")
	original := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "dbo.p", Kind: "procedure", Parameters: "@a AS INT"}},
		FeatureCoverage: must.Must(mssqlschema.RoutineOptionsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: mssqlschema.RoutineOptionsKind, Subject: before, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not declared"}}})),
	}
	lowered, err := (mssqlroutineprep.Service{}).LowerDesired(t.Context(), schemapreparation.LoweringRequest{Target: "sqlserver", Desired: original})
	c.Assert(err, qt.IsNil)
	c.Assert(lowered.Functions[0].Parameters, qt.Equals, "@a INT")
	c.Assert(original.Functions[0].Parameters, qt.Equals, "@a AS INT")
	c.Assert(lowered.FeatureCoverage.Lookup(mssqlschema.RoutineOptionsKind, after).Reason, qt.Equals, "not declared")
	c.Assert(original.FeatureCoverage.Lookup(mssqlschema.RoutineOptionsKind, before).Reason, qt.Equals, "not declared")
	twice, err := (mssqlroutineprep.Service{}).LowerDesired(t.Context(), schemapreparation.LoweringRequest{Target: "sqlserver", Desired: lowered})
	c.Assert(err, qt.IsNil)
	c.Assert(twice.Functions, qt.DeepEquals, lowered.Functions)
	c.Assert(twice.FeatureCoverage.Equal(lowered.FeatureCoverage), qt.IsTrue)
}

func TestService_LowerDesired_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, target string
		ctx          context.Context
		desired      *schemamodel.Database
		want         string
	}{
		{name: "nil context", target: "sqlserver", desired: &schemamodel.Database{}, want: ".*requires a context and desired schema"},
		{name: "nil schema", target: "sqlserver", ctx: t.Context(), want: ".*requires a context and desired schema"},
		{name: "other target", target: "postgres", ctx: t.Context(), desired: &schemamodel.Database{}, want: `.*SQL Server routine lowering on "postgres"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := (mssqlroutineprep.Service{}).LowerDesired(test.ctx, schemapreparation.LoweringRequest{Target: test.target, Desired: test.desired})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestService_LowerDesired_Canceled(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := (mssqlroutineprep.Service{}).LowerDesired(ctx, schemapreparation.LoweringRequest{Target: "sqlserver", Desired: &schemamodel.Database{}})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got, qt.IsNil)
}
