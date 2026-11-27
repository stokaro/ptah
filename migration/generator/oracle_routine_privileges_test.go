package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// An ordinary Oracle owner cannot read DBA_ROLES, so its schema read holds no
// grants. A body change and its rollback must keep the routine itself: CREATE
// OR REPLACE preserves privileges even when their grantees are not described.
// A readable grant option must not turn the same replacement into a refusal.
func TestPlanBidirectionalSchemaDiff_OracleRoutineReplacementKeepsPrivileges(t *testing.T) {
	for _, test := range []struct {
		name       string
		routine    string
		kind       string
		parameters string
		returns    string
		oldBody    string
		newBody    string
		coverage   coverage.Set
		grants     []catalog.Grant
		wantHeader string
	}{
		{
			name: "procedure with unread roles", routine: "rt", kind: schemamodel.FunctionKindProcedure,
			parameters: "answer OUT NUMBER", oldBody: "BEGIN answer := 1; END;", newBody: "BEGIN answer := 2; END;",
			coverage:   coverage.Set{}.With(coverage.Refused(coverage.Role)),
			wantHeader: "CREATE OR REPLACE PROCEDURE rt(answer OUT NUMBER) AUTHID DEFINER IS",
		},
		{
			name: "qualified procedure with unread roles", routine: "APP.rt", kind: schemamodel.FunctionKindProcedure,
			parameters: "answer OUT NUMBER", oldBody: "BEGIN answer := 1; END;", newBody: "BEGIN answer := 2; END;",
			coverage:   coverage.Set{}.With(coverage.Refused(coverage.Role)),
			wantHeader: "CREATE OR REPLACE PROCEDURE APP.rt(answer OUT NUMBER) AUTHID DEFINER IS",
		},
		{
			name: "function with unread roles", routine: "rt", kind: schemamodel.FunctionKindFunction,
			returns: "NUMBER", oldBody: "BEGIN RETURN 1; END;", newBody: "BEGIN RETURN 2; END;",
			coverage:   coverage.Set{}.With(coverage.Refused(coverage.Role)),
			wantHeader: "CREATE OR REPLACE FUNCTION rt RETURN NUMBER AUTHID DEFINER IS",
		},
		{
			name: "observed user and role privileges", routine: "rt", kind: schemamodel.FunctionKindProcedure,
			parameters: "answer OUT NUMBER", oldBody: "BEGIN answer := 1; END;", newBody: "BEGIN answer := 2; END;",
			grants: []catalog.Grant{
				{Role: "READER", ObjectType: "TABLE", ObjectName: "RT", Privilege: "EXECUTE"},
				{Role: "APP_USER", ObjectType: "TABLE", ObjectName: "RT", Privilege: "EXECUTE", WithOption: true},
			},
			wantHeader: "CREATE OR REPLACE PROCEDURE rt(answer OUT NUMBER) AUTHID DEFINER IS",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := &catalog.Database{
				NotDescribed: test.coverage, Grants: test.grants,
				Functions: []catalog.Function{{
					Schema: "APP", Name: "RT", Kind: test.kind, Language: "plsql", Security: "DEFINER",
					Volatility: "VOLATILE", Parameters: test.parameters, Returns: test.returns, Body: test.oldBody,
				}},
			}
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{
				Name: test.routine, Kind: test.kind, Language: "plsql", Security: "DEFINER",
				Volatility: "VOLATILE", Parameters: test.parameters, Returns: test.returns, Body: test.newBody,
			}}}
			semantics := identifier.ForDialect(platform.Oracle)
			semantics.DefaultSchema = "APP"
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: platform.Oracle, Schema: "APP", IdentifierSemantics: semantics}, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.FunctionsModified, qt.HasLen, 1)

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
				Dialect: platform.Oracle, Capabilities: capability.Oracle23(),
			})

			c.Assert(err, qt.IsNil)
			c.Assert(plan.Forward.Nodes, qt.HasLen, 1)
			c.Assert(plan.Reverse.Nodes, qt.HasLen, 1)
			up, err := builtin.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			down, err := builtin.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.Contains, test.wantHeader+"\n"+test.newBody+"\n")
			c.Assert(down, qt.Contains, test.wantHeader+"\n"+test.oldBody+"\n")
		})
	}
}
