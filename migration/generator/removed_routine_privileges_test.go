package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// A rollback restores observed privileges even when the declaration manages
// neither grantee. The procedure is restored before its grants, and a grant
// on another routine does not follow it.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineRestoresUnmanagedPrivileges(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		schema  string
		current catalog.Database
		want    []string
	}{
		{
			name: "MySQL", dialect: platform.MySQL, schema: "shop",
			current: catalog.Database{
				Functions: []catalog.Function{{Name: "rt", Kind: "procedure", Language: "sql", Body: "SELECT 1"}},
				RoutinePrivileges: catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
					{User: "reader", Host: "%", Schema: "shop", Name: "RT", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
					{User: "auditor", Host: "10.%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}, WithOption: true, ReaderMayGrant: true},
				}},
			},
			want: []string{
				"GRANT EXECUTE ON PROCEDURE `rt` TO `reader`@`%`;",
				"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `rt` TO `auditor`@`10.%` WITH GRANT OPTION;",
			},
		},
		{
			name: "MariaDB", dialect: platform.MariaDB, schema: "shop",
			current: catalog.Database{
				Functions: []catalog.Function{{Name: "rt", Kind: "procedure", Language: "sql", Body: "SELECT 1"}},
				RoutinePrivileges: catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
					{User: "reader", Role: true, Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}, ReaderMayGrant: true},
					{User: "auditor", Host: "%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}, WithOption: true, ReaderMayGrant: true},
				}},
			},
			want: []string{
				"GRANT EXECUTE ON PROCEDURE `rt` TO `reader`;",
				"GRANT EXECUTE ON PROCEDURE `rt` TO `auditor`@`%` WITH GRANT OPTION;",
			},
		},
		{
			name: "SQL Server", dialect: platform.SQLServer, schema: "dbo",
			current: catalog.Database{
				Functions: []catalog.Function{{Schema: "dbo", Name: "rt", Kind: "procedure", Language: "sql", Body: "BEGIN SELECT 1 END"}},
				Grants: []catalog.Grant{
					{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt"},
					{Role: "auditor", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt", WithOption: true},
					{Role: "denied", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "rt", IsPartialRevoke: true, Denied: true},
					{Role: "elsewhere", Privilege: "EXECUTE", ObjectType: "TABLE", Schema: "dbo", ObjectName: "other"},
				},
			},
			want: []string{
				"GRANT EXECUTE ON [dbo].[rt] TO [reader];",
				"GRANT EXECUTE ON [dbo].[rt] TO [auditor] WITH GRANT OPTION;",
				"DENY EXECUTE ON [dbo].[rt] TO [denied];",
			},
		},
		{
			name: "Oracle", dialect: platform.Oracle, schema: "APP",
			current: catalog.Database{
				Functions: []catalog.Function{{Name: "RT", Kind: "procedure", Language: "plsql", Body: "BEGIN NULL; END;"}},
				Grants: []catalog.Grant{
					{Role: "READER", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "RT"},
					{Role: "AUDITOR", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "RT", WithOption: true},
					{Role: "ELSEWHERE", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "OTHER"},
				},
			},
			want: []string{
				"GRANT EXECUTE ON RT TO READER;",
				"GRANT EXECUTE ON \"RT\" TO \"AUDITOR\" WITH GRANT OPTION;",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rollback := changedRoutineRollback(c, test.dialect, test.schema, &schemamodel.Database{}, &test.current)

			for _, want := range test.want {
				c.Assert(rollback, qt.Contains, want)
			}
			c.Assert(rollback, qt.Not(qt.Contains), "elsewhere")
			c.Assert(rollback, qt.Not(qt.Contains), "ELSEWHERE")
		})
	}
}

// A refused read cannot justify a down file that restores no privileges.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineRefusesUnreadPrivileges(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			current := &catalog.Database{
				Functions:         []catalog.Function{{Name: "rt", Kind: "procedure", Language: "sql", Body: "SELECT 1"}},
				RoutinePrivileges: catalog.RoutinePrivileges{Refused: true},
			}
			desired := &schemamodel.Database{}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: dialect, Schema: "shop"}, nil, runtime))

			plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
				Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
				Dialect: dialect, Capabilities: capability.ForDialect(dialect),
			})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*cannot plan the rollback of removing rt\(\): the database read could not read the routine privileges.*`)
			c.Assert(plan, qt.IsNil)
		})
	}
}

// A readable privilege snapshot is insufficient when the applying account
// cannot grant its rows again. The migration is refused before it is written.
func TestPlanBidirectionalSchemaDiff_RemovedRoutineRefusesUngrantablePrivileges(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	current := &catalog.Database{
		Functions: []catalog.Function{{Name: "rt", Kind: "procedure", Language: "sql", Body: "SELECT 1"}},
		RoutinePrivileges: catalog.RoutinePrivileges{Grants: []catalog.RoutineGrant{
			{User: "reader", Host: "%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
		}},
	}
	desired := &schemamodel.Database{}
	diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: platform.MySQL, Schema: "shop"}, nil, runtime))

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.MySQL, Capabilities: capability.ForDialect(platform.MySQL),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*cannot replace procedure rt: .*account may not grant it again.*`)
	c.Assert(plan, qt.IsNil)
}
