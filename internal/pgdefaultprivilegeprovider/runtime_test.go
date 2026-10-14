package pgdefaultprivilegeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegerender"
	"ptah.run/internal/pgdefaultprivilegeprovider"
)

func runtime(c *qt.C) *engine.Runtime {
	c.Helper()
	var targets []engine.Target
	for _, name := range pgdefaultprivilegeprovider.Targets() {
		targets = append(targets, engine.Target{Name: name})
	}
	return must.Must(engine.New(engine.Provider{ID: "example.org/targets", Targets: targets}, pgdefaultprivilegeprovider.Provider()))
}

// The runtime codecs, comparison, planning, reversal, and rendering must agree:
// ALL adds missing rights and removes an existing grant option in one plan, and
// rollback keeps the right and grant option that predate the migration.
func TestDefaultPrivileges_ComparePlanReverse(t *testing.T) {
	c := qt.New(t)
	services := runtime(c)
	semantics := identifier.ForDialect("postgres")
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, semantics))
	before := &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}
	request := schemaext.ObjectComparisonRequest{Target: "postgres", Identifiers: semantics, Capabilities: capability.Postgres17(),
		Desired: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}}})), Coverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))},
		Current: schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: before})), Coverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed))}}
	compared, err := services.CompareObjects(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(compared.Changes, qt.HasLen, 1)
	c.Assert(compared.Changes[0].Value.(*pgdefaultprivilege.Change).Before, qt.DeepEquals, before)
	before.Privileges[0].Privilege = "DELETE"
	c.Assert(compared.Changes[0].Value.(*pgdefaultprivilege.Change).Before.Privileges[0].Privilege, qt.Equals, "SELECT")
	planned, err := services.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres", Identifiers: semantics, Capabilities: capability.Postgres17(), Changes: compared.Changes})
	c.Assert(err, qt.IsNil)
	c.Assert(statements(c, planned), qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" REVOKE GRANT OPTION FOR SELECT ON TABLES FROM "reader";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" GRANT ALL ON TABLES TO "reader";`,
	})
	reversed, err := services.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Identifiers: semantics, Changes: compared.Changes})
	c.Assert(err, qt.IsNil)
	c.Assert(reversed, qt.HasLen, 1)
	inverse, err := services.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres", Identifiers: semantics, Capabilities: capability.Postgres17(), Changes: []schemaext.ChangeRecord{reversed[0].Change}})
	c.Assert(err, qt.IsNil)
	c.Assert(statements(c, inverse), qt.DeepEquals, []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" REVOKE ALL ON TABLES FROM "reader";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "owner" IN SCHEMA "app" GRANT SELECT ON TABLES TO "reader" WITH GRANT OPTION;`,
	})
}

func statements(c *qt.C, plan featureplan.Result) []string {
	c.Helper()
	handlers := must.Must(privilegerender.Registry())
	var result []string
	for _, contribution := range plan.Contributions {
		for _, step := range contribution.Steps {
			sql, err := handlers.Render(renderer.ExtensionContext{Target: "postgres"}, ast.StatementExtension, step.Payload.Payload)
			c.Assert(err, qt.IsNil)
			result = append(result, sql...)
		}
	}
	return result
}

func TestDefaultPrivileges_UnnamedACLClassDoesNotBlockNamedGrant(t *testing.T) {
	c := qt.New(t)
	services := runtime(c)
	semantics := identifier.ForDialect("cockroachdb")
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"}, semantics))
	class := must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Schema: "app", ObjectType: "TABLES"}, semantics))
	coverage := must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
		{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: class, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "FOR ALL ROLES has no named grantor"}},
	}))
	result, err := services.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "cockroachdb", Identifiers: semantics,
		Desired: schemaext.ObjectState{Coverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired)), Objects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}}))},
		Current: schemaext.ObjectState{Coverage: coverage},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 1)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Changes[0].Subject, qt.Equals, ref)
	c.Assert(result.Changes[0].Value.(*pgdefaultprivilege.Change).Granted, qt.DeepEquals, []pgdefaultprivilege.Grant{{Privilege: "SELECT"}})
}
