package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// Rollback restores captured rights and grant options without changing the
// accepted forward diff. A desired ACL alone cannot supply the prior state.
func TestDefaultPrivilegesBidirectionalPlanRestoresCapturedACL(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	caps := capability.Postgres18()
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{
		Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
	}, identifier.ForDialect("postgres")))
	before := pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{
		{Privilege: "DELETE"}, {Privilege: "SELECT"}, {Privilege: "UPDATE", WithOption: true},
	}}
	current := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(ref, before)))),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed)),
	}
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{
			Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}, {Privilege: "SELECT", WithOption: true}, {Privilege: "UPDATE"}},
		})))),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired)),
	}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges[0].Value, qt.DeepEquals, &pgdefaultprivilege.Change{
		Before: &before, Granted: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}}, Revoked: []pgdefaultprivilege.Grant{{Privilege: "DELETE"}},
		GrantOptionsAdded: []string{"SELECT"}, GrantOptionsRevoked: []string{"UPDATE"},
	})
	captured, err := runtime.Codecs().SnapshotChanges(t.Context(), diff.FeatureChanges)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current, Dialect: "postgres", Capabilities: caps,
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities("postgres", caps, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	for _, statement := range []string{
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" GRANT INSERT ON TABLES TO "app_reader";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE DELETE ON TABLES FROM "app_reader";`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" GRANT SELECT ON TABLES TO "app_reader" WITH GRANT OPTION;`,
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE GRANT OPTION FOR UPDATE ON TABLES FROM "app_reader";`,
	} {
		c.Assert(forward, qt.Contains, statement)
	}
	reverse, err := builtin.RenderSQLWithCapabilities("postgres", caps, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(reverse, qt.Contains, `ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" REVOKE ALL ON TABLES FROM "app_reader";`+"\n"+
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" GRANT DELETE, SELECT ON TABLES TO "app_reader";`+"\n"+
		`ALTER DEFAULT PRIVILEGES FOR ROLE "app_owner" IN SCHEMA "app" GRANT UPDATE ON TABLES TO "app_reader" WITH GRANT OPTION;`+"\n")
	c.Assert(plan.Reverse.Diff.FeatureChanges, qt.HasLen, 1)
	c.Assert(plan.Reverse.Diff.FeatureChanges[0].Subject, qt.Equals, ref)
	restored := plan.Reverse.Diff.FeatureChanges[0].Value.(*pgdefaultprivilege.Change)
	c.Assert(restored.Restore, qt.DeepEquals, &before)
	c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
	restored.Restore.Privileges[0].Privilege = "TRUNCATE"
	c.Assert(diff.FeatureChanges, qt.DeepEquals, captured)
}
