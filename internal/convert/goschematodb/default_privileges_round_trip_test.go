package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
)

// Conversion retains identity and per-right options through the selected
// owner. Source comments stay out of the catalog projection.
func TestToDBSchema_DefaultPrivilegesSurviveTheRoundTrip(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity pgdefaultprivilege.Identity
		declared pgdefaultprivilege.Desired
		want     pgdefaultprivilege.Observed
	}{
		{name: "one right", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
			declared: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Comment: "Future tables"},
			want:     pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{name: "mixed options", identity: pgdefaultprivilege.Identity{Grantor: "owner", Schema: "app", ObjectType: "TABLES", Grantee: "reader"},
			declared: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "INSERT", WithOption: true}, {Privilege: "UPDATE"}}},
			want:     pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT"}, {Privilege: "UPDATE"}}}},
		{name: "canonical keywords preserve identifier bytes", identity: pgdefaultprivilege.Identity{Grantor: " owner ", Schema: " app ", ObjectType: "tables", Grantee: " reader "},
			declared: pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "select"}, {Privilege: "SELECT", WithOption: true}}},
			want:     pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}}}},
		{name: "global revocation", identity: pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "PUBLIC"},
			declared: pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}}, want: pgdefaultprivilege.Observed{Revoked: []string{"EXECUTE"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := must.Must(pgdefaultprivilege.Ref(test.identity, identifier.ForDialect("postgres")))
			declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.DesiredObject(ref, test.declared)))), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))}
			read, err := goschematodb.ToDBSchema(t.Context(), declared, "postgres", builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(read.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{{Ref: ref, Value: &test.want}})
			c.Assert(read.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, ref).State, qt.Equals, schemaext.Complete)
			back, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, "postgres", builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(back.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{{Ref: ref, Value: &pgdefaultprivilege.Desired{Privileges: test.want.Privileges, Revoked: test.want.Revoked}}})
			c.Assert(back.FeatureCoverage.Lookup(pgdefaultprivilege.DefaultPrivilegeKind, ref).State, qt.Equals, schemaext.Complete)
		})
	}
}
