package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/devclean"
)

// keptState is a Supabase-shaped starting point as a read describes it.
func keptState() *catalog.Database {
	return &catalog.Database{
		Schemas:   []catalog.Schema{{Name: "auth"}},
		Tables:    []catalog.Table{{Schema: "auth", Name: "users"}, {Schema: "public", Name: "seed"}},
		Functions: []catalog.Function{{Schema: "auth", Name: "uid"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(keptDefaultPrivilege("auth"))),
		Roles:          []catalog.Role{{Name: "anon"}},
	}
}

// TestWithoutKeptStateLeavesWhatTheRunAdded pins the comparison side of a
// docker block's starting point (stokaro/ptah#4056). The starting point's
// objects leave the read, and what the run added stays wherever it is: a
// trigger on a starting-point table, a grant beside a starting-point grant, a
// table in public. An object the other side declares stays too, and an object
// read without a schema is matched in the default one.
func TestWithoutKeptStateLeavesWhatTheRunAdded(t *testing.T) {
	c := qt.New(t)
	current := keptState()
	current.Tables = []catalog.Table{
		{Schema: "auth", Name: "users"}, {Name: "seed"}, {Name: "profiles"},
	}
	current.Triggers = []catalog.Trigger{{Schema: "auth", Table: "users", Name: "on_signup"}}
	current.Grants = append(current.Grants,
		catalog.Grant{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"})
	current.Roles = append(current.Roles, catalog.Role{Name: "app"})
	declared := &catalog.Database{Functions: []catalog.Function{{Schema: "auth", Name: "uid"}}}

	got, err := devclean.WithoutKeptState(t.Context(), current, keptState(), declared, "public", must.Must(builtin.New()).Selections())
	c.Assert(err, qt.IsNil)

	c.Assert(got.Schemas, qt.HasLen, 0)
	c.Assert(got.Tables, qt.DeepEquals, []catalog.Table{{Name: "profiles"}})
	c.Assert(got.Functions, qt.DeepEquals, []catalog.Function{{Schema: "auth", Name: "uid"}})
	c.Assert(got.Triggers, qt.DeepEquals, []catalog.Trigger{{Schema: "auth", Table: "users", Name: "on_signup"}})
	c.Assert(got.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", Schema: "auth", ObjectName: "users"},
	})
	c.Assert(got.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(got.Roles, qt.DeepEquals, []catalog.Role{{Name: "app"}})
	c.Assert(current.Tables, qt.HasLen, 3)
}

// TestWithoutKeptStateWithoutAStartingPoint pins that a dev database with no
// starting point, which is every one a docker block did not provision, is
// compared as it was read.
func TestWithoutKeptStateWithoutAStartingPoint(t *testing.T) {
	c := qt.New(t)
	current := keptState()

	got, err := devclean.WithoutKeptState(t.Context(), current, nil, nil, "public", must.Must(builtin.New()).Selections())
	c.Assert(err, qt.IsNil)

	c.Assert(got, qt.Equals, current)
}

// TestBaselineWithoutStartingPointOfAClaimedURL pins that a baseline a claim
// did not record a starting point for, the zero one here, leaves the read as
// it is.
func TestBaselineWithoutStartingPointOfAClaimedURL(t *testing.T) {
	c := qt.New(t)
	current := keptState()

	got, err := devclean.Baseline{}.WithoutStartingPoint(t.Context(), current, nil, "public", must.Must(builtin.New()).Selections())
	c.Assert(err, qt.IsNil)

	c.Assert(got, qt.Equals, current)
}

// TestWithoutKeptStateMatchesFeatureObjectsInTheDefaultSchema pins that a
// named feature object is matched the way every other family is: one read
// without a schema is in the caller's default schema. The read's identity
// fills in PostgreSQL's own default, so a dev database whose schema is app held
// `daily` under public and a declaration of app.daily did not match it; the
// comparison then planned a CREATE of an object the replay already had.
func TestWithoutKeptStateMatchesFeatureObjectsInTheDefaultSchema(t *testing.T) {
	aggregate := func(schema, name string) schemaext.Object {
		return tsschema.ObservedContinuousAggregateObject(schema, name, tsschema.ObservedContinuousAggregate{Definition: "SELECT 1"})
	}
	tests := []struct {
		name     string
		declared []schemaext.Object
		want     []string
	}{
		{name: "declared under the default schema's name", declared: []schemaext.Object{aggregate("app", "daily")}, want: []string{"daily"}},
		{name: "declared without a schema", declared: []schemaext.Object{aggregate("", "daily")}, want: []string{"daily"}},
		{name: "declared in another schema", declared: []schemaext.Object{aggregate("public", "daily")}},
		{name: "not declared"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			env := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(aggregate("", "daily")))}
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(aggregate("", "daily")))}
			declared := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(test.declared...))}

			got, err := devclean.WithoutKeptState(t.Context(), current, env, declared, "app", must.Must(builtin.New()).Selections())
			c.Assert(err, qt.IsNil)

			var names []string
			for _, ref := range got.FeatureObjects.Refs() {
				names = append(names, tsschema.QualifiedName(ref))
			}
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

func keptDefaultPrivilege(schema string) schemaext.Object {
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{
		Grantor: "postgres", Schema: schema, ObjectType: "TABLES", Grantee: "reader",
	}, identifier.ForDialect("postgres")))
	return must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
	}))
}

// A global default and one in the replay's default schema remain distinct.
// Declaring one must neither retain nor subtract the other.
func TestWithoutKeptStatePreservesDefaultPrivilegeScopes(t *testing.T) {
	tests := []struct {
		name     string
		baseline string
		declared string
		want     []string
	}{
		{name: "global baseline", baseline: "", declared: "public", want: []string{"public", " public "}},
		{name: "schema baseline", baseline: "public", declared: "", want: []string{"", " public "}},
		{name: "declared global baseline", baseline: "", declared: "", want: []string{"", "public", " public "}},
		{name: "declared schema baseline", baseline: "public", declared: "public", want: []string{"", "public", " public "}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
				keptDefaultPrivilege(""), keptDefaultPrivilege("public"), keptDefaultPrivilege(" public "),
			))}
			env := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(keptDefaultPrivilege(test.baseline)))}
			declared := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(keptDefaultPrivilege(test.declared)))}

			got, err := devclean.WithoutKeptState(t.Context(), current, env, declared, "public", must.Must(builtin.New()).Selections())
			c.Assert(err, qt.IsNil)

			var want []objectidentity.ID
			for _, schema := range test.want {
				want = append(want, keptDefaultPrivilege(schema).Ref)
			}
			c.Assert(got.FeatureObjects.Refs(), qt.ContentEquals, want)
			c.Assert(current.FeatureObjects.Len(), qt.Equals, 3)
			c.Assert(env.FeatureObjects.Len(), qt.Equals, 1)
		})
	}
}

// An ACL identity groups several rights. A right added during replay remains
// visible even when the environment already holds another right in that ACL.
func TestWithoutKeptStateKeepsAddedDefaultPrivilegeRights(t *testing.T) {
	tests := []struct {
		name     string
		declared *pgdefaultprivilege.Observed
		want     []pgdefaultprivilege.Grant
	}{
		{name: "nil declaration", want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}}},
		{name: "new right", declared: &pgdefaultprivilege.Observed{}, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}}},
		{name: "explicit baseline right", declared: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}, {Privilege: "SELECT", WithOption: true}}},
		{name: "another declared right", declared: &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "UPDATE"}}}, want: []pgdefaultprivilege.Grant{{Privilege: "INSERT", WithOption: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := keptDefaultPrivilege("app").Ref
			currentObjects := must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT", WithOption: true}, {Privilege: "INSERT", WithOption: true}}}))))
			env := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(keptDefaultPrivilege("app")))}
			declared := declaredBaselineACL(ref, test.declared)
			current := &catalog.Database{FeatureObjects: currentObjects}
			got, err := devclean.WithoutKeptState(t.Context(), current, env, declared, "public", must.Must(builtin.New()).Selections())
			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(got.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{Privileges: test.want}))})
			c.Assert(must.Must(current.FeatureObjects.All()), qt.DeepEquals, must.Must(currentObjects.All()))
		})
	}
}

// declaredBaselineACL keeps a nil comparison side distinct from an empty ACL.
func declaredBaselineACL(ref objectidentity.ID, value *pgdefaultprivilege.Observed) *catalog.Database {
	if value == nil {
		return nil
	}
	return &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(ref, *value))))}
}
