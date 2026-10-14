package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/atlasfilter"
)

// The same declarations populate both comparison sides. No common ACL field
// participates, so the tests exercise owner registration and the public scope.
func defaultPrivilegeFixture(representation schemaext.Representation) schemaext.Objects {
	identities := []pgdefaultprivilege.Identity{
		{Grantor: "defaults_owner", Schema: "public", ObjectType: "TABLES", Grantee: "defaults_reader"},
		{Grantor: "defaults_owner", Schema: "app", ObjectType: "SEQUENCES", Grantee: "defaults_reader"},
		{Grantor: "other_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "other_reader"},
	}
	var objects []schemaext.Object
	for i, identity := range identities {
		objects = append(objects, defaultPrivilegeObject(representation, identity, []string{"SELECT", "USAGE", "EXECUTE"}[i]))
	}
	return must.Must(schemaext.NewObjects(objects...))
}

func defaultPrivilegeObject(representation schemaext.Representation, identity pgdefaultprivilege.Identity, privilege string) schemaext.Object {
	ref := must.Must(pgdefaultprivilege.Ref(identity, identifier.ForDialect("postgres")))
	grants := []pgdefaultprivilege.Grant{{Privilege: privilege}}
	if representation == schemaext.Observed {
		return schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Observed{Privileges: grants}}
	}
	return schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Desired{Privileges: grants}}
}

func defaultPrivilegeDatabaseFixture() *catalog.Database {
	return &catalog.Database{
		Schemas:         []catalog.Schema{{Name: "public"}, {Name: "app"}},
		Tables:          []catalog.Table{{Name: "users", Columns: []catalog.Column{{Name: "id"}}}},
		Roles:           []catalog.Role{{Name: "defaults_owner"}, {Name: "defaults_reader"}, {Name: "other_owner"}, {Name: "other_reader"}},
		FeatureObjects:  defaultPrivilegeFixture(schemaext.Observed),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed)),
	}
}

func defaultPrivilegeGeneratedFixture() *schemamodel.Database {
	return &schemamodel.Database{
		Schemas:         []schemamodel.Schema{{Name: "public"}, {Name: "app"}},
		Tables:          []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields:          []schemamodel.Field{{StructName: "User", Name: "id", Type: "BIGINT"}},
		Roles:           []schemamodel.Role{{Name: "defaults_owner"}, {Name: "defaults_reader"}, {Name: "other_owner"}, {Name: "other_reader"}},
		FeatureObjects:  defaultPrivilegeFixture(schemaext.Desired),
		FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired)),
	}
}

func defaultPrivilegeTypes(objects schemaext.Objects) []string {
	types := make([]string, 0, objects.Len())
	for _, ref := range objects.Refs() {
		types = append(types, must.Must(pgdefaultprivilege.IdentityOf(ref)).ObjectType)
	}
	return types
}

// An asymmetric filter creates an unintended grant or revoke. The control also
// proves an unrelated exclusion keeps the owner objects on both sides.
func TestExcludeSubtractsDefaultPrivilegesOnBothSidesTheSameWay(t *testing.T) {
	tests := []struct {
		name           string
		patterns, want []string
		unmatched      []string
	}{
		{name: "unrelated selector", patterns: []string{"nosuch_object"}, want: []string{"TABLES", "SEQUENCES", "FUNCTIONS"}, unmatched: []string{"nosuch_object"}},
		{name: "schema", patterns: []string{"app"}, want: []string{"TABLES"}},
		{name: "grantee", patterns: []string{"defaults_reader[type=role]"}, want: []string{"FUNCTIONS"}},
		{name: "grantor", patterns: []string{"defaults_owner[type=role]"}, want: []string{"FUNCTIONS"}},
		{name: "both roles match", patterns: []string{"defaults_owner[type=role]", "defaults_reader[type=role]"}, want: []string{"FUNCTIONS"}},
		{name: "one row", patterns: []string{"other_reader[type=role]"}, want: []string{"TABLES", "SEQUENCES"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, generated := defaultPrivilegeDatabaseFixture(), defaultPrivilegeGeneratedFixture()
			// Matching must come from the ACL too, even when roles are not declared.
			database.Roles, generated.Roles = nil, nil
			gotDatabase, reportDatabase, err := atlasfilter.ExcludeDatabaseReport(ownerSelections(), database, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			gotGenerated, reportGenerated, err := atlasfilter.ExcludeGeneratedReport(ownerSelections(), generated, test.patterns, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(defaultPrivilegeTypes(gotDatabase.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(defaultPrivilegeTypes(gotGenerated.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(reportDatabase.Unmatched, qt.ContentEquals, test.unmatched)
			c.Assert(reportGenerated.Unmatched, qt.ContentEquals, test.unmatched)
		})
	}
}

func TestExcludeGeneratedLeavesTheCallersDefaultPrivilegesAlone(t *testing.T) {
	c := qt.New(t)
	schema := defaultPrivilegeGeneratedFixture()
	before := must.Must(schema.FeatureObjects.All())
	got, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), schema, []string{"nosuch_object"}, "public")
	c.Assert(err, qt.IsNil)
	values := must.Must(got.FeatureObjects.All())
	values[0].Value.(*pgdefaultprivilege.Desired).Privileges[0].Privilege = "DELETE"
	c.Assert(must.Must(schema.FeatureObjects.All()), qt.DeepEquals, before)
	c.Assert(must.Must(got.FeatureObjects.All()), qt.DeepEquals, before)
}

func TestScopeNarrowsDefaultPrivilegesToTheSchemaUniverse(t *testing.T) {
	tests := []struct {
		name                          string
		schemas, include, want, roles []string
	}{
		{name: "connected schema", schemas: []string{"public"}, want: []string{"TABLES"}, roles: []string{"defaults_owner", "defaults_reader", "other_owner", "other_reader"}},
		{name: "named schema", schemas: []string{"app"}, want: []string{"SEQUENCES", "FUNCTIONS"}, roles: []string{"defaults_owner", "defaults_reader", "other_owner", "other_reader"}},
		{name: "both schemas", schemas: []string{"public", "app"}, want: []string{"TABLES", "SEQUENCES", "FUNCTIONS"}, roles: []string{"defaults_owner", "defaults_reader", "other_owner", "other_reader"}},
		{name: "include with explicit schema keeps both roles", schemas: []string{"public"}, include: []string{"users"}, want: []string{"TABLES"}, roles: []string{"defaults_owner", "defaults_reader"}},
		{name: "include without explicit schema", include: []string{"users"}, want: make([]string, 0), roles: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Schemas: test.schemas, Include: test.include, DefaultSchema: "public"}
			database, err := atlasfilter.ScopeDatabase(defaultPrivilegeDatabaseFixture(), scope)
			c.Assert(err, qt.IsNil)
			generated, err := atlasfilter.ScopeGenerated(defaultPrivilegeGeneratedFixture(), scope)
			c.Assert(err, qt.IsNil)
			c.Assert(defaultPrivilegeTypes(database.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(defaultPrivilegeTypes(generated.FeatureObjects), qt.ContentEquals, test.want)
			c.Assert(databaseRoleNames(database.Roles), qt.ContentEquals, test.roles)
			c.Assert(generatedRoleNames(generated.Roles), qt.ContentEquals, test.roles)
		})
	}
}

func TestScopeKeepsTheGlobalDefaultInEverySchemaUniverse(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		want    int
	}{
		{name: "any schema", want: 1}, {name: "include narrows global state", include: []string{"users"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"app"}, Include: test.include, DefaultSchema: "app"}
			identity := pgdefaultprivilege.Identity{Grantor: "defaults_owner", ObjectType: "SCHEMAS", Grantee: "defaults_reader"}
			database, generated := defaultPrivilegeDatabaseFixture(), defaultPrivilegeGeneratedFixture()
			database.FeatureObjects = must.Must(schemaext.NewObjects(defaultPrivilegeObject(schemaext.Observed, identity, "USAGE")))
			generated.FeatureObjects = must.Must(schemaext.NewObjects(defaultPrivilegeObject(schemaext.Desired, identity, "USAGE")))
			gotDatabase, err := atlasfilter.ScopeDatabase(database, scope)
			c.Assert(err, qt.IsNil)
			gotGenerated, err := atlasfilter.ScopeGenerated(generated, scope)
			c.Assert(err, qt.IsNil)
			c.Assert(gotDatabase.FeatureObjects.Len(), qt.Equals, test.want)
			c.Assert(gotGenerated.FeatureObjects.Len(), qt.Equals, test.want)
		})
	}
}

// Unknown class and object coverage remains visible even when the comparison
// selects another schema. Neither creates a role declaration as a dependency.
func TestDefaultPrivilegeCoverageSurvivesSelection(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	refs := []objectidentity.ID{
		must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Grantor: "defaults_owner", Schema: "app", ObjectType: "TABLES"}, semantics)),
		must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Schema: "app", ObjectType: "TABLES"}, semantics)),
		must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "defaults_owner", Schema: "app", ObjectType: "FUNCTIONS", Grantee: "defaults_reader"}, semantics)),
	}
	var subjects []schemaext.SubjectCoverage
	for _, ref := range refs {
		subjects = append(subjects, schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "ACL contents are unknown"}})
	}
	database, generated := defaultPrivilegeDatabaseFixture(), defaultPrivilegeGeneratedFixture()
	database.FeatureObjects, generated.FeatureObjects = schemaext.Objects{}, schemaext.Objects{}
	database.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, subjects))
	generated.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, subjects))
	scope := atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"public"}, Include: []string{"users"}, DefaultSchema: "public"}
	gotDatabase, err := atlasfilter.ScopeDatabase(database, scope)
	c.Assert(err, qt.IsNil)
	gotGenerated, err := atlasfilter.ScopeGenerated(generated, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(gotDatabase.FeatureCoverage.SubjectRecords(), qt.DeepEquals, database.FeatureCoverage.SubjectRecords())
	c.Assert(gotGenerated.FeatureCoverage.SubjectRecords(), qt.DeepEquals, generated.FeatureCoverage.SubjectRecords())
	c.Assert(gotDatabase.Roles, qt.HasLen, 0)
	c.Assert(gotGenerated.Roles, qt.HasLen, 0)
	database.Roles, generated.Roles = nil, nil
	patterns := []string{"defaults_owner[type=role]", "app"}
	excludedDatabase, reportDatabase, err := atlasfilter.ExcludeDatabaseReport(ownerSelections(), database, patterns, "public")
	c.Assert(err, qt.IsNil)
	excludedGenerated, reportGenerated, err := atlasfilter.ExcludeGeneratedReport(ownerSelections(), generated, patterns, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(reportDatabase.Unmatched, qt.HasLen, 0)
	c.Assert(reportGenerated.Unmatched, qt.HasLen, 0)
	c.Assert(excludedDatabase.FeatureCoverage.SubjectRecords(), qt.DeepEquals, database.FeatureCoverage.SubjectRecords())
	c.Assert(excludedGenerated.FeatureCoverage.SubjectRecords(), qt.DeepEquals, generated.FeatureCoverage.SubjectRecords())
}

// Subject knowledge leaves with its held object; namespace enrollment remains.
func TestDefaultPrivilegeObjectCoverageLeavesWithItsObject(t *testing.T) {
	c := qt.New(t)
	database, generated := defaultPrivilegeDatabaseFixture(), defaultPrivilegeGeneratedFixture()
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "defaults_owner", Schema: "public", ObjectType: "TABLES", Grantee: "defaults_reader"}, identifier.ForDialect("postgres")))
	subject := schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: ref, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}
	database.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{subject}))
	generated.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{subject}))
	scope := atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"app"}, DefaultSchema: "public"}
	selectedDatabase, err := atlasfilter.ScopeDatabase(database, scope)
	c.Assert(err, qt.IsNil)
	selectedGenerated, err := atlasfilter.ScopeGenerated(generated, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(selectedDatabase.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(selectedGenerated.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(selectedDatabase.FeatureCoverage.KindRecords(), qt.DeepEquals, database.FeatureCoverage.KindRecords())
	c.Assert(selectedGenerated.FeatureCoverage.KindRecords(), qt.DeepEquals, generated.FeatureCoverage.KindRecords())
	excludedDatabase, err := atlasfilter.ExcludeDatabase(ownerSelections(), database, []string{"defaults_reader[type=role]"})
	c.Assert(err, qt.IsNil)
	excludedGenerated, err := atlasfilter.ExcludeGenerated(ownerSelections(), generated, []string{"defaults_reader[type=role]"})
	c.Assert(err, qt.IsNil)
	c.Assert(excludedDatabase.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(excludedGenerated.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
}

// A role selection can retain schema-wide state without retaining a table in
// that schema. Its CREATE SCHEMA declaration must still be present.
func TestScopeKeepsTheSchemaADefaultPrivilegeNames(t *testing.T) {
	c := qt.New(t)
	database, generated := defaultPrivilegeDatabaseFixture(), defaultPrivilegeGeneratedFixture()
	scope := atlasfilter.Scope{Selections: ownerSelections(), Schemas: []string{"app"}, Include: []string{"defaults_reader[type=role]"}, DefaultSchema: "public"}
	gotDatabase, err := atlasfilter.ScopeDatabase(database, scope)
	c.Assert(err, qt.IsNil)
	gotGenerated, err := atlasfilter.ScopeGenerated(generated, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(gotDatabase.Tables, qt.HasLen, 0)
	c.Assert(gotGenerated.Tables, qt.HasLen, 0)
	c.Assert(gotDatabase.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "app"}})
	c.Assert(gotGenerated.Schemas, qt.DeepEquals, []schemamodel.Schema{{Name: "app"}})
	c.Assert(defaultPrivilegeTypes(gotDatabase.FeatureObjects), qt.ContentEquals, []string{"FUNCTIONS", "SEQUENCES"})
	c.Assert(defaultPrivilegeTypes(gotGenerated.FeatureObjects), qt.ContentEquals, []string{"FUNCTIONS", "SEQUENCES"})
}
