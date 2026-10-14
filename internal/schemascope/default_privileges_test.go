package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgdefaultprivilege/privilegeselect"
	"ptah.run/internal/schemascope"
)

func defaultPrivilegeSelections() selectext.Set {
	return must.Must(selectext.NewSet(privilegeselect.Extension()))
}

func defaultPrivilegeObjects(representation schemaext.Representation) schemaext.Objects {
	var objects []schemaext.Object
	for _, row := range []struct{ schema, class, privilege string }{
		{"auth", "TABLES", "SELECT"}, {"billing", "SEQUENCES", "USAGE"}, {"", "FUNCTIONS", "EXECUTE"},
	} {
		ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: row.schema, ObjectType: row.class, Grantee: "app_reader"}, identifier.ForDialect("postgres")))
		var value schemaext.Value = &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: row.privilege}}}
		if representation == schemaext.Observed {
			value = &pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: row.privilege}}}
		}
		objects = append(objects, schemaext.Object{Ref: ref, Value: value})
	}
	return must.Must(schemaext.NewObjects(objects...))
}

func generatedDefaultPrivilegeFixture() *schemamodel.Database {
	return &schemamodel.Database{FeatureObjects: defaultPrivilegeObjects(schemaext.Desired), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))}
}

func databaseDefaultPrivilegeFixture() *catalog.Database {
	return &catalog.Database{FeatureObjects: defaultPrivilegeObjects(schemaext.Observed), FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed))}
}

func defaultPrivilegeTypes(objects schemaext.Objects) []string {
	var types []string
	for _, ref := range objects.Refs() {
		types = append(types, must.Must(pgdefaultprivilege.IdentityOf(ref)).ObjectType)
	}
	return types
}

// TestFilterScopesDefaultPrivilegesOnBothSidesTheSameWay drives both
// projections from one row, because a schema scope that keeps a default
// privilege on one side and drops it on the other invents work: kept live and
// dropped desired reads as a privilege to revoke, and the other way round as one
// to grant. Neither is something the operator asked for by naming a schema.
//
// Without the keep lines the projection carries every schema's defaults through
// unfiltered, which is not a failure anywhere: `--schema auth` then describes
// billing's defaults as part of auth. The global default is kept in every
// scope, because it applies in the scoped schemas too; read as the connected
// schema's, it would vanish from every scope that did not name that schema
// (stokaro/ptah#3772).
func TestFilterScopesDefaultPrivilegesOnBothSidesTheSameWay(t *testing.T) {
	tests := []struct {
		name          string
		schemas       []string
		defaultSchema string
		want          []string
	}{
		{
			name:    "one named schema keeps its own defaults and the global one",
			schemas: []string{"auth"},
			want:    []string{"TABLES", "FUNCTIONS"},
		},
		{
			name:    "two named schemas keep both and the global one",
			schemas: []string{"auth", "billing"},
			want:    []string{"TABLES", "SEQUENCES", "FUNCTIONS"},
		},
		{
			name:          "the connected schema does not claim the global row",
			schemas:       []string{"auth"},
			defaultSchema: "billing",
			want:          []string{"TABLES", "FUNCTIONS"},
		},
		{
			name:    "a schema holding no default keeps the global one",
			schemas: []string{"public"},
			want:    []string{"FUNCTIONS"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			gotGenerated, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), defaultPrivilegeSelections(),
				generatedDefaultPrivilegeFixture(), test.schemas, test.defaultSchema)
			c.Assert(err, qt.IsNil)
			gotDatabase, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), defaultPrivilegeSelections(),
				databaseDefaultPrivilegeFixture(), test.schemas, test.defaultSchema)
			c.Assert(err, qt.IsNil)

			c.Assert(defaultPrivilegeTypes(gotGenerated.FeatureObjects),
				qt.ContentEquals, test.want)
			c.Assert(defaultPrivilegeTypes(gotDatabase.FeatureObjects),
				qt.ContentEquals, test.want)
		})
	}
}

// TestFilterWithoutSchemasLeavesDefaultPrivilegesAlone is the control on the
// table above: an empty allow-list is not a narrow scope, it is no scope, and a
// projection that dropped the family there would satisfy every row that expects
// something gone.
func TestFilterWithoutSchemasLeavesDefaultPrivilegesAlone(t *testing.T) {
	c := qt.New(t)

	gotGenerated, err := schemascope.FilterGenerated(t.Context(), defaultPrivilegeSelections(), generatedDefaultPrivilegeFixture(), nil)
	c.Assert(err, qt.IsNil)
	gotDatabase, err := schemascope.FilterDatabase(t.Context(), defaultPrivilegeSelections(), databaseDefaultPrivilegeFixture(), nil)
	c.Assert(err, qt.IsNil)

	c.Assert(defaultPrivilegeTypes(gotGenerated.FeatureObjects),
		qt.ContentEquals, []string{"TABLES", "SEQUENCES", "FUNCTIONS"})
	c.Assert(defaultPrivilegeTypes(gotDatabase.FeatureObjects),
		qt.ContentEquals, []string{"TABLES", "SEQUENCES", "FUNCTIONS"})
}

func TestFilterKeepsUnknownDefaultPrivilegeCoverage(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("postgres")
	class := must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Schema: "billing", ObjectType: "TABLES"}, semantics))
	held := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "app_owner", Schema: "billing", ObjectType: "SEQUENCES", Grantee: "app_reader"}, semantics))
	unknown := schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: class, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "FOR ALL ROLES cannot be declared"}}
	known := schemaext.SubjectCoverage{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: held, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}
	desired, current := generatedDefaultPrivilegeFixture(), databaseDefaultPrivilegeFixture()
	desired.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{unknown, known}))
	current.FeatureCoverage = must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{unknown, known}))
	generated, err := schemascope.FilterGenerated(c.Context(), defaultPrivilegeSelections(), desired, []string{"auth"})
	c.Assert(err, qt.IsNil)
	database, err := schemascope.FilterDatabase(c.Context(), defaultPrivilegeSelections(), current, []string{"auth"})
	c.Assert(err, qt.IsNil)
	c.Assert(generated.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{unknown})
	c.Assert(database.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{unknown})
	c.Assert(desired.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
	c.Assert(current.FeatureCoverage.SubjectRecords(), qt.HasLen, 2)
}

func TestFilterRefusesInvalidOwnerStateWithoutProjection(t *testing.T) {
	c := qt.New(t)
	desired, current := generatedDefaultPrivilegeFixture(), databaseDefaultPrivilegeFixture()
	object := must.Must(desired.FeatureObjects.All())[0]
	object.Ref.Name.Source = "not an ACL identity"
	desired.FeatureObjects = must.Must(schemaext.NewObjects(object))
	object.Value = &pgdefaultprivilege.Observed{}
	current.FeatureObjects = must.Must(schemaext.NewObjects(object))
	generated, err := schemascope.FilterGenerated(c.Context(), defaultPrivilegeSelections(), desired, []string{"auth"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(generated, qt.IsNil)
	database, err := schemascope.FilterDatabase(c.Context(), defaultPrivilegeSelections(), current, []string{"auth"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(database, qt.IsNil)
}
