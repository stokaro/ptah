package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/synonym"
	"ptah.run/internal/atlasfilter"
)

// synonymNames lists the synonyms objects holds, each with its declared
// comment.
func synonymNames(c *qt.C, objects schemaext.Objects) []string {
	c.Helper()
	all := must.Must(objects.All())
	names := make([]string, 0, len(all))
	for _, object := range all {
		name := object.Ref.Schema.Source + "." + object.Ref.Name.Source
		if declared, ok := object.Value.(*synonym.DesiredSynonym); ok {
			name += "=" + declared.Comment
		}
		names = append(names, name)
	}
	return names
}

func declaredSynonym(schema, name, comment string) schemaext.Object {
	return synonym.DesiredObject(synonym.DesiredSynonym{Synonym: synonym.Synonym{Schema: schema, Name: name, Target: "dbo.t"}, Comment: comment})
}

// TestExcludeDatabase_SubtractsASynonymsComment honors the synonym comment
// field selector the reference documents: the synonym stays, its comment
// goes, and the selector is not reported as naming nothing. Moving synonyms
// to their owner left the selector accepted and doing nothing.
func TestExcludeDatabase_SubtractsASynonymsComment(t *testing.T) {
	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{name: "every synonym's comment", exclude: []string{"*[type=synonym].comment"}, want: []string{"dbo.kept=", "dbo.other="}},
		{name: "one synonym's comment", exclude: []string{"kept[type=synonym].comment"}, want: []string{"dbo.kept=", "dbo.other=b"}},
		{name: "every field", exclude: []string{"*[type=synonym].*"}, want: []string{"dbo.kept=", "dbo.other="}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "kept", "a"), declaredSynonym("dbo", "other", "b")))}

			filtered, report, err := atlasfilter.ExcludeDatabaseReport(ownerSelections(), db, test.exclude, "dbo")

			c.Assert(err, qt.IsNil)
			c.Assert(report.Unmatched, qt.HasLen, 0)
			c.Assert(synonymNames(c, filtered.FeatureObjects), qt.ContentEquals, test.want)
		})
	}
}

// TestScopeGenerated_KeepsADottedSynonymInItsSchema selects a declared
// synonym whose name holds a dot by its schema and name, as the read side
// does. Matching it by its qualified spelling put `dbo."a.b"` in the default
// schema: a scope of dbo dropped it, and a scope of the default kept it.
func TestScopeGenerated_KeepsADottedSynonymInItsSchema(t *testing.T) {
	tests := []struct {
		name  string
		scope atlasfilter.Scope
		want  []string
	}{
		{name: "its schema", scope: atlasfilter.Scope{Schemas: []string{"dbo"}}, want: []string{"dbo.a.b="}},
		{name: "the default schema", scope: atlasfilter.Scope{Schemas: []string{"public"}}, want: make([]string, 0)},
		{name: "its type in its schema", scope: atlasfilter.Scope{Include: []string{"dbo.*[type=synonym]"}}, want: []string{"dbo.a.b="}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.scope.DefaultSchema, test.scope.Selections = "public", ownerSelections()
			db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "a.b", "")))}

			scoped, err := atlasfilter.ScopeGenerated(db, test.scope)

			c.Assert(err, qt.IsNil)
			c.Assert(synonymNames(c, scoped.FeatureObjects), qt.ContentEquals, test.want)
		})
	}
}

// TestExcludeGenerated_ExcludesASynonymOnTheDeclaredSide drops a synonym an
// exclusion names from the declaration as well as from the read. Leaving it
// in the declaration while the read lost it planned CREATE SYNONYM for the
// object the selector was written to leave alone.
func TestExcludeGenerated_ExcludesASynonymOnTheDeclaredSide(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "alias", "a"), declaredSynonym("dbo", "kept", "b")))}
	read := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "alias", "a"), declaredSynonym("dbo", "kept", "b")))}

	generated, err := atlasfilter.ExcludeGeneratedWithDefaultSchema(ownerSelections(), declared, []string{"alias[type=synonym]"}, "dbo")
	c.Assert(err, qt.IsNil)
	current, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), read, []string{"alias[type=synonym]"}, "dbo")
	c.Assert(err, qt.IsNil)

	c.Assert(synonymNames(c, generated.FeatureObjects), qt.DeepEquals, []string{"dbo.kept=b"})
	c.Assert(synonymNames(c, current.FeatureObjects), qt.DeepEquals, []string{"dbo.kept=b"})
}

// TestExcludeGenerated_SubtractsASynonymsComment subtracts a declared
// synonym's comment from the declaration, where the comment is written.
func TestExcludeGenerated_SubtractsASynonymsComment(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "alias", "a"), declaredSynonym("dbo", "kept", "b")))}

	generated, report, err := atlasfilter.ExcludeGeneratedReport(ownerSelections(), declared, []string{"alias[type=synonym].comment"}, "dbo")

	c.Assert(err, qt.IsNil)
	c.Assert(report.Unmatched, qt.HasLen, 0)
	c.Assert(synonymNames(c, generated.FeatureObjects), qt.ContentEquals, []string{"dbo.alias=", "dbo.kept=b"})
}

// TestExcludeDatabase_ASynonymExclusionNamesItsRelation leaves out, with an
// excluded synonym, the table grant written to its name: the alias is a
// relation's name, and an exclusion of it covers what names that relation.
func TestExcludeDatabase_ASynonymExclusionNamesItsRelation(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(declaredSynonym("dbo", "alias", ""))),
		Grants: []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "alias"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "other"}},
	}

	current, err := atlasfilter.ExcludeDatabaseWithDefaultSchema(ownerSelections(), read, []string{"alias[type=synonym]"}, "dbo")

	c.Assert(err, qt.IsNil)
	c.Assert(current.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(current.Grants, qt.DeepEquals, []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "dbo", ObjectName: "other"}})
}
