package schemaprep_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/definedtypetest"
	"ptah.run/internal/schemaprep"
)

// unplacedDatabase writes every kind PlaceUnqualified places once without a
// schema and once in schema "other", which it leaves as written.
func unplacedDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:            []schemamodel.Table{{Name: "notes"}, {Name: "kept", Schema: "other"}},
		Sequences:         []schemamodel.Sequence{{Name: "s"}, {Name: "s", Schema: "other"}},
		Enums:             []schemamodel.Enum{{Name: "mood"}, {Name: "shade", Schema: "other"}},
		Functions:         []schemamodel.Function{{Name: "f"}, {Name: "other.g"}},
		Views:             []schemamodel.View{{Name: "v"}, {Name: "other.w"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "mv"}, {Name: "other.mw"}},
		Fields: []schemamodel.Field{
			{StructName: "notes", Name: "m", Type: "mood"},
			{StructName: "notes", Name: "ms", Type: "mood[]"},
			{StructName: "notes", Name: "parent", Type: "bigint", Foreign: "notes(id)"},
			{StructName: "notes", Name: "away", Type: "bigint", Foreign: "other.kept(id)"},
		},
		Indexes: []schemamodel.Index{
			{Name: "notes_m", StructName: "notes", TableName: "notes"},
			{Name: "kept_x", StructName: "kept", TableName: "other.kept"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "fk", Table: "notes", ForeignTable: "notes"},
			{Name: "away", Table: "notes", ForeignTable: "other.kept"},
		},
		Triggers:         []schemamodel.Trigger{{Name: "t", Table: "notes", ExecuteFunction: "f"}},
		RLSPolicies:      []schemamodel.RLSPolicy{{Name: "p", Table: "notes"}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "notes"}},
		Grants:           []schemamodel.Grant{{OnTable: "notes"}, {OnSequence: "s"}, {OnRoutine: "f"}},
		RevokedGrants:    []schemamodel.Grant{{OnTable: "other.kept"}},
		ManagedData:      []schemamodel.ManagedData{{Table: "notes"}, {Table: "kept", Schema: "other"}},
		Dependencies:     map[string][]string{"notes": {"notes", "other.kept"}},
		FunctionDependencies: map[string][]string{
			"f": {"notes"},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(
			definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
			definedtypetest.Object(objectidentity.KindDomain, "other", "code", "text"),
		)),
	}
}

// placedDatabase is unplacedDatabase with every name it writes without a
// schema placed in app.
func placedDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:            []schemamodel.Table{{Name: "notes", Schema: "app"}, {Name: "kept", Schema: "other"}},
		Sequences:         []schemamodel.Sequence{{Name: "s", Schema: "app"}, {Name: "s", Schema: "other"}},
		Enums:             []schemamodel.Enum{{Name: "app.mood"}, {Name: "shade", Schema: "other"}},
		Functions:         []schemamodel.Function{{Name: "app.f"}, {Name: "other.g"}},
		Views:             []schemamodel.View{{Name: "app.v"}, {Name: "other.w"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "app.mv"}, {Name: "other.mw"}},
		Fields: []schemamodel.Field{
			{StructName: "notes", Name: "m", Type: "app.mood"},
			{StructName: "notes", Name: "ms", Type: "app.mood[]"},
			{StructName: "notes", Name: "parent", Type: "bigint", Foreign: "app.notes(id)"},
			{StructName: "notes", Name: "away", Type: "bigint", Foreign: "other.kept(id)"},
		},
		Indexes: []schemamodel.Index{
			{Name: "notes_m", StructName: "notes", TableName: "app.notes"},
			{Name: "kept_x", StructName: "kept", TableName: "other.kept"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "fk", Table: "app.notes", ForeignTable: "app.notes"},
			{Name: "away", Table: "app.notes", ForeignTable: "other.kept"},
		},
		Triggers:         []schemamodel.Trigger{{Name: "t", Table: "app.notes", ExecuteFunction: "app.f"}},
		RLSPolicies:      []schemamodel.RLSPolicy{{Name: "p", Table: "app.notes"}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "app.notes"}},
		Grants:           []schemamodel.Grant{{OnTable: "app.notes"}, {OnSequence: "app.s"}, {OnRoutine: "app.f"}},
		RevokedGrants:    []schemamodel.Grant{{OnTable: "other.kept"}},
		ManagedData:      []schemamodel.ManagedData{{Table: "notes", Schema: "app"}, {Table: "kept", Schema: "other"}},
		Dependencies:     map[string][]string{"app.notes": {"app.notes", "other.kept"}},
		FunctionDependencies: map[string][]string{
			"app.f": {"app.notes"},
		},
	}
}

// TestPlaceUnqualified_HappyPath pins every name a source writes without a
// schema and every one it writes with one: the first takes the schema, the
// second stays as written.
func TestPlaceUnqualified_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := unplacedDatabase()

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	want := placedDatabase()
	got := *placed
	got.FeatureObjects = schemaext.Objects{}
	c.Assert(&got, qt.DeepEquals, want)
	c.Assert(database, qt.DeepEquals, unplacedDatabase())
}

// TestPlaceUnqualified_FeatureObjects pins the identity of a feature object
// whose schema the source left to the default: it names the schema, no longer
// defaulted, so the comparison binds it nowhere else, and a column of its type
// names the schema too.
func TestPlaceUnqualified_FeatureObjects(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
			definedtypetest.Object(objectidentity.KindDomain, "other", "code", "text"),
		)),
		Fields: []schemamodel.Field{{Name: "a", Type: "amount"}, {Name: "c", Type: "code"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(schemaext.DefinedTypes(placed.FeatureObjects), qt.HasLen, 2)
	amount := definedNamed(c, placed.FeatureObjects, "amount")
	c.Assert(amount.Schema.Source, qt.Equals, "app")
	c.Assert(amount.Schema.Defaulted, qt.IsFalse)
	c.Assert(definedNamed(c, placed.FeatureObjects, "code").Schema.Source, qt.Equals, "other")
	c.Assert(placed.Fields[0].Type, qt.Equals, "app.amount")
	c.Assert(placed.Fields[1].Type, qt.Equals, "other.code")
	c.Assert(definedNamed(c, database.FeatureObjects, "amount").Schema.Defaulted, qt.IsTrue)
}

// definedNamed is the identity of the type objects defines under name.
func definedNamed(c *qt.C, objects schemaext.Objects, name string) objectidentity.ID {
	c.Helper()
	defined := schemaext.DefinedTypes(objects)
	at := slices.IndexFunc(defined, func(d schemaext.DefinedType) bool { return d.ID.Name.Source == name })
	c.Assert(at, qt.Not(qt.Equals), -1, qt.Commentf("no type named %q", name))
	return defined[at].ID
}

// TestPlaceUnqualified_NothingToPlace pins the inputs PlaceUnqualified returns
// as they are.
func TestPlaceUnqualified_NothingToPlace(t *testing.T) {
	for _, test := range []struct {
		name     string
		database *schemamodel.Database
		schema   string
	}{
		{name: "no schema", database: unplacedDatabase(), schema: " "},
		{name: "no database", schema: "app"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			placed, err := schemaprep.PlaceUnqualified(test.database, test.schema, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(placed, qt.Equals, test.database)
		})
	}
}

// TestPlaceUnqualified_AmbiguousEnumType pins a column type two enums of one
// name could mean: it stays as written, as QualifyDeclaredUserTypes leaves it.
func TestPlaceUnqualified_AmbiguousEnumType(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Enums:  []schemamodel.Enum{{Name: "mood"}, {Name: "mood"}},
		Fields: []schemamodel.Field{{Name: "m", Type: "mood"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Fields[0].Type, qt.Equals, "mood")
}

func TestPlaceUnqualified_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
		definedtypetest.Object(objectidentity.KindDomain, "app", "amount", "numeric"),
	))}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.ErrorMatches, `place feature objects written without a schema in "app": .*`)
	c.Assert(placed, qt.IsNil)
}
