package schemaclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/schemaclean"
)

// installedKind is the model kind of an owner nobody ships, whose objects
// belong to the whole database and are installed into one schema.
const installedKind schemaext.Kind = "example.org/schemaclean/installed"

// installedValue is an observation of such an object, installed in Schema.
type installedValue struct{ Schema string }

func (*installedValue) Kind() schemaext.Kind { return installedKind }

func (v *installedValue) Clone() schemaext.Value { return &installedValue{Schema: v.Schema} }

func (v *installedValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*installedValue)
	return ok && w.Schema == v.Schema
}

func (v *installedValue) InstalledSchema() string { return v.Schema }

// plainKind is the model kind of an owner whose values name no installation
// schema.
const plainKind schemaext.Kind = "example.org/schemaclean/plain"

// plainValue is the control: the same shape, without InstalledSchema.
type plainValue struct{ Schema string }

func (*plainValue) Kind() schemaext.Kind { return plainKind }

func (v *plainValue) Clone() schemaext.Value { return &plainValue{Schema: v.Schema} }

func (v *plainValue) Equal(other schemaext.Value) bool {
	w, ok := other.(*plainValue)
	return ok && w.Schema == v.Schema
}

// installationObject is the object of kind named name, holding value.
func installationObject(kind schemaext.Kind, name string, value schemaext.Value) schemaext.Object {
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(kind), "", name)
	return schemaext.Object{Ref: ref, Value: value}
}

// installationSnapshot holds an object of the invented owner installed in
// the writer's schema, one installed in another schema, and a control value
// that names no installation schema.
func installationSnapshot() *catalog.Database {
	return &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		installationObject(installedKind, "inside", &installedValue{Schema: " app "}),
		installationObject(installedKind, "outside", &installedValue{Schema: "other"}),
		installationObject(plainKind, "plain", &plainValue{Schema: "other"}),
	))}
}

// TestSnapshotWithinWriterScopeKeepsWhatIsInstalledInTheWritersSchema pins
// the writer's scope over any owner's installed objects: an object installed
// in the writer's schema stays, one installed elsewhere leaves, and a value
// that names no installation schema stays. A dialect whose cleanup is not
// schema-scoped keeps every object.
func TestSnapshotWithinWriterScopeKeepsWhatIsInstalledInTheWritersSchema(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: "postgres", want: []string{"inside", "plain"}},
		{dialect: "yugabytedb", want: []string{"inside", "plain"}},
		{dialect: "mysql", want: []string{"inside", "outside", "plain"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := installationSnapshot()

			got := schemaclean.SnapshotWithinWriterScope(schema, test.dialect, "app")

			names := make([]string, 0, got.FeatureObjects.Len())
			for _, ref := range got.FeatureObjects.Refs() {
				names = append(names, ref.Name.Source)
			}
			c.Assert(names, qt.DeepEquals, test.want)
			c.Assert(schema.FeatureObjects.Len(), qt.Equals, 3)
		})
	}
}
