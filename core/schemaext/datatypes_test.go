package schemaext_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// typeObjectKind is the owner model of a test data type.
const typeObjectKind = "example.org/type"

// definedValue is an object that defines a data type of TypeKind, naming
// References. An Anonymous one answers with a type that has no name.
type definedValue struct {
	TypeKind   objectidentity.Kind
	References []string
	Anonymous  bool
}

func (*definedValue) Kind() schemaext.Kind { return typeObjectKind }
func (v *definedValue) Clone() schemaext.Value {
	return &definedValue{TypeKind: v.TypeKind, References: slices.Clone(v.References), Anonymous: v.Anonymous}
}
func (*definedValue) Equal(other schemaext.Value) bool { _, ok := other.(*definedValue); return ok }
func (v *definedValue) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	ref.Kind = v.TypeKind
	if v.Anonymous {
		ref.Name = objectidentity.Part{}
	}
	return schemaext.DefinedType{ID: ref, References: v.References}
}

// typeRef is the identity of a test data type object in schema.
func typeRef(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(typeObjectKind, schema, name)
}

// definedRef is the identity a test data type answers with: typeRef under kind.
func definedRef(kind objectidentity.Kind, schema, name string) objectidentity.ID {
	ref := typeRef(schema, name)
	ref.Kind = kind
	return ref
}

func TestDefinedTypes_HappyPath(t *testing.T) {
	c := qt.New(t)
	objects, err := schemaext.NewObjects(
		schemaext.Object{Ref: typeRef("app", "zip"), Value: &definedValue{TypeKind: objectidentity.KindDomain, References: []string{"text"}}},
		schemaext.Object{Ref: typeRef("app", "addr"), Value: &definedValue{TypeKind: objectidentity.KindComposite}},
		object(&silentObject{}, "money"),
	)
	c.Assert(err, qt.IsNil)

	got := schemaext.DefinedTypes(objects)

	// Identity order, by each object's own identity, so addr comes before
	// zip. The object that defines no type is passed over.
	c.Assert(got, qt.DeepEquals, []schemaext.DefinedType{
		{ID: definedRef(objectidentity.KindComposite, "app", "addr")},
		{ID: definedRef(objectidentity.KindDomain, "app", "zip"), References: []string{"text"}},
	})
}

// TestDefinedTypes_SharesNoReferences pins that a host may keep the answer:
// changing it changes nothing the objects hold.
func TestDefinedTypes_SharesNoReferences(t *testing.T) {
	c := qt.New(t)
	value := &definedValue{TypeKind: objectidentity.KindDomain, References: []string{"text"}}
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: typeRef("app", "zip"), Value: value})
	c.Assert(err, qt.IsNil)
	first := schemaext.DefinedTypes(objects)

	first[0].References[0] = "mutated"
	second := schemaext.DefinedTypes(objects)

	c.Assert(second[0].References, qt.DeepEquals, []string{"text"})
}

// TestDefinedTypes_FailurePath pins that an object whose value answers with
// an invalid type never enters a collection, so DefinedTypes never meets one:
// both ways in refuse it.
func TestDefinedTypes_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value *definedValue
		want  string
	}{
		{name: "a kind that is not a data type", value: &definedValue{TypeKind: objectidentity.KindTable},
			want: `object .*: invalid feature value: a defined type is a domain, a composite type or a range type, not "table"`},
		{name: "an enum, which stays a common object", value: &definedValue{TypeKind: objectidentity.KindEnum},
			want: `object .*: invalid feature value: a defined type is a domain, a composite type or a range type, not "enum"`},
		{name: "a type without a name", value: &definedValue{TypeKind: objectidentity.KindRange, Anonymous: true},
			want: `object .*: invalid feature value: a defined type has a name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			defining := schemaext.Object{Ref: typeRef("app", "zip"), Value: test.value}

			created, createErr := schemaext.NewObjects(defining)
			extended, withErr := schemaext.Objects{}.With(defining)

			c.Assert(createErr, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(createErr, qt.ErrorMatches, test.want)
			c.Assert(created.Len(), qt.Equals, 0)
			c.Assert(withErr, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(withErr, qt.ErrorMatches, test.want)
			c.Assert(extended.Len(), qt.Equals, 0)
		})
	}
}

// typeChange creates the data type of TypeKind its subject names, or drops it
// when Drops is set.
type typeChange struct {
	TypeKind objectidentity.Kind
	Drops    bool
}

func (*typeChange) Kind() schemaext.Kind { return typeObjectKind + "-change" }
func (v *typeChange) CloneChange() schemaext.ChangeValue {
	return &typeChange{TypeKind: v.TypeKind, Drops: v.Drops}
}
func (v *typeChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	subject.Kind = v.TypeKind
	return schemaext.DefinedType{ID: subject, References: []string{"float8"}}, !v.Drops
}

func TestCreatedTypes_HappyPath(t *testing.T) {
	c := qt.New(t)
	changes := []schemaext.ChangeRecord{
		{Subject: typeRef("app", "zip"), Value: &typeChange{TypeKind: objectidentity.KindRange}},
		{Subject: typeRef("app", "old"), Value: &typeChange{TypeKind: objectidentity.KindDomain, Drops: true}},
		{Subject: named("other"), Value: &silentChange{}},
		{Subject: typeRef("app", "addr"), Value: &typeChange{TypeKind: objectidentity.KindComposite}},
	}

	got, err := schemaext.CreatedTypes(changes)

	c.Assert(err, qt.IsNil)
	// Change order. The drop and the change that creates nothing are passed
	// over.
	c.Assert(got, qt.DeepEquals, []schemaext.DefinedType{
		{ID: definedRef(objectidentity.KindRange, "app", "zip"), References: []string{"float8"}},
		{ID: definedRef(objectidentity.KindComposite, "app", "addr"), References: []string{"float8"}},
	})
}

func TestCreatedTypes_FailurePath(t *testing.T) {
	c := qt.New(t)
	changes := []schemaext.ChangeRecord{{Subject: typeRef("app", "zip"), Value: &typeChange{TypeKind: objectidentity.KindEnum}}}

	got, err := schemaext.CreatedTypes(changes)

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `change of .*: invalid feature value: a defined type is a domain, a composite type or a range type, not "enum"`)
	c.Assert(got, qt.IsNil)
}
