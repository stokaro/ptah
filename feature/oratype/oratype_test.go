package oratype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/feature/oratype"
)

// TestRef_FoldsTheNameAsOracleDoes pins the identity rule: a name compares
// without regard to ASCII case, so a declaration's dom_email and the catalog's
// DOM_EMAIL are one domain, and the source spelling is kept.
func TestRef_FoldsTheNameAsOracleDoes(t *testing.T) {
	c := qt.New(t)

	declared, read := oratype.Ref(oratype.DomainKind, "", "dom_email"), oratype.Ref(oratype.DomainKind, "", "DOM_EMAIL")

	c.Assert(declared.Key(), qt.Equals, read.Key())
	c.Assert(declared.Name.Source, qt.Equals, "dom_email")
	c.Assert(oratype.QualifiedName(oratype.Ref(oratype.ObjectTypeKind, "app", "addr_t")), qt.Equals, "app.addr_t")
	c.Assert(oratype.Subject(declared).Kind, qt.Equals, objectidentity.KindDomain)
	c.Assert(oratype.Label(read), qt.Equals, "domain")
}

// TestDesiredObject_HappyPath pins that a valid declaration becomes a feature
// object of its own model, independent of the value it was built from.
func TestDesiredObject_HappyPath(t *testing.T) {
	c := qt.New(t)
	value := &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}}

	object, err := oratype.DesiredObject(oratype.Ref(oratype.ObjectTypeKind, "", "t"), value)
	value.Attributes[0].Name = "changed"

	c.Assert(err, qt.IsNil)
	c.Assert(object.Value, qt.DeepEquals, &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}}})
}

// TestDesiredObject_FailurePath pins the declarations and identities a feature
// object refuses.
func TestDesiredObject_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		ref   objectidentity.ID
		value schemaext.Value
		want  string
	}{
		{name: "a domain without a base type", ref: oratype.Ref(oratype.DomainKind, "", "d"), value: &oratype.DesiredDomain{},
			want: `.*a domain requires a base type.*`},
		{name: "an object type without attributes", ref: oratype.Ref(oratype.ObjectTypeKind, "", "t"), value: &oratype.DesiredObjectType{},
			want: `.*an object type requires at least one attribute.*`},
		{name: "an attribute without a type", ref: oratype.Ref(oratype.ObjectTypeKind, "", "t"),
			value: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n"}}}, want: `.*has a name and a type.*`},
		{name: "an attribute declared twice", ref: oratype.Ref(oratype.ObjectTypeKind, "", "t"),
			value: &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "NUMBER"}, {Name: "n", Type: "DATE"}}},
			want:  `.*attribute "n" is declared twice.*`},
		{name: "an identity of the other kind", ref: oratype.Ref(oratype.ObjectTypeKind, "", "d"),
			value: &oratype.DesiredDomain{BaseType: "NUMBER"}, want: `.*the domain object is identified as "ptah.run/oratype/object-type".*`},
		{name: "an identity without a name", ref: oratype.Ref(oratype.DomainKind, "", " "),
			value: &oratype.DesiredDomain{BaseType: "NUMBER"}, want: `.*the domain identity is schema-scoped and has a name.*`},
		{name: "an observation", ref: oratype.Ref(oratype.DomainKind, "", "d"),
			value: &oratype.ObservedDomain{BaseType: "NUMBER"}, want: `.*expected the desired representation, got the observed one.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			object, err := oratype.DesiredObject(test.ref, test.value)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(object, qt.DeepEquals, schemaext.Object{})
		})
	}
}

// TestChanges_FailurePath pins the change values Validate refuses: no side, a
// difference beside a creation, a surviving type naming none, and a
// difference out of order or of another kind.
func TestChanges_FailurePath(t *testing.T) {
	held, declared := &oratype.ObservedDomain{BaseType: "NUMBER"}, &oratype.DesiredDomain{BaseType: "DATE"}
	tests := []struct {
		name  string
		value interface{ Validate() error }
		want  string
	}{
		{name: "no side", value: &oratype.DomainChange{}, want: `.*the domain change has no current or desired side.*`},
		{name: "a difference beside a creation", value: &oratype.DomainChange{After: declared, Differs: []string{oratype.DiffBaseType}},
			want: `.*the domain change of a creation or a drop names no difference.*`},
		{name: "a survivor naming no difference", value: &oratype.DomainChange{Before: held, After: declared},
			want: `.*the domain change of a surviving type names what differs.*`},
		{name: "differences out of order", value: &oratype.DomainChange{Before: held, After: declared,
			Differs: []string{oratype.DiffNotNull, oratype.DiffBaseType}}, want: `.*the domain change names each difference once, sorted.*`},
		{name: "another kind's difference", value: &oratype.ObjectTypeChange{
			Before: &oratype.ObservedObjectType{Attributes: []oratype.Attribute{{Name: "N", Type: "NUMBER"}}},
			After:  &oratype.DesiredObjectType{Attributes: []oratype.Attribute{{Name: "n", Type: "DATE"}}}, Differs: []string{oratype.DiffBaseType}},
			want: `.*"base_type" is not a difference the object type change names.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.value.Validate()

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
