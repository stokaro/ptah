package pgtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// TestObservedTypes_ReadRelationsByRowType pins what a type the catalog holds
// answers a host that drops a relation before the type: a type reads a
// relation when it names the relation's row type, as a composite type's
// attribute, a domain's base type or a range's subtype, or an array of it.
// Names compare without case, and a schema only where both sides name one.
func TestObservedTypes_ReadRelationsByRowType(t *testing.T) {
	composite := func(types ...string) *pgtype.ObservedComposite {
		value := &pgtype.ObservedComposite{}
		for _, spelled := range types {
			value.Attributes = append(value.Attributes, pgtype.Attribute{Name: "a", Type: spelled})
		}
		return value
	}
	tests := []struct {
		name     string
		value    schemaext.RelationReader
		relation string
		want     bool
	}{
		{name: "a composite type with the row type as an attribute", value: composite("integer", "archive"), relation: "archive", want: true},
		{name: "a composite type with an array of the row type", value: composite("archive[]"), relation: "archive", want: true},
		{name: "a composite type that names the row type in its schema", value: composite("public.archive"), relation: "archive", want: true},
		{name: "a composite type that names the relation's schema", value: composite("archive"), relation: "public.archive", want: true},
		{name: "a composite type that names a row type in another schema", value: composite("audit.archive"), relation: "public.archive"},
		{name: "a composite type that names the row type in another case", value: composite(`"Archive"`), relation: "archive", want: true},
		{name: "a composite type of other types", value: composite("text", "character varying(10)", "timestamp with time zone"), relation: "archive"},
		{name: "a domain over the row type", value: &pgtype.ObservedDomain{BaseType: "archive"}, relation: "archive", want: true},
		{name: "a domain over integer", value: &pgtype.ObservedDomain{BaseType: "integer"}, relation: "archive"},
		{name: "a range over the row type", value: &pgtype.ObservedRange{Subtype: "archive"}, relation: "archive", want: true},
		{name: "a range over numeric", value: &pgtype.ObservedRange{Subtype: "numeric"}, relation: "archive"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.value.ReadsRelation(test.relation)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestObservedTypes_ReadRelationsInTheDefaultSchema pins the answer with the
// default schema known: an unqualified type name is in that schema, so it is
// not the row type of a relation of the same name in another schema.
func TestObservedTypes_ReadRelationsInTheDefaultSchema(t *testing.T) {
	tests := []struct {
		name     string
		value    schemaext.RelationReaderInSchema
		relation string
		want     bool
	}{
		{name: "an unqualified attribute type and a relation in the default schema",
			value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "r", Type: "archive"}}}, relation: "public.archive", want: true},
		{name: "an unqualified attribute type and a relation in another schema",
			value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "r", Type: "archive"}}}, relation: "audit.archive"},
		{name: "a qualified base type and an unqualified relation",
			value: &pgtype.ObservedDomain{BaseType: "audit.archive"}, relation: "archive"},
		{name: "a range over the row type in the default schema",
			value: &pgtype.ObservedRange{Subtype: "public.archive"}, relation: "archive", want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.value.ReadsRelationIn(test.relation, "public")

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
