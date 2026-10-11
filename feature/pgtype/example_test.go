package pgtype_test

import (
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// ExampleDesiredObject shows a declared domain as a feature object and what a
// host reads off it without importing this package: the type under the common
// domain kind, in the schema the declaration wrote, naming its base type.
func ExampleDesiredObject() {
	object, err := pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "email"),
		&pgtype.DesiredDomain{BaseType: "text", Check: "VALUE LIKE '%@%'"})
	if err != nil {
		fmt.Println(err)
		return
	}
	objects, err := schemaext.NewObjects(object)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, defined := range schemaext.DefinedTypes(objects) {
		fmt.Println(defined.ID.Kind, defined.ID.Schema.Authored(), defined.ID.Name.Source, defined.References)
	}
	// Output:
	// domain app email [text]
}

// ExampleCompositeChange shows a composite type that gains an attribute in
// place: the change names the delta ALTER TYPE reaches the declaration with,
// and it creates the type for a host that orders routines after the types a
// plan creates.
func ExampleCompositeChange() {
	change := &pgtype.CompositeChange{
		Before:  &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "city", Type: "text"}}},
		After:   &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "city", Type: "text"}, {Name: "zip", Type: "text"}}},
		Differs: []string{pgtype.DiffAttributes},
		Added:   []pgtype.Attribute{{Name: "zip", Type: "text"}},
	}
	if err := change.Validate(); err != nil {
		fmt.Println(err)
		return
	}
	created, ok := change.CreatedType(pgtype.Ref(pgtype.CompositeKind, "", "address"))
	fmt.Println(change.RemovesState(), ok, created.ID.Kind, created.References)
	// Output:
	// false true composite type [text text]
}
