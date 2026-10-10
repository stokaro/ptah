package annotation_test

import (
	"fmt"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
)

// shade is a feature owner's table setting in this example.
type shade struct{ Name string }

func (*shade) Kind() schemaext.Kind { return "example.org/paint/shade" }
func (s *shade) Clone() schemaext.Value {
	cloned := *s
	return &cloned
}
func (s *shade) Equal(other schemaext.Value) bool {
	o, ok := other.(*shade)
	return ok && *o == *s
}

// ExampleNewSet shows an owner declaring a directive of its own and the Go
// annotation frontend reading it once the caller selects the owner. A real
// owner also claims the knowledge a source holds about its models; this one
// claims none.
func ExampleNewSet() {
	owners, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/paint",
		Directives: []annotation.Directive{{
			Name:   "ptah:schema:shade",
			Scopes: []annotation.Scope{annotation.ScopeStruct},
			Attributes: []annotation.Attribute{
				{Name: "table", Description: "Table to paint.", Value: "string", Required: true},
				{Name: "name", Description: "The shade.", Value: "string", Required: true},
			},
		}},
		Kinds: []schemaext.Kind{"example.org/paint/shade"},
		Decode: func(declaration annotation.Declaration) ([]annotation.Contribution, error) {
			return []annotation.Contribution{{
				Table: declaration.Attributes["table"], Label: "a shade",
				Facet: &shade{Name: declaration.Attributes["name"]},
			}}, nil
		},
		Coverage: func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil },
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	db, err := goschema.ParseSource(owners, "rooms.go", `package models

//ptah:schema:table name="rooms"
type Room struct{}

//ptah:schema:shade table="rooms" name="teal"
type RoomShade struct{}
`)
	if err != nil {
		fmt.Println(err)
		return
	}
	painted, found, err := schemaext.FacetAs[*shade](db.Tables[0].Facets, "example.org/paint/shade")
	if err != nil || !found {
		fmt.Println("no shade", err)
		return
	}
	fmt.Println(db.Tables[0].Name, painted.Name)
	// Output:
	// rooms teal
}
