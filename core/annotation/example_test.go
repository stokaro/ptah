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
		Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
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

// palette is a file decoder that mixes every shade a file declares for one
// table into one facet, which no decoder handed a single declaration can do.
type palette struct{ declared []annotation.Declaration }

func (p *palette) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	p.declared = append(p.declared, declaration)
	return nil, nil
}

func (p *palette) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	mixed := make(map[int]*shade)
	first := make(map[int]annotation.Declaration)
	var order []int
	for _, declaration := range p.declared {
		index, err := tables.Owning(declaration.Struct, declaration.Attributes["table"], "a shade")
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Attribute: "table", Err: err}
		}
		if existing, seen := mixed[index]; seen {
			existing.Name += "+" + declaration.Attributes["name"]
			continue
		}
		mixed[index] = &shade{Name: declaration.Attributes["name"]}
		first[index] = declaration
		order = append(order, index)
	}
	contributions := make([]annotation.Contribution, 0, len(order))
	for _, index := range order {
		contributions = append(contributions, annotation.Contribution{Facet: mixed[index], Label: "a shade",
			Table: first[index].Attributes["table"], Source: first[index]})
	}
	return contributions, nil
}

// ExampleFileDecoder shows an owner reading a whole file before it
// contributes: the shades come before the table they belong to, and the owner
// mixes them into one facet once the frontend knows the file's tables.
func ExampleFileDecoder() {
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
		Kinds:    []schemaext.Kind{"example.org/paint/shade"},
		File:     func() annotation.FileDecoder { return &palette{} },
		Coverage: annotation.Unlimited(func() (schemaext.Coverage, error) { return schemaext.Coverage{}, nil }),
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	db, err := goschema.ParseSource(owners, "rooms.go", `package models

//ptah:schema:shade table="rooms" name="teal"
//ptah:schema:shade table="rooms" name="navy"
type RoomShades struct{}

//ptah:schema:table name="rooms"
type Room struct{}
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
	// rooms teal+navy
}
