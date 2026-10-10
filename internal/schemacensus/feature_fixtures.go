package schemacensus

import (
	"reflect"
	"strings"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
)

// unknownFacet exercises the renderer's refusal at every common attachment
// point. Silently losing an opaque value would make that field unobservable.
type unknownFacet struct{}

func (*unknownFacet) Kind() schemaext.Kind   { return "ptah.run/census/unknown" }
func (*unknownFacet) Clone() schemaext.Value { return &unknownFacet{} }
func (*unknownFacet) Equal(other schemaext.Value) bool {
	_, ok := other.(*unknownFacet)
	return ok
}

func withFacetFixtures(fixtures []Fixture) []Fixture {
	result := append([]Fixture(nil), fixtures...)
	for _, path := range Fields() {
		if !strings.HasSuffix(path, ".Facets") {
			continue
		}
		for _, fixture := range fixtures {
			copyOfSchema := deepCopyDatabase(fixture.Schema)
			found := false
			visitField(reflect.ValueOf(&copyOfSchema).Elem(), path, readWrite, func(field reflect.Value) {
				if field.CanSet() {
					field.Set(reflect.ValueOf(must.Must(schemaext.NewFacets(&unknownFacet{}))))
					found = true
				}
			})
			if found {
				fixture.Name = "facet-" + path
				fixture.Schema = copyOfSchema
				result = append(result, fixture)
				break
			}
		}
	}
	return result
}
