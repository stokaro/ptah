package diffreport_test

import (
	"reflect"
	"strings"

	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// diffFieldByJSONName returns the SchemaDiff field that serializes under name.
func diffFieldByJSONName(name string) (reflect.StructField, bool) {
	for field := range reflect.TypeFor[difftypes.SchemaDiff]().Fields() {
		tag, tagged := field.Tag.Lookup("json")
		serialized, _, _ := strings.Cut(tag, ",")
		if !tagged {
			serialized = field.Name
		}
		if serialized == name {
			return field, true
		}
	}
	return reflect.StructField{}, false
}

// diffWithOnly builds a diff whose one non-empty list is field, holding a
// single zero-valued element.
func diffWithOnly(field reflect.StructField) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	reflect.ValueOf(diff).Elem().FieldByIndex(field.Index).Set(reflect.MakeSlice(field.Type, 1, 1))
	return diff
}
