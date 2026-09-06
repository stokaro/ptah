package catalog_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestDatabase_EveryFieldItSerializesNamesItselfHappyPath is the guard
// stokaro/ptah#2760 needed, rather than a check for the field it reported.
//
// A [catalog.Database] is what `ptah db read` and `ptah schema inspect` print,
// and what a schema fingerprint and a plan's current_schema_digest hash. Its
// convention is snake_case, and encoding/json falls back to the GO IDENTIFIER
// for a field with no tag -- so an untagged field puts `Refresh` among
// lowercase keys, and a reader following the document's own convention cannot
// reach it or anything inside it.
//
// It walks the type graph rather than a rendered document on purpose. A
// document only shows the fields a fixture happened to populate, so a sweep
// over one would report success for every type it did not reach; the
// measurement that opened the issue found one instance of three for exactly
// that reason.
func TestDatabase_EveryFieldItSerializesNamesItselfHappyPath(t *testing.T) {
	c := qt.New(t)

	untagged, reached := untaggedFieldsUnder(reflect.TypeFor[catalog.Database]())

	// A liveness floor: a walk that stopped at the first type would report no
	// findings and read exactly like a clean one.
	c.Assert(reached > 20, qt.IsTrue, qt.Commentf("reached %d types", reached))
	c.Assert(untagged, qt.HasLen, 0)
}

// untaggedFieldsUnder returns every exported field with no `json` tag reachable
// from a type, and how many struct types it visited.
//
// Reachability is through pointers, slices, arrays and map values as well as
// fields, because that is how the encoder reaches them: catalog.Database holds
// its materialized views in a slice, and the field this issue reported was two
// levels down through a pointer into another package.
func untaggedFieldsUnder(root reflect.Type) ([]string, int) {
	seen := make(map[reflect.Type]bool)
	var untagged []string

	var walk func(reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.Map {
			walk(typ.Elem())
			return
		}
		if typ.Kind() != reflect.Struct || seen[typ] {
			return
		}
		seen[typ] = true
		for field := range typ.Fields() {
			if !field.IsExported() {
				continue
			}
			if _, tagged := field.Tag.Lookup("json"); !tagged {
				untagged = append(untagged, qualify(typ, field.Name))
			}
			walk(field.Type)
		}
	}
	walk(root)

	slices.Sort(untagged)
	return untagged, len(seen)
}

// qualify names a field the way a reader would look for it, package included:
// the field this issue reported lives in core/ast, which the catalog embeds.
func qualify(typ reflect.Type, field string) string {
	pkg := typ.PkgPath()
	if index := strings.LastIndex(pkg, "/"); index >= 0 {
		pkg = pkg[index+1:]
	}
	return pkg + "." + typ.Name() + "." + field
}
