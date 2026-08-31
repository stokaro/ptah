package schemacensus

import (
	"reflect"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/schemamodel"
)

// Fields returns every exported struct field reachable from
// [schemamodel.Database], sorted, as `<package>.<Type>.<Field>`.
//
// It walks the TYPE graph, so the answer does not depend on any value: a field
// is here because the model has it, not because a fixture happened to populate
// it. Types from outside schemamodel are included where the model reaches them
// -- a Spanner row-TTL parameter lives in core/ast and is exactly as droppable
// as one that does not.
func Fields() []string {
	found := make(map[string]bool)
	walkType(reflect.TypeFor[schemamodel.Database](), found, make(map[reflect.Type]bool))
	names := make([]string, 0, len(found))
	for name := range found {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// elementType unwraps pointers, slices, maps and arrays down to what they hold.
func elementType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice ||
		t.Kind() == reflect.Map || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	return t
}

// typeName is the package-qualified name a field path starts with.
func typeName(t reflect.Type) string {
	path := t.PkgPath()
	if index := strings.LastIndex(path, "/"); index >= 0 {
		path = path[index+1:]
	}
	return path + "." + t.Name()
}

// walkType records every exported field of t and descends into the Ptah types it
// reaches. The visiting set breaks recursion through a type that reaches itself.
func walkType(t reflect.Type, found map[string]bool, visiting map[reflect.Type]bool) {
	t = elementType(t)
	if t.Kind() != reflect.Struct || visiting[t] {
		return
	}
	visiting[t] = true
	defer delete(visiting, t)

	for field := range t.Fields() {
		if field.PkgPath != "" {
			continue
		}
		found[typeName(t)+"."+field.Name] = true
		if next := elementType(field.Type); strings.HasPrefix(next.PkgPath(), ptahModule) {
			walkType(next, found, visiting)
		}
	}
}

// ptahModule scopes the descent to this module's own types. A time.Time reached
// from the model is one field, not a census of the standard library.
const ptahModule = "go.5x5.cz/ptah"

// Populated reports whether any instance of the field named by path carries a
// value other than its zero.
//
// It is the census's coverage question: an ablation of a field no fixture
// declares changes nothing, and that reads exactly like a field nothing renders.
func Populated(schema schemamodel.Database, path string) bool {
	populated := false
	visitField(reflect.ValueOf(schema), path, func(field reflect.Value) {
		if !field.IsZero() {
			populated = true
		}
	})
	return populated
}

// deepCopyDatabase returns a database sharing no memory with its input.
//
// The copy lands in a typed variable rather than coming back through
// reflect.Value.Interface, so there is no assertion to get wrong.
func deepCopyDatabase(schema schemamodel.Database) schemamodel.Database {
	var copied schemamodel.Database
	reflect.ValueOf(&copied).Elem().Set(deepCopy(reflect.ValueOf(schema)))
	return copied
}

// Ablate returns a copy of schema with every instance of the field named by path
// set to its zero value. The input is not modified: the copy shares no slice,
// map or pointer with it.
func Ablate(schema schemamodel.Database, path string) schemamodel.Database {
	// The walk is handed the ADDRESS of the copy, which is what makes the
	// fields it reaches settable; a value obtained from reflect.ValueOf is not.
	ablated := deepCopyDatabase(schema)
	visitField(reflect.ValueOf(&ablated).Elem(), path, func(field reflect.Value) {
		if field.CanSet() {
			field.Set(reflect.Zero(field.Type()))
		}
	})
	return ablated
}

// visitField calls visit for every instance of the field named by path that is
// reachable from value.
func visitField(value reflect.Value, path string, visit func(reflect.Value)) {
	separator := strings.LastIndex(path, ".")
	if separator < 0 {
		return
	}
	owner, name := path[:separator], path[separator+1:]

	var walk func(reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Slice, reflect.Array:
			for index := range v.Len() {
				walk(v.Index(index))
			}
		case reflect.Map:
			for _, key := range v.MapKeys() {
				walk(v.MapIndex(key))
			}
		case reflect.Struct:
			if typeName(v.Type()) == owner {
				if field := v.FieldByName(name); field.IsValid() {
					visit(field)
				}
			}
			for index := range v.Type().NumField() {
				if v.Type().Field(index).PkgPath == "" {
					walk(v.Field(index))
				}
			}
		}
	}
	walk(value)
}

// deepCopy returns a value that shares no memory with its input.
//
// A struct copy is not enough. [schemamodel.Finalize] writes into the slices it
// is handed, so rendering a fixture through a struct copy leaves the FIXTURE
// finalized -- and a later ablation is then undone by the derivation Finalize
// already wrote into the input. Measured while building this package: four host
// fields read as unobservable for that reason alone.
func deepCopy(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return value
		}
		copied := reflect.New(value.Type().Elem())
		copied.Elem().Set(deepCopy(value.Elem()))
		return copied
	case reflect.Slice:
		if value.IsNil() {
			return value
		}
		copied := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for index := range value.Len() {
			copied.Index(index).Set(deepCopy(value.Index(index)))
		}
		return copied
	case reflect.Map:
		if value.IsNil() {
			return value
		}
		copied := reflect.MakeMapWithSize(value.Type(), value.Len())
		for _, key := range value.MapKeys() {
			copied.SetMapIndex(key, deepCopy(value.MapIndex(key)))
		}
		return copied
	case reflect.Struct:
		copied := reflect.New(value.Type()).Elem()
		for index := range value.NumField() {
			if value.Type().Field(index).PkgPath == "" {
				copied.Field(index).Set(deepCopy(value.Field(index)))
			}
		}
		return copied
	default:
		return value
	}
}
