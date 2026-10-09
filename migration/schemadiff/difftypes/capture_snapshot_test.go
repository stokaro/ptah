package difftypes_test

import (
	"fmt"
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// Reflection discovers mutable properties so adding a pointer, slice, or map
// cannot silently expand a snapshot beyond the fields its clone owns. Feature
// containers have no exported mutable representation and have their own tests.
func TestCommonValueClonesOwnEveryMutableField(t *testing.T) {
	cases := []struct {
		name      string
		prototype any
		clone     func(any) any
	}{
		{name: "declared capture", prototype: schemacapture.TableDeclaration{}, clone: func(v any) any { return v.(schemacapture.TableDeclaration).Clone() }},
		{name: "table removal", prototype: difftypes.TableRemoval{}, clone: func(v any) any { return v.(difftypes.TableRemoval).Clone() }},
		{name: "table removal collection", prototype: difftypes.TableRemovals{}, clone: func(v any) any { return v.(difftypes.TableRemovals).Clone() }},
		{name: "observed capture", prototype: schemacapture.TableObservation{}, clone: func(v any) any { return v.(schemacapture.TableObservation).Clone() }},
		{name: "observed table", prototype: catalog.Table{}, clone: func(v any) any { return v.(catalog.Table).Clone() }},
		{name: "observed column", prototype: catalog.Column{}, clone: func(v any) any { return v.(catalog.Column).Clone() }},
		{name: "observed constraint", prototype: catalog.Constraint{}, clone: func(v any) any { return v.(catalog.Constraint).Clone() }},
		{name: "observed index", prototype: catalog.Index{}, clone: func(v any) any { return v.(catalog.Index).Clone() }},
		{name: "declared table", prototype: schemamodel.Table{}, clone: func(v any) any { return v.(schemamodel.Table).Clone() }},
		{name: "declared field", prototype: schemamodel.Field{}, clone: func(v any) any { return v.(schemamodel.Field).Clone() }},
		{name: "declared constraint", prototype: schemamodel.Constraint{}, clone: func(v any) any { return v.(schemamodel.Constraint).Clone() }},
		{name: "declared index", prototype: schemamodel.Index{}, clone: func(v any) any { return v.(schemamodel.Index).Clone() }},
		{name: "declared enum", prototype: schemamodel.Enum{}, clone: func(v any) any { return v.(schemamodel.Enum).Clone() }},
		{name: "declared trigger", prototype: schemamodel.Trigger{}, clone: func(v any) any { return v.(schemamodel.Trigger).Clone() }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.clone(test.prototype), qt.DeepEquals, test.prototype)
			original := populatedSnapshot(reflect.TypeOf(test.prototype))
			expected := populatedSnapshot(reflect.TypeOf(test.prototype))
			cloned := reflect.New(original.Type()).Elem()
			cloned.Set(reflect.ValueOf(test.clone(original.Interface())))
			c.Assert(cloned.Interface(), qt.DeepEquals, expected.Interface())
			mutateSnapshot(cloned)
			c.Assert(original.Interface(), qt.DeepEquals, expected.Interface())
			c.Assert(reflect.DeepEqual(cloned.Interface(), expected.Interface()), qt.IsFalse)
		})
	}
}

func immutableSnapshot(typ reflect.Type) bool {
	return typ == reflect.TypeFor[schemaext.Facets]() || typ == reflect.TypeFor[schemaext.Objects]() || typ == reflect.TypeFor[schemaext.Coverage]()
}

func populatedSnapshot(typ reflect.Type) reflect.Value {
	value := reflect.New(typ).Elem()
	if immutableSnapshot(typ) {
		return value
	}
	switch typ.Kind() {
	case reflect.Struct:
		for field, entry := range value.Fields() {
			entry.Set(populatedSnapshot(field.Type))
		}
	case reflect.Pointer:
		value.Set(reflect.New(typ.Elem()))
		value.Elem().Set(populatedSnapshot(typ.Elem()))
	case reflect.Slice:
		value.Set(reflect.MakeSlice(typ, 1, 1))
		value.Index(0).Set(populatedSnapshot(typ.Elem()))
	case reflect.Map:
		value.Set(reflect.MakeMap(typ))
		value.SetMapIndex(populatedSnapshot(typ.Key()), populatedSnapshot(typ.Elem()))
	case reflect.String:
		value.SetString("original")
	case reflect.Bool:
		value.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(7)
	default:
		panic(fmt.Sprintf("snapshot fixture needs a value for %s", typ))
	}
	return value
}

func mutateSnapshot(value reflect.Value) {
	if immutableSnapshot(value.Type()) {
		return
	}
	switch value.Kind() {
	case reflect.Struct:
		for _, field := range value.Fields() {
			mutateSnapshot(field)
		}
	case reflect.Pointer:
		mutateSnapshot(value.Elem())
	case reflect.Slice:
		for i := range value.Len() {
			mutateSnapshot(value.Index(i))
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			item := reflect.New(value.Type().Elem()).Elem()
			item.Set(value.MapIndex(key))
			mutateSnapshot(item)
			value.SetMapIndex(key, item)
		}
	case reflect.String:
		value.SetString("mutated")
	case reflect.Bool:
		value.SetBool(false)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value.SetInt(19)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value.SetUint(19)
	default:
		panic(fmt.Sprintf("snapshot mutation needs a case for %s", value.Type()))
	}
}

func TestTableObservationOwnsNestedCatalogState(t *testing.T) {
	c := qt.New(t)
	table := populatedSnapshot(reflect.TypeFor[catalog.Table]())
	constraint := populatedSnapshot(reflect.TypeFor[catalog.Constraint]())
	current := &catalog.Database{Constraints: []catalog.Constraint{constraint.Interface().(catalog.Constraint)}}
	observed := difftypes.TableObservationFor(current, table.Interface().(catalog.Table), "postgres", identifier.ForDialect("postgres"))
	c.Assert(observed.Constraints, qt.HasLen, 1)
	mutateSnapshot(table)
	mutateSnapshot(reflect.ValueOf(&current.Constraints[0]).Elem())
	c.Assert(observed.Table, qt.DeepEquals, populatedSnapshot(reflect.TypeFor[catalog.Table]()).Interface())
	c.Assert(observed.Constraints[0], qt.DeepEquals, populatedSnapshot(reflect.TypeFor[catalog.Constraint]()).Interface())
	cloned := observed.Clone()
	mutateSnapshot(reflect.ValueOf(&cloned.Table).Elem())
	mutateSnapshot(reflect.ValueOf(&cloned.Constraints[0]).Elem())
	c.Assert(observed.Table, qt.DeepEquals, populatedSnapshot(reflect.TypeFor[catalog.Table]()).Interface())
	c.Assert(observed.Constraints[0], qt.DeepEquals, populatedSnapshot(reflect.TypeFor[catalog.Constraint]()).Interface())
}

func TestTableDeclarationsOwnNestedSourceState(t *testing.T) {
	c := qt.New(t)
	table := populatedSnapshot(reflect.TypeFor[schemamodel.Table]()).Interface().(schemamodel.Table)
	field := populatedSnapshot(reflect.TypeFor[schemamodel.Field]()).Interface().(schemamodel.Field)
	constraint := populatedSnapshot(reflect.TypeFor[schemamodel.Constraint]()).Interface().(schemamodel.Constraint)
	trigger := populatedSnapshot(reflect.TypeFor[schemamodel.Trigger]()).Interface().(schemamodel.Trigger)
	trigger.Table = table.QualifiedName()
	enum := populatedSnapshot(reflect.TypeFor[schemamodel.Enum]()).Interface().(schemamodel.Enum)
	field.Foreign = ""
	field.GeneratedFromEmbedded = false
	constraint.ForeignTable = ""
	desired := &schemamodel.Database{Tables: []schemamodel.Table{table}, Fields: []schemamodel.Field{field}, Constraints: []schemamodel.Constraint{constraint}, Triggers: []schemamodel.Trigger{trigger}, Enums: []schemamodel.Enum{enum}}
	declaration := schemacapture.DeclareTable(desired, table, identifier.ForDialect("postgres"))
	creation := difftypes.TableCreationFor(desired, table, table.QualifiedName(), identifier.ForDialect("postgres"))
	mutateSnapshot(reflect.ValueOf(&desired.Tables[0]).Elem())
	mutateSnapshot(reflect.ValueOf(&desired.Fields[0]).Elem())
	mutateSnapshot(reflect.ValueOf(&desired.Constraints[0]).Elem())
	mutateSnapshot(reflect.ValueOf(&desired.Triggers[0]).Elem())
	mutateSnapshot(reflect.ValueOf(&desired.Enums[0]).Elem())
	c.Assert(declaration.Table, qt.DeepEquals, populatedSnapshot(reflect.TypeFor[schemamodel.Table]()).Interface())
	c.Assert(creation.Table, qt.DeepEquals, declaration.Table)
	c.Assert(declaration.Fields[0].Overrides["original"]["original"], qt.Equals, "original")
	c.Assert(creation.Fields[0].Overrides["original"]["original"], qt.Equals, "original")
	c.Assert(declaration.Constraints[0].Columns, qt.DeepEquals, []string{"original"})
	c.Assert(creation.Constraints[0].Columns, qt.DeepEquals, []string{"original"})
	c.Assert(declaration.Enums[0].Values, qt.DeepEquals, []string{"original"})
	c.Assert(creation.Enums[0].Values, qt.DeepEquals, []string{"original"})
	c.Assert(declaration.Triggers[0].Dialects, qt.DeepEquals, []string{"original"})
}
