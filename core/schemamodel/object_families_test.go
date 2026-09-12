package schemamodel_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestNewDatabase_InitializesEveryObjectFamily holds the constructor to the
// type it constructs.
//
// [schemamodel.NewDatabase] and [schemamodel.AppendDatabase] each spell out by
// hand which object families a Database carries, and nothing joins either list
// to the struct. Appending to a nil slice is legal, so a family one function
// names and the other omits costs nothing where the omission happens: the
// difference surfaces far away, as a family that encodes as `null` while its
// siblings encode as `[]`.
//
// The assertion reads the Database the constructor returns rather than the Go
// source of the constructor. Parsing the function body with go/ast would answer
// the same question about today's spelling and a narrower one about tomorrow's:
// it would refuse an initialization moved into a helper and accept one written
// in place but assigned to the wrong field. What callers depend on is the value.
func TestNewDatabase_InitializesEveryObjectFamily(t *testing.T) {
	c := qt.New(t)

	c.Assert(initializedFamilies(schemamodel.NewDatabase()), qt.DeepEquals, databaseObjectFamilies())
}

// TestAppendDatabase_CarriesEveryObjectFamily is the same control over the
// other enumeration, and it fails in a costlier way than its sibling: a family
// [schemamodel.AppendDatabase] skips drops every object of that kind from a
// merged schema, and a comparator reads the absence as a removal.
func TestAppendDatabase_CarriesEveryObjectFamily(t *testing.T) {
	c := qt.New(t)

	destination := &schemamodel.Database{}
	schemamodel.AppendDatabase(destination, oneObjectPerFamily())

	c.Assert(populatedFamilies(destination), qt.DeepEquals, databaseObjectFamilies())
}

// databaseObjectFamilies names every [schemamodel.Database] field holding a
// slice of schema objects, in declaration order. Each one is a family both
// merge functions owe an entry.
func databaseObjectFamilies() []string {
	databaseType := reflect.TypeFor[schemamodel.Database]()
	names := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		names = append(names, nameWhen(field.Name, field.Type.Kind() == reflect.Slice)...)
	}
	return names
}

// initializedFamilies names the families of db whose slice is not nil.
func initializedFamilies(db *schemamodel.Database) []string {
	return familiesWhere(db, func(family reflect.Value) bool { return !family.IsNil() })
}

// populatedFamilies names the families of db holding at least one object.
func populatedFamilies(db *schemamodel.Database) []string {
	return familiesWhere(db, func(family reflect.Value) bool { return family.Len() > 0 })
}

// familiesWhere names the object families of db that satisfy holds, in
// declaration order, so a failure prints the diff against
// [databaseObjectFamilies] with the missing families in it.
func familiesWhere(db *schemamodel.Database, holds func(family reflect.Value) bool) []string {
	value := reflect.ValueOf(db).Elem()
	names := make([]string, 0, value.NumField())
	for _, name := range databaseObjectFamilies() {
		names = append(names, nameWhen(name, holds(value.FieldByName(name)))...)
	}
	return names
}

// oneObjectPerFamily returns a Database holding one zero-valued object in every
// family, so a family the destination ends up empty in is a family the append
// path forgot rather than one the fixture never declared.
func oneObjectPerFamily() *schemamodel.Database {
	db := &schemamodel.Database{}
	value := reflect.ValueOf(db).Elem()
	for _, name := range databaseObjectFamilies() {
		family := value.FieldByName(name)
		family.Set(reflect.MakeSlice(family.Type(), 1, 1))
	}
	return db
}

// nameWhen returns name as a one-element slice when include holds, and nothing
// otherwise. It keeps the branch out of the callers, which are read by the
// declarative-test gate.
func nameWhen(name string, include bool) []string {
	return map[bool][]string{true: {name}}[include]
}
