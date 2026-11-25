// Package annotationschema reads the values of a Go annotation that name a
// schema. The frontend in core/goschema reads every tag with it before a
// directive reads the tag, and the annotation cleanup in
// internal/goannotationcleanup reads each tag it removes with it before it
// compares the tag with the export, so the two read one value the same way.
// Read in two places, a quoted schema="\"App\"" would name App on the frontend
// and "App" in the cleanup, and the cleanup would refuse to remove a tag the
// export holds.
package annotationschema

import (
	"strings"

	"ptah.run/internal/tableref"
)

// attributes are the attributes whose value names a schema, in a tag of any
// directive: the schema an object is in and the schema a grant is on. A tag is
// text an author typed, so the space around such a value is removed where the
// tag is read, and a value written as one quoted identifier, schema="\" app\"",
// is the name inside the quotes. Every layer below keeps a schema name's bytes,
// because PostgreSQL holds " app" and "app" as two schemas, so the quoted
// spelling is how a tag names the first. A bare value that begins or ends with
// white space is refused: read without its space it names another schema, and
// a sequence tagged schema="app " beside a live app.counter would plan its
// CREATE SEQUENCE in app as if the author had written app.
//
// An object's own name is not in the list. A column, an index or a constraint
// keeps the bytes its tag writes, because the catalog holds " note" and "note"
// as two columns: trimmed, field name=" note" beside a live " note" planned an
// ADD of "note" and a DROP of " note", and the apply lost the column's values.
//
// The list is one declaration for every directive, rather than a trim at each
// read of a value, so a directive added later reads the value the others read.
var attributes = []string{"schema", "on_schema"}

// schemaDirective is the directive whose name attribute names a schema, the
// one it declares.
const schemaDirective = "ptah:schema:schema"

// Read reads, in place, the value of every attribute of values that names a
// schema in a tag of directive, such as "ptah:schema:table": the schema and
// on_schema attributes of any directive, and the name of
// //ptah:schema:schema. Each is read as [Name] reads it. A value of white space
// alone becomes empty, which each directive reads as the attribute left out.
// The first refusal is returned, and values is then partly read.
func Read(directive string, values map[string]string) error {
	for _, key := range attributes {
		if err := read(values, key); err != nil {
			return err
		}
	}
	if directive == schemaDirective {
		return read(values, "name")
	}
	return nil
}

func read(values map[string]string, key string) error {
	value, ok := values[key]
	if !ok {
		return nil
	}
	name, err := Name(key, value)
	if err != nil {
		return err
	}
	values[key] = name
	return nil
}

// Name reads value, the value of the attribute that names a schema: the space
// around it is removed, and a value that is one quoted identifier, in double
// quotes, backticks or brackets, is the name inside the quotes. A bare value
// that begins or ends with white space is refused with an error matching
// [tableref.ErrSpacedBareName] that names the attribute and says how to quote
// the value.
func Name(attribute, value string) (string, error) {
	if tableref.BareSpace(value, "\"`[", "\"`]") {
		return "", tableref.SpacedBareName(attribute+" attribute", value)
	}
	value = strings.TrimSpace(value)
	if ref, parsed := tableref.Parse(value); parsed && !ref.Qualified {
		value = ref.Name
	}
	return value, nil
}
