package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// plainReader answers ReadsRelation only, as a reader written against
// [schemaext.RelationReader] alone does.
type plainReader struct{ reads bool }

func (plainReader) Kind() schemaext.Kind               { return "example.test/plain-reader" }
func (r plainReader) Clone() schemaext.Value           { return r }
func (r plainReader) Equal(other schemaext.Value) bool { return other == schemaext.Value(r) }
func (r plainReader) ReadsRelation(string) bool        { return r.reads }

// schemaReader records the default schema it is asked with, and reads the
// relation only in the schema it is told to.
type schemaReader struct {
	plainReader
	readsIn string
}

func (r schemaReader) Clone() schemaext.Value           { return r }
func (r schemaReader) Equal(other schemaext.Value) bool { return other == schemaext.Value(r) }
func (r schemaReader) ReadsRelationIn(_, defaultSchema string) bool {
	return defaultSchema == r.readsIn
}

// TestReadsRelationIn_AsksTheSchemaAwareMethod pins that a reader which can be
// told the default schema is asked with it: the plain method of the same
// reader answers otherwise, so the answer shows which one was asked.
func TestReadsRelationIn_AsksTheSchemaAwareMethod(t *testing.T) {
	reader := schemaReader{plainReader: plainReader{reads: true}, readsIn: "app"}
	tests := []struct {
		name          string
		defaultSchema string
		want          bool
	}{
		{name: "the schema it reads in", defaultSchema: "app", want: true},
		{name: "another schema", defaultSchema: "public", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaext.ReadsRelationIn(reader, "orders", test.defaultSchema), qt.Equals, test.want)
		})
	}
}

// TestReadsRelationIn_FallsBackToReadsRelation pins the reader that cannot be
// told the schema: it is asked ReadsRelation, whatever the schema.
func TestReadsRelationIn_FallsBackToReadsRelation(t *testing.T) {
	tests := []struct {
		name   string
		reader plainReader
		want   bool
	}{
		{name: "a reader that reads the relation", reader: plainReader{reads: true}, want: true},
		{name: "a reader that does not", reader: plainReader{reads: false}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaext.ReadsRelationIn(test.reader, "orders", "public"), qt.Equals, test.want)
		})
	}
}
