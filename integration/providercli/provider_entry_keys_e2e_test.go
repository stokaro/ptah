//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/yamlschema"
	"ptah.run/engine"
)

func entryKeysProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "entry-keys")
	return runtime
}

// entryKeysYAML writes a retention on a column entry, on a table entry for a
// column of the table, on an index entry of the table with its type, and on
// a top-level index entry.
const entryKeysYAML = `tables:
  records:
    columns:
      id: {type: INTEGER, retention: {seconds: 5}}
      body: {type: TEXT}
    retention: {seconds: 60, column: body}
    indexes:
      by_body: {fields: [body], type: btree, retention: {seconds: 9}}
indexes:
  by_id: {table: records, fields: [id], retention: {minutes: 1}}
`

// TestProviderYAMLEntryKeys_HappyPath reads the provider's keys of column and
// index entries, and a table key that declares a setting of one of its
// columns, and attaches each value to the part of the table it belongs to.
func TestProviderYAMLEntryKeys_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := entryKeysProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(entryKeysYAML))

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{
		`column id {"seconds":5}`,
		`column body {"seconds":60}`,
		`index by_body {"seconds":9}`,
		`index by_id {"seconds":60}`,
	})
}

// TestProviderYAMLEntryKeys_FailurePath refuses an index the provider reads
// the inherited type of and cannot hold, a key of a column entry nobody
// reads, a setting a table key declares for a column of another table, and a
// top-level index key whose table the document does not declare.
func TestProviderYAMLEntryKeys_FailurePath(t *testing.T) {
	runtime := entryKeysProvider(qt.New(t))
	const head = "tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n"
	for _, row := range []struct{ name, source, want string }{
		{"an index whose inherited type the provider refuses", head + "    indexes:\n      by_id: {fields: [id], type: hash, retention: {seconds: 9}}\n",
			`.*provider decode source failed \(Unknown\)`},
		{"a column key nobody reads", head + "      body: {type: TEXT, retenton: {seconds: 9}}\n",
			`parse YAML schema: line 5: unknown key "retenton" of column "body" of table "records"`},
		{"a table key's setting of another table's column", head + "    retention: {seconds: 60, column: id, of: other}\n",
			`(?s).*decoded YAML facet is owned by column "id", which is neither the table "records" of its key nor a column of it`},
		{"a top-level index key without its table", head + "indexes:\n  by_id: {table: missing, fields: [id], retention: {seconds: 9}}\n",
			`parse YAML schema: index "by_id" names table "missing", which the document does not declare; an owner's key of an index needs its table`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(row.source))

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestProviderYAMLEntryKeysWithoutTheProvider_FailurePath is the control: a
// run that did not select the provider refuses its column key as unknown.
func TestProviderYAMLEntryKeysWithoutTheProvider_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER, retention: {seconds: 5}}\n"))

	c.Assert(err, qt.ErrorMatches, `parse YAML schema: line 4: unknown key "retention" of column "id" of table "records"`)
	c.Assert(db, qt.IsNil)
}

const entryKeysGo = `package p

//ptah:schema:table name="records"
type Record struct {
	//ptah:schema:field name="id" type="INTEGER"
	ID int
	//ptah:schema:field name="body" type="TEXT"
	Body string

	//ptah:schema:index name="by_body" fields="body" type="btree" seconds=9
	_ int
	//ptah:schema:index name="by_id" fields="id" type="btree"
	_ int
}
`

// TestProviderGoInheritedAttributes_HappyPath sends the provider the index
// type with its own attributes of the frontend's index directive, and an
// index that writes the type alone declares nothing.
func TestProviderGoInheritedAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := entryKeysProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", entryKeysGo)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{`index by_body {"seconds":9}`})
	c.Assert(db.Indexes[0].Type, qt.Equals, "btree")
}

// TestProviderGoInheritedAttributes_FailurePath refuses an index whose
// inherited type the provider cannot hold a retention on.
func TestProviderGoInheritedAttributes_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := entryKeysProvider(c)
	source := "package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\"\n\tID int\n\n" +
		"\t//ptah:schema:index name=\"by_id\" fields=\"id\" type=\"hash\" seconds=9\n\t_ int\n}\n"

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*provider decode source failed \(Unknown\).*`)
	c.Assert(db.Tables, qt.HasLen, 0)
}
