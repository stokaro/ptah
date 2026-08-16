package goschema_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
)

const usersSource = `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	//ptah:schema:field name="name" type="TEXT"
	Name string
}
`

const ordersSource = `package entities

//ptah:schema:table name="orders"
type Order struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
	//ptah:schema:field name="user_id" type="INTEGER" foreign="users(id)"
	UserID int64
}
`

// parseRaw parses a single Go source into a raw (un-finalized) Database, the
// form Merge expects — the same thing ParseFS accumulates per file.
func parseRaw(c *qt.C, filename, source string) *goschema.Database {
	db, err := goschema.ParseSource(filename, source)
	c.Assert(err, qt.IsNil)
	return &db
}

// tableIndex returns the position of the named table in db.Tables, or -1.
func tableIndex(db *goschema.Database, name string) int {
	for i, table := range db.Tables {
		if table.Name == name {
			return i
		}
	}
	return -1
}

func TestMergeCombinesDistinctSourcesAndOrdersForeignKeys(t *testing.T) {
	c := qt.New(t)

	users := parseRaw(c, "users.go", usersSource)
	orders := parseRaw(c, "orders.go", ordersSource)

	merged, err := goschema.Merge(users, orders)
	c.Assert(err, qt.IsNil)

	// Both tables are present.
	c.Assert(merged.Tables, qt.HasLen, 2)
	usersIdx := tableIndex(merged, "users")
	ordersIdx := tableIndex(merged, "orders")
	c.Assert(usersIdx >= 0, qt.IsTrue)
	c.Assert(ordersIdx >= 0, qt.IsTrue)

	// orders references users, so the dependency sort places users first.
	c.Assert(usersIdx < ordersIdx, qt.IsTrue)

	// Fields from both sources survive the merge.
	c.Assert(merged.Fields, qt.HasLen, 4)
}

func TestMergeDeduplicatesIdenticalObjects(t *testing.T) {
	c := qt.New(t)

	// The same table defined in two sources collapses to one, as it does across
	// files within a single ParseFS.
	first := parseRaw(c, "users_a.go", usersSource)
	second := parseRaw(c, "users_b.go", usersSource)

	merged, err := goschema.Merge(first, second)
	c.Assert(err, qt.IsNil)

	c.Assert(merged.Tables, qt.HasLen, 1)
	c.Assert(tableIndex(merged, "users") >= 0, qt.IsTrue)
	c.Assert(merged.Fields, qt.HasLen, 2)
}

func TestMergeErrorsOnConflictingDefinitions(t *testing.T) {
	c := qt.New(t)

	// Two sources declaring the same view differently must be rejected rather
	// than silently picking one.
	first := &goschema.Database{
		Views: []goschema.View{{StructName: "ActiveUsers", Name: "active_users", Body: "SELECT id FROM users WHERE active"}},
	}
	second := &goschema.Database{
		Views: []goschema.View{{StructName: "ActiveUsers", Name: "active_users", Body: "SELECT id FROM users"}},
	}

	merged, err := goschema.Merge(first, second)
	c.Assert(err, qt.IsNotNil)
	c.Assert(merged, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "active_users")
}

func TestMergeIdenticalViewsAcrossSourcesDeduplicate(t *testing.T) {
	c := qt.New(t)

	view := goschema.View{StructName: "ActiveUsers", Name: "active_users", Body: "SELECT id FROM users"}
	first := &goschema.Database{Views: []goschema.View{view}}
	second := &goschema.Database{Views: []goschema.View{view}}

	merged, err := goschema.Merge(first, second)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Views, qt.HasLen, 1)
}

func TestMergeSkipsNilSources(t *testing.T) {
	c := qt.New(t)

	users := parseRaw(c, "users.go", usersSource)

	merged, err := goschema.Merge(nil, users, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Tables, qt.HasLen, 1)
	c.Assert(tableIndex(merged, "users") >= 0, qt.IsTrue)
}

func TestMergeNoSourcesReturnsEmpty(t *testing.T) {
	c := qt.New(t)

	merged, err := goschema.Merge()
	c.Assert(err, qt.IsNil)
	c.Assert(merged, qt.IsNotNil)
	c.Assert(merged.Tables, qt.HasLen, 0)
}

func TestParseFS_FinalizationMatchesMerge(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"orders.go": {Data: []byte(ordersSource)},
		"users.go":  {Data: []byte(usersSource)},
	}
	parsed, err := goschema.ParseFS(fsys, ".")
	c.Assert(err, qt.IsNil)

	users := parseRaw(c, "users.go", usersSource)
	orders := parseRaw(c, "orders.go", ordersSource)
	merged, err := goschema.Merge(orders, users)
	c.Assert(err, qt.IsNil)

	c.Assert(parsed, qt.DeepEquals, merged)
}

func TestMerge_AcceptsFinalizedSchema(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"users.go": {Data: []byte(`package fixtures

type Timestamps struct {
	//ptah:schema:field name="created_at" type="TIMESTAMP"
	CreatedAt string
}

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64

	//ptah:embedded mode="inline"
	Timestamps
}
`)},
	}
	finalized, err := goschema.ParseFS(fsys, ".")
	c.Assert(err, qt.IsNil)

	merged, err := goschema.Merge(finalized)
	c.Assert(err, qt.IsNil)

	c.Assert(merged, qt.DeepEquals, finalized)
}

func TestMerge_DoesNotMutateInputs(t *testing.T) {
	c := qt.New(t)

	source := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			PrimaryKey: []string{"id"},
		}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"},
			{StructName: "Metadata", FieldName: "Label", Name: "label", Type: "TEXT"},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "User",
			EmbeddedTypeName: "Metadata",
			Mode:             "inline",
		}},
	}
	want := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			PrimaryKey: []string{"id"},
		}},
		Fields: []goschema.Field{
			{StructName: "User", FieldName: "ID", Name: "id", Type: "BIGINT"},
			{StructName: "Metadata", FieldName: "Label", Name: "label", Type: "TEXT"},
		},
		EmbeddedFields: []goschema.EmbeddedField{{
			StructName:       "User",
			EmbeddedTypeName: "Metadata",
			Mode:             "inline",
		}},
	}

	_, err := goschema.Merge(source, &goschema.Database{})

	c.Assert(err, qt.IsNil)
	c.Assert(source, qt.DeepEquals, want)
}

// A limit one source declared is a limit of the whole composite. Merge built
// every source and the result with newDatabase and appended only the object
// slices, so each source's NotDescribed was dropped on the floor: a schema that
// said "I do not describe extensions" merged into one that said nothing, and
// the comparator then read its silence as a removal -- the exact failure
// coverage exists to prevent (stokaro/ptah#1028).
func TestMergePreservesWhatEachSourceDeclinedToDescribe(t *testing.T) {
	c := qt.New(t)
	first := &goschema.Database{
		NotDescribed: coverage.Set{}.WithKind(coverage.Extension),
	}
	second := &goschema.Database{
		NotDescribed: coverage.Set{}.WithKind(coverage.VirtualTable),
	}

	merged, err := goschema.Merge(first, second)

	c.Assert(err, qt.IsNil)
	c.Assert(merged.NotDescribed.Describes(coverage.Extension, "pgcrypto"), qt.IsFalse)
	c.Assert(merged.NotDescribed.Describes(coverage.VirtualTable, "docs"), qt.IsFalse)
	// A kind neither source declined is still described, so the merge widened
	// nothing it was not given.
	c.Assert(merged.NotDescribed.Describes(coverage.Sequence, "order_seq"), qt.IsTrue)
}
