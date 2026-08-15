package graphqlrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/graphqlrender"
)

func fixture() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Author", Name: "authors"},
			{StructName: "Book", Name: "books"},
		},
		Fields: []goschema.Field{
			{StructName: "Author", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Author", Name: "name", Type: "VARCHAR(255)"},
			{StructName: "Author", Name: "status", Type: "enum_author_status", Nullable: true, Enum: []string{"active", "retired"}},
			{StructName: "Book", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Book", Name: "title", Type: "TEXT"},
			{StructName: "Book", Name: "price", Type: "DECIMAL(10,2)"},
			{StructName: "Book", Name: "published_at", Type: "TIMESTAMP", Nullable: true},
			{StructName: "Book", Name: "in_print", Type: "BOOLEAN"},
			{StructName: "Book", Name: "author_id", Type: "INTEGER", Foreign: "authors(id)"},
			{StructName: "Book", Name: "metadata", Type: "JSONB", Nullable: true},
		},
		Enums: []goschema.Enum{{Name: "enum_author_status", Values: []string{"active", "retired"}}},
	}
}

// allOperations is the fullest selection, used where a test needs every shape
// present at once.
var allOperations = graphqlrender.Operations{
	List: true, ByID: true, CreateInput: true, UpdateInput: true,
}

func render(tb testing.TB, opts graphqlrender.Options) string {
	c := qt.New(tb)
	c.Helper()
	res, err := graphqlrender.Render(fixture(), opts)
	c.Assert(err, qt.IsNil)
	return string(res.Data)
}

func TestRenderObjectTypesAndScalars(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{})

	c.Assert(sdl, qt.Contains, "scalar DateTime")
	c.Assert(sdl, qt.Contains, "scalar JSON")

	c.Assert(sdl, qt.Contains, "enum AuthorStatus {\n  active\n  retired\n}")

	// Primary keys map to ID and non-null.
	c.Assert(sdl, qt.Contains, "type Author {")
	c.Assert(sdl, qt.Contains, "  id: ID!")
	c.Assert(sdl, qt.Contains, "  name: String!")
	c.Assert(sdl, qt.Contains, "  status: AuthorStatus")

	c.Assert(sdl, qt.Contains, "  price: Float!")
	c.Assert(sdl, qt.Contains, "  published_at: DateTime")
	c.Assert(sdl, qt.Contains, "  in_print: Boolean!")
	c.Assert(sdl, qt.Contains, "  metadata: JSON")
}

func TestRenderForeignKeyRelation(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{})
	// The scalar id column is kept and a relation object is added alongside it.
	c.Assert(sdl, qt.Contains, "  author_id: Int!")
	c.Assert(sdl, qt.Contains, "  author: Author!")
}

// TestRenderDefaultIsTypesOnly pins the default export: data types and nothing
// that looks like an executable API. Ptah generates no resolver, authorization,
// or data access, so an operation surface must be asked for by name.
func TestRenderDefaultIsTypesOnly(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{})

	c.Assert(sdl, qt.Contains, "type Author {")
	c.Assert(sdl, qt.Contains, "type Book {")

	absent := []string{
		"type Query", "input ", "Edge", "Connection", "PageInfo", "first: Int",
	}
	for _, marker := range absent {
		c.Assert(sdl, qt.Not(qt.Contains), marker,
			qt.Commentf("types-only export must not contain %q", marker))
	}
}

func TestRenderListOperations(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{Operations: graphqlrender.Operations{List: true}})

	c.Assert(sdl, qt.Contains, "type PageInfo {")
	c.Assert(sdl, qt.Contains, "type AuthorEdge {\n  node: Author!\n  cursor: String!\n}")
	c.Assert(sdl, qt.Contains, "type AuthorConnection {\n  edges: [AuthorEdge!]!\n  pageInfo: PageInfo!\n}")

	query := section(sdl, "type Query {")
	c.Assert(query, qt.Contains, "authors(first: Int, after: String): AuthorConnection")
	c.Assert(query, qt.Contains, "books(first: Int, after: String): BookConnection")
	// The by-id shape is a separate selection.
	c.Assert(query, qt.Not(qt.Contains), "author(id: ID!)")
	c.Assert(sdl, qt.Not(qt.Contains), "input ")
}

func TestRenderByIDOperations(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{Operations: graphqlrender.Operations{ByID: true}})

	query := section(sdl, "type Query {")
	c.Assert(query, qt.Contains, "author(id: ID!): Author")
	c.Assert(query, qt.Contains, "book(id: ID!): Book")
	// Connections belong to the list shape, which was not selected.
	c.Assert(sdl, qt.Not(qt.Contains), "Connection")
	c.Assert(sdl, qt.Not(qt.Contains), "PageInfo")
	c.Assert(sdl, qt.Not(qt.Contains), "input ")
}

func TestRenderCreateInputExcludesServerGeneratedKey(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{Operations: graphqlrender.Operations{CreateInput: true}})

	input := section(sdl, "input BookCreateInput {")
	c.Assert(input, qt.Not(qt.Contains), "  id: ") // the serial primary key is server-generated
	c.Assert(input, qt.Contains, "title: String!")
	c.Assert(input, qt.Contains, "author_id: Int!")
	// An input is not a root operation; no Query is declared for it.
	c.Assert(sdl, qt.Not(qt.Contains), "type Query")
}

func TestRenderUpdateInputDropsKeyAndIsPartial(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{Operations: graphqlrender.Operations{UpdateInput: true}})

	input := section(sdl, "input BookUpdateInput {")
	c.Assert(input, qt.Not(qt.Contains), "  id: ")
	// Every field is optional: an omitted field means "unchanged", so a NOT NULL
	// column must not be re-declared as required on the way in.
	c.Assert(input, qt.Contains, "title: String\n")
	c.Assert(input, qt.Contains, "price: Float\n")
	c.Assert(input, qt.Not(qt.Contains), "!")
}

func TestRenderCreateAndUpdateInputsAreDistinct(t *testing.T) {
	c := qt.New(t)
	sdl := render(c.TB, graphqlrender.Options{
		Operations: graphqlrender.Operations{CreateInput: true, UpdateInput: true},
	})

	c.Assert(sdl, qt.Contains, "input AuthorCreateInput {")
	c.Assert(sdl, qt.Contains, "input AuthorUpdateInput {")
	c.Assert(sdl, qt.Contains, "input BookCreateInput {")
	c.Assert(sdl, qt.Contains, "input BookUpdateInput {")
	// The create shape keeps a required column required; the update shape does not.
	c.Assert(section(sdl, "input BookCreateInput {"), qt.Contains, "title: String!")
	c.Assert(section(sdl, "input BookUpdateInput {"), qt.Contains, "title: String\n")
}

// TestRenderWriteProjectionExcludesServerOwnedColumns covers the whole class of
// columns whose value the database produces, not just the serial one the
// exporter used to recognize.
func TestRenderWriteProjectionExcludesServerOwnedColumns(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Row", Name: "rows"}},
		Fields: []goschema.Field{
			{StructName: "Row", Name: "id", Type: "BIGINT", Primary: true, IdentityGeneration: "ALWAYS"},
			{StructName: "Row", Name: "counter", Type: "INTEGER", AutoInc: true},
			{StructName: "Row", Name: "legacy_seq", Type: "SERIAL"},
			{StructName: "Row", Name: "full_name", Type: "TEXT", GeneratedExpression: "first || ' ' || last"},
			{StructName: "Row", Name: "updated_at", Type: "TIMESTAMP", UpdateExpression: "CURRENT_TIMESTAMP"},
			{StructName: "Row", Name: "note", Type: "TEXT"},
			{StructName: "Row", Name: "state", Type: "TEXT", DefaultSet: true, Default: "new"},
			{StructName: "Row", Name: "chosen_at", Type: "TIMESTAMP", DefaultExpr: "NOW()"},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{CreateInput: true},
	})
	c.Assert(err, qt.IsNil)

	// The whole input, pinned exactly: a column that slipped back in would show
	// up here rather than only in a "not contains" that a rename could dodge.
	c.Assert(string(res.Data), qt.Contains,
		"input RowCreateInput {\n  note: String!\n  state: String\n  chosen_at: DateTime\n}")
}

// TestRenderIdentityByDefaultStaysAssignable separates the two identity modes.
// GENERATED BY DEFAULT accepts a caller-supplied value, so removing it from the
// write projection would drop a column the caller may legitimately set.
func TestRenderIdentityByDefaultStaysAssignable(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Row", Name: "rows"}},
		Fields: []goschema.Field{
			{StructName: "Row", Name: "id", Type: "BIGINT", Primary: true, IdentityGeneration: "BY_DEFAULT"},
			{StructName: "Row", Name: "note", Type: "TEXT"},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{CreateInput: true},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Contains, "input RowCreateInput {\n  id: ID!\n  note: String!\n}")
}

func TestRenderOperationNoticeAccompaniesOperations(t *testing.T) {
	tests := []struct {
		name string
		ops  graphqlrender.Operations
	}{
		{name: "list", ops: graphqlrender.Operations{List: true}},
		{name: "by id", ops: graphqlrender.Operations{ByID: true}},
		{name: "create input", ops: graphqlrender.Operations{CreateInput: true}},
		{name: "update input", ops: graphqlrender.Operations{UpdateInput: true}},
		{name: "every shape", ops: allOperations},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sdl := render(c.TB, graphqlrender.Options{Operations: test.ops})
			c.Assert(sdl, qt.Contains, "Ptah generates no resolvers")
			c.Assert(sdl, qt.Contains, "no authorization or tenant")
		})
	}

	t.Run("types only says nothing about operations", func(t *testing.T) {
		c := qt.New(t)
		sdl := render(c.TB, graphqlrender.Options{})
		c.Assert(sdl, qt.Not(qt.Contains), "Operation shapes")
	})
}

func TestRenderRelationToExcludedTableIsOmitted(t *testing.T) {
	c := qt.New(t)
	res, err := graphqlrender.Render(fixture(), graphqlrender.Options{IncludeTables: []string{"books"}})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)

	c.Assert(sdl, qt.Not(qt.Contains), "author: Author")
	c.Assert(sdl, qt.Contains, "  author_id: Int!") // scalar column stays
	var found bool
	for _, d := range res.Diagnostics {
		if strings.Contains(d.Message, "not exported") {
			found = true
		}
	}
	c.Assert(found, qt.IsTrue)
}

func TestRenderInvalidEnumFallsBackToString(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "phase", Type: "enum_phase", Enum: []string{"in-progress", "done"}},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	c.Assert(sdl, qt.Not(qt.Contains), "enum ")    // no enum type emitted
	c.Assert(sdl, qt.Contains, "  phase: String!") // fell back to scalar
	c.Assert(len(res.Diagnostics) >= 1, qt.IsTrue)
}

func TestRenderSanitizesInvalidNames(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "2fa_tokens"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "2fa_enabled", Type: "BOOLEAN"},
			{StructName: "T", Name: "user-agent", Type: "TEXT", Nullable: true},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	// The digit-leading type name and invalid column names are made legal.
	c.Assert(sdl, qt.Contains, "type _2faToken {")
	c.Assert(sdl, qt.Contains, "  _2fa_enabled: Boolean!")
	c.Assert(sdl, qt.Contains, "  user_agent: String")
	c.Assert(len(res.Diagnostics) >= 2, qt.IsTrue)
}

func TestRenderArrayColumn(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "posts"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "T", Name: "tags", Type: "TEXT[]", Nullable: true},
			{StructName: "T", Name: "scores", Type: "INTEGER[]"},
		},
	}
	sdl := string(mustRender(c.TB, db))
	c.Assert(sdl, qt.Contains, "  tags: [String]")
	c.Assert(sdl, qt.Contains, "  scores: [Int]!")
}

// TestRenderEmptySelectionKeepsQueryParsable covers a partial selection that
// matches nothing: with a query shape requested the root must still be a legal,
// non-empty type, and with none requested there must be no root at all.
func TestRenderEmptySelectionKeepsQueryParsable(t *testing.T) {
	t.Run("query shape selected", func(t *testing.T) {
		c := qt.New(t)
		res, err := graphqlrender.Render(fixture(), graphqlrender.Options{
			IncludeTables: []string{"does_not_exist"},
			Operations:    allOperations,
		})
		c.Assert(err, qt.IsNil)
		sdl := string(res.Data)
		c.Assert(sdl, qt.Contains, "type Query {\n  _empty: Boolean\n}")
		c.Assert(sdl, qt.Not(qt.Contains), "type Author")
	})

	t.Run("types only", func(t *testing.T) {
		c := qt.New(t)
		res, err := graphqlrender.Render(fixture(), graphqlrender.Options{
			IncludeTables: []string{"does_not_exist"},
		})
		c.Assert(err, qt.IsNil)
		c.Assert(string(res.Data), qt.Not(qt.Contains), "type Query")
	})
}

// TestRenderByIDOmittedWhenKeyColumnIsNotPublished covers an incomplete key: the
// primary-key column lost a name collision and is not in the object type, so a
// by-id field naming it would reference a field that does not exist.
func TestRenderByIDOmittedWhenKeyColumnIsNotPublished(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "widgets"}},
		Fields: []goschema.Field{
			// Both sanitize to "a_b"; the second one loses and is dropped, and it
			// is the declared primary key.
			{StructName: "T", Name: "a_b", Type: "TEXT"},
			{StructName: "T", Name: "a-b", Type: "TEXT", Primary: true},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{ByID: true},
	})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "type Query {\n  _empty: Boolean\n}")
	c.Assert(sdl, qt.Not(qt.Contains), "widget(")
	c.Assert(diagnosticText(res), qt.Contains,
		"the primary key column is not present in the exported type; by-id query omitted")
}

// TestRenderByIDArgumentMatchesPublishedColumn pins the key argument to the type
// the object type actually published. A key whose column type did not resolve is
// not an ID, and declaring it as one would describe a schema Ptah did not emit.
func TestRenderByIDArgumentMatchesPublishedColumn(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "widgets"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "key", Type: "SOME_UNKNOWN_TYPE", Primary: true},
			{StructName: "T", Name: "note", Type: "TEXT"},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{ByID: true},
	})
	c.Assert(err, qt.IsNil)
	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "  key: String!")
	c.Assert(sdl, qt.Contains, "  widget(key: String!): Widget")
}

// TestRenderCompositeKeyHasNoByIDQuery keeps the by-id shape off a table whose
// key cannot be expressed as one argument.
func TestRenderCompositeKeyHasNoByIDQuery(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "memberships", PrimaryKey: []string{"user_id", "group_id"}}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "user_id", Type: "BIGINT"},
			{StructName: "T", Name: "group_id", Type: "BIGINT"},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{ByID: true, List: true},
	})
	c.Assert(err, qt.IsNil)
	query := section(string(res.Data), "type Query {")
	c.Assert(query, qt.Contains, "memberships(first: Int, after: String): MembershipConnection")
	c.Assert(query, qt.Not(qt.Contains), "membership(")
	c.Assert(diagnosticText(res), qt.Contains,
		"by-id query needs a single-column primary key; this table declares 2 key column(s), so it is omitted")
}

// TestRenderInputOmittedWhenProjectionIsEmpty keeps an unparsable "input X {}"
// out of the output when every column is server-owned.
func TestRenderInputOmittedWhenProjectionIsEmpty(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "counters"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "SERIAL", Primary: true},
		},
	}
	res, err := graphqlrender.Render(db, graphqlrender.Options{
		Operations: graphqlrender.Operations{CreateInput: true, UpdateInput: true},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(res.Data), qt.Not(qt.Contains), "input ")
	c.Assert(diagnosticText(res), qt.Contains, "the create projection is empty; input omitted")
	c.Assert(diagnosticText(res), qt.Contains, "the update projection is empty; input omitted")
}

func mustRender(tb testing.TB, db *goschema.Database) []byte {
	c := qt.New(tb)
	c.Helper()
	res, err := graphqlrender.Render(db, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)
	return res.Data
}

func TestRenderDeterministic(t *testing.T) {
	tests := []struct {
		name string
		ops  graphqlrender.Operations
	}{
		{name: "types only", ops: graphqlrender.Operations{}},
		{name: "list", ops: graphqlrender.Operations{List: true}},
		{name: "by id", ops: graphqlrender.Operations{ByID: true}},
		{name: "create input", ops: graphqlrender.Operations{CreateInput: true}},
		{name: "update input", ops: graphqlrender.Operations{UpdateInput: true}},
		{name: "every shape", ops: allOperations},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := graphqlrender.Options{Operations: test.ops}
			c.Assert(render(c.TB, opts), qt.Equals, render(c.TB, opts))
		})
	}
}

// TestRenderObjectTypeNamesAreStableAcrossProfiles keeps the data half of the
// schema identical however the operation half is selected, so turning an
// operation on never renames a published type.
func TestRenderObjectTypeNamesAreStableAcrossProfiles(t *testing.T) {
	c := qt.New(t)
	typesOnly := section(render(c.TB, graphqlrender.Options{}), "type Book {")

	tests := []struct {
		name string
		ops  graphqlrender.Operations
	}{
		{name: "list", ops: graphqlrender.Operations{List: true}},
		{name: "by id", ops: graphqlrender.Operations{ByID: true}},
		{name: "create input", ops: graphqlrender.Operations{CreateInput: true}},
		{name: "update input", ops: graphqlrender.Operations{UpdateInput: true}},
		{name: "every shape", ops: allOperations},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := section(render(c.TB, graphqlrender.Options{Operations: test.ops}), "type Book {")
			c.Assert(got, qt.Equals, typesOnly)
		})
	}
}

// diagnosticText joins a result's diagnostics so a test can assert on them with
// a plain substring check rather than a loop with a conditional in it.
func diagnosticText(res graphqlrender.Result) string {
	messages := make([]string, 0, len(res.Diagnostics))
	for _, diagnostic := range res.Diagnostics {
		messages = append(messages, string(diagnostic.Severity)+": "+diagnostic.Path+": "+diagnostic.Message)
	}
	return strings.Join(messages, "\n")
}

// section returns the block starting at header up to the next blank line, so a
// substring assertion is scoped to one definition.
func section(sdl, header string) string {
	idx := strings.Index(sdl, header)
	if idx < 0 {
		return ""
	}
	rest := sdl[idx:]
	if before, _, ok := strings.Cut(rest, "\n}\n"); ok {
		return before
	}
	return rest
}
