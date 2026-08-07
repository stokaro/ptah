package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderInspectedDeclaresTheSchemaEveryObjectReferences pins that a schema
// read out of a database is rendered as a valid HCL file (stokaro/ptah#1234).
//
// A catalog does not repeat the schema on objects the engine treats it as
// implicit for, so an inspected IR arrives with no schema anywhere. HCL has no
// such notion, and the pinned Atlas community binary v1.3.0 refuses the result
// with `cannot extract schema name for table "t"`.
//
// Both halves are needed and each row says which. A `schema = schema.public`
// reference with no matching block is refused by that binary just as the bare
// table is; the block alone leaves the table unattached. Measured:
//
//	table with schema = schema.public, no block   exit 1
//	block plus reference                          exit 0
func TestRenderInspectedDeclaresTheSchemaEveryObjectReferences(t *testing.T) {
	tests := []struct {
		name          string
		defaultSchema string
		tableSchema   string
		wantBlock     string
		wantReference string
	}{
		{
			name:          "a table the catalog reported without a schema",
			defaultSchema: "public",
			tableSchema:   "",
			wantBlock:     "schema \"public\" {\n}\n",
			wantReference: "  schema = schema.public\n",
		},
		{
			name:          "SQLite's implicit schema is named the same way",
			defaultSchema: "main",
			tableSchema:   "",
			wantBlock:     "schema \"main\" {\n}\n",
			wantReference: "  schema = schema.main\n",
		},
		{
			// A reader that does report the schema is believed. The default is
			// only a fallback, so a table in another schema keeps it.
			name:          "a table that carries its own schema keeps it",
			defaultSchema: "public",
			tableSchema:   "reporting",
			wantBlock:     "schema \"reporting\" {\n}\n",
			wantReference: "  schema = schema.reporting\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(
				inspectedTable(test.tableSchema), platform.Postgres, test.defaultSchema,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.wantBlock)
			c.Assert(string(result.Data), qt.Contains, test.wantReference)
		})
	}
}

// TestRenderInspectedDoesNotDuplicateADeclaredSchema pins that a read which did
// report the schema keeps exactly what it reported, comment and all, rather
// than gaining a second bare block beside it.
func TestRenderInspectedDoesNotDuplicateADeclaredSchema(t *testing.T) {
	c := qt.New(t)

	db := inspectedTable("public")
	db.Schemas = []goschema.Schema{{Name: "public", Comment: "standard public schema"}}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "schema \"public\" {\n  comment = \"standard public schema\"\n}\n")
	c.Assert(string(result.Data), qt.Not(qt.Contains), "schema \"public\" {\n}\n")
}

// TestRenderForDialectSynthesizesNoSchema pins that the parse-and-re-render
// callers are untouched.
//
// Their IR came from HCL, so it already carries whatever the author declared.
// Synthesizing a schema there would invent one the author did not write, and
// silently change a file they control.
func TestRenderForDialectSynthesizesNoSchema(t *testing.T) {
	tests := []struct {
		name   string
		render func(*goschema.Database) (atlashclrender.Result, error)
	}{
		{
			name: "the dialect-aware entry point",
			render: func(db *goschema.Database) (atlashclrender.Result, error) {
				return atlashclrender.RenderForDialect(db, platform.Postgres)
			},
		},
		{
			name:   "the plain one",
			render: atlashclrender.Render,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := test.render(inspectedTable(""))

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Not(qt.Contains), "schema \"")
			c.Assert(string(result.Data), qt.Not(qt.Contains), "  schema = ")
		})
	}
}

// TestRenderWritesPermissionBodiesThatEvaluate pins that a permission block is
// written so the pinned binary can read the file it is in.
//
// That binary drops a block whose name it does not model -- it has no
// `permission` block of its own -- but only after the body evaluates. A bare
// PUBLIC or USAGE is an HCL variable reference with nothing behind it, and the
// whole file is refused with `There is no variable named "PUBLIC"`.
//
// Measured on that binary with everything else held fixed:
//
//	to = PUBLIC    privileges = [USAGE]      exit 1
//	to = "PUBLIC"  privileges = [USAGE]      exit 1
//	to = PUBLIC    privileges = ["USAGE"]    exit 1
//	to = "PUBLIC"  privileges = ["USAGE"]    exit 0
//
// so both attributes had to move, and the third row is why quoting only the
// grantee was not enough.
//
// A named role stays a reference: `to = role.app` with the matching `role`
// block present is measured to evaluate on that binary, so quoting it would
// lose a reference and buy nothing.
func TestRenderWritesPermissionBodiesThatEvaluate(t *testing.T) {
	tests := []struct {
		name string
		role string
		want string
	}{
		{
			name: "PUBLIC is a quoted string, not a variable",
			role: "PUBLIC",
			want: "  to = \"PUBLIC\"\n",
		},
		{
			name: "a lower-cased spelling of it too",
			role: "public",
			want: "  to = \"PUBLIC\"\n",
		},
		{
			name: "a named role stays a reference",
			role: "app",
			want: "  to = role.app\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("public")
			db.Grants = []goschema.Grant{{
				Role:       test.role,
				OnSchema:   "public",
				Privileges: []string{"USAGE"},
			}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
			c.Assert(string(result.Data), qt.Contains, "  privileges = [\"USAGE\"]\n")
		})
	}
}

// TestRenderedPermissionRoundTrips pins that quoting cost nothing on Ptah's own
// side: the parser reads the quoted spelling back to the same grant.
//
// Without this the change would be measured only against the other binary, and
// a rendering Ptah itself could no longer read would still look like progress.
func TestRenderedPermissionRoundTrips(t *testing.T) {
	tests := []struct {
		name string
		role string
	}{
		{name: "PUBLIC", role: "PUBLIC"},
		{name: "a named role", role: "app"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("public")
			db.Roles = []goschema.Role{{Name: "app"}}
			db.Grants = []goschema.Grant{{
				Role:       test.role,
				OnSchema:   "public",
				Privileges: []string{"USAGE", "CREATE"},
			}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Grants, qt.HasLen, 1)
			c.Assert(parsed.Grants[0].Role, qt.Equals, test.role)
			c.Assert(parsed.Grants[0].Privileges, qt.DeepEquals, []string{"USAGE", "CREATE"})
			c.Assert(parsed.Grants[0].OnSchema, qt.Equals, "public")
		})
	}
}

// inspectedTable builds the IR a database read produces for one table, with the
// schema the reader reported -- which is nothing at all wherever the engine
// treats it as implicit.
func inspectedTable(schema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t", Schema: schema}},
		Fields: []goschema.Field{{StructName: "T", Name: "id", Type: "integer"}},
	}
}
