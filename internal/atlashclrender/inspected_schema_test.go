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
// A named role stays a reference only where the document declares the matching
// `role` block. The last two rows are that pair, and they are the half the
// original fix asserted in prose and never enforced: a `permission` block is a
// child of the object granted on, so `--exclude '*[type=role]'` takes the role
// blocks away and leaves every grant to them behind. Measured on the same
// binary, one operand varied:
//
//	role "app" declared, to = role.app   exit 0
//	role "app" absent,   to = role.app   exit 1  There is no variable named "role"
//	role "app" absent,   to = "app"      exit 0
func TestRenderWritesPermissionBodiesThatEvaluate(t *testing.T) {
	tests := []struct {
		name  string
		role  string
		roles []goschema.Role
		want  string
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
			name:  "a named role the document declares stays a reference",
			role:  "app",
			roles: []goschema.Role{{Name: "app"}},
			want:  "  to = role.app\n",
		},
		{
			name: "a named role the document does not declare is a string",
			role: "app",
			want: "  to = \"app\"\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("public")
			db.Roles = test.roles
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

// TestRenderInspectedDeclaresTheSchemaAGrantReferences pins the other half of
// the same document: the schema blocks are the ones the body turned out to
// reference, whatever wrote the reference.
//
// Every PostgreSQL database carries `GRANT USAGE ON SCHEMA public TO PUBLIC`,
// so the first row is what an EMPTY database renders as. Its `permission` block
// says `for = schema.public` with nothing to resolve it to, and the pinned
// Atlas community binary v1.3.0 refuses the file with `There is no variable
// named "schema"` -- with no table anywhere to predict the declaration from.
//
// The last row is the control that keeps this from being satisfied by
// declaring the default unconditionally: a document that references no schema
// declares none, which is what the empty-include-selection contract needs.
func TestRenderInspectedDeclaresTheSchemaAGrantReferences(t *testing.T) {
	tests := []struct {
		name        string
		db          func() *goschema.Database
		wantPresent []string
		wantAbsent  []string
	}{
		{
			name: "a grant on the schema is the only thing referencing it",
			db: func() *goschema.Database {
				return &goschema.Database{Grants: []goschema.Grant{{
					Role:       "PUBLIC",
					OnSchema:   "public",
					Privileges: []string{"USAGE"},
				}}}
			},
			wantPresent: []string{"schema \"public\" {\n}\n", "  for = schema.public\n"},
		},
		{
			name: "a table references it as well",
			db: func() *goschema.Database {
				db := inspectedTable("")
				db.Grants = []goschema.Grant{{
					Role:       "PUBLIC",
					OnSchema:   "public",
					Privileges: []string{"USAGE"},
				}}
				return db
			},
			wantPresent: []string{
				"schema \"public\" {\n}\n",
				"  schema = schema.public\n",
				"  for = schema.public\n",
			},
		},
		{
			name: "nothing references it",
			db: func() *goschema.Database {
				return &goschema.Database{Roles: []goschema.Role{{Name: "app"}}}
			},
			wantPresent: []string{"role \"app\" {\n"},
			wantAbsent:  []string{"schema \"public\"", "schema."},
		},
		{
			// A grant conferring no privilege is dropped with a diagnostic, so
			// its target reference is never written -- and a schema block
			// declared for it would be a declaration of nothing.
			//
			// The grantee is present on purpose. The completeness check reads
			// left to right, so a row missing the ROLE would short-circuit
			// before the target is computed and could not tell whether asking
			// for the target had declared anything.
			name: "a grant too incomplete to render",
			db: func() *goschema.Database {
				return &goschema.Database{Grants: []goschema.Grant{{
					Role:     "app",
					OnSchema: "public",
				}}}
			},
			wantAbsent: []string{"schema \"public\"", "schema.", "permission {"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db(), platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantPresent {
				c.Assert(string(result.Data), qt.Contains, want)
			}
			for _, unwanted := range test.wantAbsent {
				c.Assert(string(result.Data), qt.Not(qt.Contains), unwanted)
			}
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
		name  string
		role  string
		roles []goschema.Role
	}{
		{name: "PUBLIC", role: "PUBLIC", roles: []goschema.Role{{Name: "app"}}},
		{name: "a named role", role: "app", roles: []goschema.Role{{Name: "app"}}},
		{
			// The spelling the fix introduces. Quoting is only free if the
			// grantee survives it, and this row is what says it does.
			name: "a named role the document does not declare",
			role: "app",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedTable("public")
			db.Roles = test.roles
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
