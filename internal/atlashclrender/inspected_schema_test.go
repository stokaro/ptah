package atlashclrender_test

import (
	"slices"
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

// TestRenderNamesTheBlockTypeTheDocumentDeclares pins that a reference to a
// relation spells the block type the same document writes for it
// (stokaro/ptah#1234).
//
// A reference in HCL names a BLOCK, and the block type is the first word of the
// traversal, so `table.v` reads as "the v attribute of the table object" and
// resolves to nothing where the document declares `view "v"`. Measured on the
// pinned Atlas community binary v1.3.0 against the document
// `ptah-compat schema inspect` writes for
//
//	CREATE TABLE t (id integer PRIMARY KEY);
//	CREATE VIEW v AS SELECT id FROM t;
//
// one operand varied and nothing else touched:
//
//	for = table.v   exit 1  Unsupported attribute; This object does not have
//	                        an attribute named "v"
//	for = view.v    exit 0
//	for = "v"       exit 0
//
// and on a document declaring `materialized "mv"`, the same pair:
//
//	for = table.mv         exit 1  ... an attribute named "mv"
//	for = materialized.mv  exit 0
//
// That is the DEFAULT invocation on any database carrying a view, with no
// selection involved: PostgreSQL reports the owner's implicit privileges on a
// view exactly as it does on a table, so the grant reaches the renderer in
// Grant.OnTable and the field cannot be what picks the word.
//
// The last two rows are the controls that keep this from being "call anything
// with a view in the IR a view". A reference is only allowed to name what this
// render actually WRITES, so a view the render drops is not a block to name,
// and a name the document declares nothing under is not one either. What those
// two write instead is a QUOTED name, because a traversal to a block that is
// not there is refused whichever word it starts with -- measured on the same
// binary, `for = table.gone` at exit 1 `Unsupported attribute; This object does
// not have an attribute named "gone"` against `for = "gone"` at exit 0, and
// with no table block in the document at all the same traversal is refused with
// `Unknown variable; There is no variable named "table"`.
func TestRenderNamesTheBlockTypeTheDocumentDeclares(t *testing.T) {
	tests := []struct {
		name string
		db   func() *goschema.Database
		want []string
	}{
		{
			name: "a grant on a table names the table block",
			db: func() *goschema.Database {
				return relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "t", Privileges: []string{"SELECT"},
				})
			},
			want: []string{"  for = table.t\n"},
		},
		{
			name: "a grant on a view names the view block",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
				})
				db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
				return db
			},
			want: []string{"view \"v\" {\n", "  for = view.v\n"},
		},
		{
			name: "a grant on a materialized view names the materialized block",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "mv", Privileges: []string{"SELECT"},
				})
				db.MaterializedViews = []goschema.MaterializedView{{Name: "mv", Body: "SELECT id FROM t"}}
				return db
			},
			want: []string{"materialized \"mv\" {\n", "  for = materialized.mv\n"},
		},
		{
			// A trigger's target is a relation too: `INSTEAD OF` triggers only
			// exist on views, and Trigger.Table is where the reader puts one.
			// Measured on the same binary, `on = table.v` refused with the same
			// message and `on = view.v` read at exit 0.
			name: "a trigger on a view names the view block",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "t", Privileges: []string{"SELECT"},
				})
				db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
				db.Triggers = []goschema.Trigger{{
					Name: "v_ins", Table: "v", Timing: "INSTEAD OF", Event: "INSERT",
					ForEach: "ROW", Body: "RETURN NEW;",
				}}
				return db
			},
			want: []string{"  on = view.v\n"},
		},
		{
			name: "a view this render drops is not a block to name",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
				})
				db.Views = []goschema.View{{Name: "v"}}
				return db
			},
			want: []string{"  for = \"v\"\n"},
		},
		{
			// A read puts a sequence grant in OnTable -- the catalog reports it
			// through the same table-grant path -- while a Go annotation puts it
			// in OnSequence. One reference, so one spelling: both name the block
			// the document declares.
			name: "a grant on a sequence names the sequence block whichever field it arrived in",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "order_seq", Privileges: []string{"SELECT"},
				})
				db.Grants = append(db.Grants, goschema.Grant{
					Role: "app", OnSequence: "order_seq", Privileges: []string{"USAGE"},
				})
				db.Sequences = []goschema.Sequence{{Name: "order_seq"}}
				return db
			},
			want: []string{
				"sequence \"order_seq\" {\n",
				"  for = sequence.order_seq\n  privileges = [\"SELECT\"]\n",
				"  for = sequence.order_seq\n  privileges = [\"USAGE\"]\n",
			},
		},
		{
			name: "a name the document declares nothing under is quoted",
			db: func() *goschema.Database {
				return relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "gone", Privileges: []string{"SELECT"},
				})
			},
			want: []string{"  for = \"gone\"\n"},
		},
		{
			// The shape the review of #1303 refuted: two schemas each holding a
			// relation of one name. The label is declared TWICE, so no traversal
			// names one of them in particular -- `view.v` evaluates on the pinned
			// binary and means neither, and Ptah's own reader drops the schema
			// for a label it finds twice -- while `table.other.v` and
			// `view.other.v` are both refused with `This object does not have an
			// attribute named "other"`. `for = "other.v"` is read at exit 0 and
			// keeps the schema.
			name: "a label two schemas both declare is a quoted qualified name",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "other.v", Privileges: []string{"SELECT"},
				})
				db.Schemas = append(db.Schemas, goschema.Schema{Name: "other"})
				db.Views = []goschema.View{
					{Name: "v", Body: "SELECT id FROM t"},
					{Name: "other.v", Body: "SELECT id FROM other.t"},
				}
				db.Grants = append(db.Grants, goschema.Grant{
					Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
				})
				return db
			},
			want: []string{
				"  for = \"other.v\"\n",
				"  for = \"v\"\n",
			},
		},
		{
			// Same document, the other position that can name a relation.
			name: "a trigger on a label two schemas both declare is a quoted qualified name",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "t", Privileges: []string{"SELECT"},
				})
				db.Schemas = append(db.Schemas, goschema.Schema{Name: "other"})
				db.Views = []goschema.View{
					{Name: "v", Body: "SELECT id FROM t"},
					{Name: "other.v", Body: "SELECT id FROM other.t"},
				}
				db.Triggers = []goschema.Trigger{{
					Name: "v_ins", Table: "other.v", Timing: "INSTEAD OF", Event: "INSERT",
					ForEach: "ROW", Body: "RETURN NEW;",
				}}
				return db
			},
			want: []string{"  on = \"other.v\"\n"},
		},
		{
			// The fallback is the position's own guess, and for a sequence
			// target that is `sequence`, not `table`. A reference to a block the
			// document does not declare is unreadable either way, so the one
			// that says what the object IS is the one to write.
			name: "a sequence the document declares no block for keeps the sequence spelling",
			db: func() *goschema.Database {
				return relationTargetDocument(goschema.Grant{
					Role: "app", OnSequence: "gone_seq", Privileges: []string{"USAGE"},
				})
			},
			want: []string{"  for = sequence.gone_seq\n"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db(), platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(string(result.Data), qt.Contains, want)
			}
		})
	}
}

// TestRenderedRelationTargetRoundTrips pins that Ptah reads back every block
// type its own renderer can now write into a target position.
//
// Without this the change would be measured only against the other binary, and
// a document Ptah could no longer read would still look like progress -- which
// is exactly what emitting `view.<name>` without teaching the parser would be.
func TestRenderedRelationTargetRoundTrips(t *testing.T) {
	tests := []struct {
		name   string
		db     func() *goschema.Database
		target string
	}{
		{
			// All three targets come back qualified, and that uniformity is
			// the point. goschema.Finalize reads a grant's schema off the block
			// it names, and an inspected render now attributes a view and a
			// materialized view to the read's schema exactly as it has always
			// attributed a table. Before stokaro/ptah#1138 only this row was
			// qualified; the other two came back bare, which was the asymmetry
			// rather than the rule.
			name: "a table",
			db: func() *goschema.Database {
				return relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "t", Privileges: []string{"SELECT"},
				})
			},
			target: "public.t",
		},
		{
			name: "a view",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
				})
				db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
				return db
			},
			target: "public.v",
		},
		{
			name: "a materialized view",
			db: func() *goschema.Database {
				db := relationTargetDocument(goschema.Grant{
					Role: "app", OnTable: "mv", Privileges: []string{"SELECT"},
				})
				db.MaterializedViews = []goschema.MaterializedView{{Name: "mv", Body: "SELECT id FROM t"}}
				return db
			},
			target: "public.mv",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db(), platform.Postgres, "public")
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Grants, qt.HasLen, 1)
			c.Assert(parsed.Grants[0].OnTable, qt.Equals, test.target)
			c.Assert(parsed.Grants[0].Role, qt.Equals, "app")
			c.Assert(parsed.Grants[0].Privileges, qt.DeepEquals, []string{"SELECT"})
		})
	}
}

// TestRenderedTriggerOnAViewRoundTrips is the same round trip for the other
// position that can name one.
func TestRenderedTriggerOnAViewRoundTrips(t *testing.T) {
	c := qt.New(t)

	db := relationTargetDocument(goschema.Grant{
		Role: "app", OnTable: "t", Privileges: []string{"SELECT"},
	})
	db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
	db.Triggers = []goschema.Trigger{{
		Name: "v_ins", Table: "v", Timing: "INSTEAD OF", Event: "INSERT",
		ForEach: "ROW", Body: "RETURN NEW;",
	}}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "  on = view.v\n")

	parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	// Qualified, because an inspected render attributes the view block to the
	// read's schema the way it has always attributed a table block. A trigger
	// on an inspected TABLE has always come back `public.t`; the bare `v` this
	// line used to expect was the view being the odd one out
	// (stokaro/ptah#1138).
	c.Assert(parsed.Triggers[0].Table, qt.Equals, "public.v")
}

// TestRenderedRelationTargetKeepsItsSchema pins that shortening the reference
// costs nothing on Ptah's own side in a MULTI-SCHEMA document, which is the
// only place there is a schema to lose.
//
// A reference names a block by its labels, so `view.v` is the only spelling the
// pinned Atlas community binary v1.3.0 reads -- measured on a two-schema
// PostgreSQL 17 inspect, `for = table.other.v` refused with
// `This object does not have an attribute named "other"` and `for = view.v`
// read at exit 0. goschema.Finalize restores a grant's schema off a TABLE block
// and says in its own closing note that it does nothing for views, so the
// restore for these lives in the HCL reader beside the other two positions the
// same note leaves out.
//
// The second row is the one an inspected read produces for its own schema. The
// grant goes in bare, because a catalog reports the read's own schema as
// implicit, and comes back naming that schema -- exactly as the same document's
// TABLE target always has (stokaro/ptah#1138). Separating the written target
// from the expected one is what makes the row an assertion about the restore
// rather than an echo.
func TestRenderedRelationTargetKeepsItsSchema(t *testing.T) {
	tests := []struct {
		name        string
		viewName    string
		grantTarget string
		wantTarget  string
	}{
		{
			name:        "a view outside the default schema",
			viewName:    "other.v",
			grantTarget: "other.v",
			wantTarget:  "other.v",
		},
		{
			name:        "a view the catalog reported without a schema",
			viewName:    "v",
			grantTarget: "v",
			wantTarget:  "public.v",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := relationTargetDocument(goschema.Grant{
				Role: "app", OnTable: test.grantTarget, Privileges: []string{"SELECT"},
			})
			db.Views = []goschema.View{{Name: test.viewName, Body: "SELECT id FROM t"}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, "  for = view.v\n")

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Grants, qt.HasLen, 1)
			c.Assert(parsed.Grants[0].OnTable, qt.Equals, test.wantTarget)
		})
	}
}

// TestRenderedDuplicateRelationLabelRoundTrips is the round trip for the shape
// no traversal can spell: two schemas each declaring a relation of one name.
//
// It is the half that keeps the quoted fallback from being a way to make the
// pinned binary happy at Ptah's expense. `view.v` would evaluate there and mean
// neither block, and relationBlockSchema in internal/atlashcl returns nothing
// for a label it finds twice -- so the short form would come back as a bare `v`
// and the schema would be gone. The quoted qualified name comes back as written,
// on both positions that can name a relation, and the grant on the view in the
// DEFAULT schema is the paired row: it was bare going in and must stay bare, not
// acquire a schema on the way through.
func TestRenderedDuplicateRelationLabelRoundTrips(t *testing.T) {
	c := qt.New(t)

	db := relationTargetDocument(goschema.Grant{
		Role: "app", OnTable: "other.v", Privileges: []string{"SELECT"},
	})
	db.Schemas = append(db.Schemas, goschema.Schema{Name: "other"})
	db.Views = []goschema.View{
		{Name: "v", Body: "SELECT id FROM t"},
		{Name: "other.v", Body: "SELECT id FROM other.t"},
	}
	db.Grants = append(db.Grants, goschema.Grant{
		Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
	})
	db.Triggers = []goschema.Trigger{{
		Name: "v_ins", Table: "other.v", Timing: "INSTEAD OF", Event: "INSERT",
		ForEach: "ROW", Body: "RETURN NEW;",
	}}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	rendered := string(result.Data)
	c.Assert(rendered, qt.Contains, "  for = \"other.v\"\n", qt.Commentf("rendered HCL:\n%s", rendered))
	c.Assert(rendered, qt.Contains, "  for = \"v\"\n", qt.Commentf("rendered HCL:\n%s", rendered))
	c.Assert(rendered, qt.Contains, "  on = \"other.v\"\n", qt.Commentf("rendered HCL:\n%s", rendered))

	parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", rendered))
	c.Assert(parsed.Grants, qt.HasLen, 2)
	c.Assert(grantTargets(parsed.Grants), qt.DeepEquals, []string{"other.v", "v"})
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	c.Assert(parsed.Triggers[0].Table, qt.Equals, "other.v")
}

// grantTargets lists the parsed grant targets in a stable order, so the
// assertion names the same thing however the render ordered the blocks.
func grantTargets(grants []goschema.Grant) []string {
	targets := make([]string, 0, len(grants))
	for _, grant := range grants {
		targets = append(targets, grant.OnTable)
	}
	slices.Sort(targets)
	return targets
}

// TestRenderedSequenceTargetKeepsItsSchema is the same restore for the block
// type a grant reaches through Grant.OnSequence.
func TestRenderedSequenceTargetKeepsItsSchema(t *testing.T) {
	tests := []struct {
		name           string
		sequenceSchema string
		grantTarget    string
		wantTarget     string
	}{
		{
			name:           "a sequence outside the default schema",
			sequenceSchema: "app",
			grantTarget:    "app.order_seq",
			wantTarget:     "app.order_seq",
		},
		{
			// Bare in, because a catalog reports the read's own schema as
			// implicit; qualified out, because the sequence block now names
			// that schema the way the table block always has
			// (stokaro/ptah#1138).
			name:        "a sequence the catalog reported without a schema",
			grantTarget: "order_seq",
			wantTarget:  "public.order_seq",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := relationTargetDocument(goschema.Grant{
				Role: "app", OnSequence: test.grantTarget, Privileges: []string{"USAGE"},
			})
			db.Sequences = []goschema.Sequence{{Name: "order_seq", Schema: test.sequenceSchema}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, "  for = sequence.order_seq\n")

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Grants, qt.HasLen, 1)
			c.Assert(parsed.Grants[0].OnSequence, qt.Equals, test.wantTarget)
		})
	}
}

// relationTargetDocument builds the IR a database read produces for one table
// plus one grant, with no schema anywhere -- which is what a catalog reports
// for the read's own schema.
func relationTargetDocument(grant goschema.Grant) *goschema.Database {
	db := inspectedTable("")
	db.Roles = []goschema.Role{{Name: "app"}}
	db.Grants = []goschema.Grant{grant}
	return db
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
