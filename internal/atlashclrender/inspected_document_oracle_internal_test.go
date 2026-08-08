package atlashclrender

// White-box testing required: this run reuses the oracle harness the other
// conformance runs in this package define -- oracleVersion, requireTypeOracle,
// requireDevURL, schemaNameByDialect, and runReferenceOracle, which writes one
// document to a temp file and returns the binary's output and exit status. A
// private copy would drift from the pinned version constant, and the whole value
// of these runs is that they measure the SAME build.

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// TestOracleReadsTheInspectedDocumentPtahRenders puts a WHOLE inspected
// document to the pinned community binary, which is what stokaro/ptah#1234
// asks for: the column-type half of that issue is measured one column at a
// time by TestOracleModeledColumnTypesMatchTheBinary, and the two halves left
// -- a `permission` body that does not evaluate, and a `schema.<name>`
// reference nothing declares -- are properties of the file as a whole and
// cannot be seen in one attribute.
//
// Each document is rendered through RenderInspectedForAtlasCLI, the entry point
// `ptah-compat schema inspect` calls, so what is measured is the output a user
// gets rather than a fixture written to agree with it.
//
// The `unreadable` rows are what make the `rendered` row a measurement. Each is
// produced FROM the accepted document by one textual substitution, so nothing
// else differs between the pair, and each asserts the binary's MESSAGE and not
// merely a non-zero status: a document refused for an unrelated reason would
// otherwise look like confirmation and this run would keep passing after it had
// stopped measuring anything. Every substitution is asserted to have changed the
// text, so a spelling that stopped being emitted fails the run instead of
// silently mutating nothing.
//
// The second document is the one no prediction reaches. Every PostgreSQL
// database carries `GRANT USAGE ON SCHEMA public TO PUBLIC`, so an EMPTY
// database renders exactly it -- a `permission` block referencing a schema, with
// no table anywhere to declare that schema from.
func TestOracleReadsTheInspectedDocumentPtahRenders(t *testing.T) {
	oracle := requireTypeOracle(t)

	for _, dialect := range slices.Sorted(maps.Keys(schemaNameByDialect)) {
		t.Run(dialect, func(t *testing.T) {
			devURL := requireDevURL(t, dialect)
			schema := schemaNameByDialect[dialect]

			for _, document := range inspectedOracleDocuments {
				t.Run(document.name, func(t *testing.T) {
					c := qt.New(t)

					result, err := RenderInspectedForAtlasCLI(document.db(schema), dialect, schema)
					c.Assert(err, qt.IsNil)
					rendered := string(result.Data)
					for _, want := range document.wantContains(schema) {
						c.Assert(rendered, qt.Contains, want,
							qt.Commentf("the document no longer carries the spelling this row measures:\n%s", rendered))
					}

					t.Run("rendered", func(t *testing.T) {
						c := qt.New(t)

						out, code := runReferenceOracle(c, oracle, devURL, rendered)

						c.Assert(code, qt.Equals, 0,
							qt.Commentf("the binary refuses the document ptah-compat renders on %s: %s\n%s",
								dialect, out, rendered))
					})

					for _, mutation := range document.unreadable(schema) {
						t.Run("unreadable/"+mutation.name, func(t *testing.T) {
							c := qt.New(t)

							mutated := strings.Replace(rendered, mutation.from, mutation.to, 1)
							c.Assert(mutated, qt.Not(qt.Equals), rendered,
								qt.Commentf("substituting %q changed nothing, so this row measures nothing", mutation.from))

							out, code := runReferenceOracle(c, oracle, devURL, mutated)

							c.Assert(code, qt.Not(qt.Equals), 0,
								qt.Commentf("the binary now reads %s on %s; the rule this row guards can go: %s",
									mutation.name, dialect, out))
							c.Assert(out, qt.Contains, mutation.wantMessage,
								qt.Commentf("refused for a reason that is not %s, so this row is not measuring it: %s",
									mutation.name, out))
						})
					}
				})
			}
		})
	}
}

// inspectedOracleMutation is one operand varied against the accepted document.
type inspectedOracleMutation struct {
	name        string
	from        string
	to          string
	wantMessage string
}

// inspectedOracleDocuments are the inspected shapes put to the binary whole.
var inspectedOracleDocuments = []struct {
	name string
	db   func(schema string) *goschema.Database
	// wantContains are the spellings this document exists to measure. They are
	// asserted before the binary runs, so a render that stopped emitting one
	// fails here rather than passing an oracle row that no longer covers it.
	wantContains func(schema string) []string
	unreadable   func(schema string) []inspectedOracleMutation
}{
	{
		name: "a table with grants",
		db:   inspectedOracleTableDocument,
		wantContains: func(schema string) []string {
			return []string{
				fmt.Sprintf("schema %q {\n}\n", schema),
				"  schema = schema." + schema + "\n",
				"  to = \"PUBLIC\"\n",
				"  privileges = [\"USAGE\"]\n",
				"  to = role.app\n",
				"  to = \"reporting\"\n",
			}
		},
		unreadable: func(schema string) []inspectedOracleMutation {
			return []inspectedOracleMutation{
				{
					name:        "a schema reference with no block",
					from:        fmt.Sprintf("schema %q {\n}\n\n", schema),
					to:          "",
					wantMessage: `There is no variable named "schema"`,
				},
				{
					// The document declares `role "app"`, so `role` resolves to
					// an object and the missing label is reported as a missing
					// attribute of it. The other spelling of the same defect --
					// no role block anywhere, which is what excluding roles
					// leaves -- is the third document below.
					name:        "a grantee reference to a role this document does not declare",
					from:        "to = \"reporting\"",
					to:          "to = role.reporting",
					wantMessage: `This object does not have an attribute named "reporting"`,
				},
				{
					name:        "PUBLIC written bare",
					from:        "to = \"PUBLIC\"",
					to:          "to = PUBLIC",
					wantMessage: `There is no variable named "PUBLIC"`,
				},
				{
					name:        "a privilege written bare",
					from:        "privileges = [\"USAGE\"]",
					to:          "privileges = [USAGE]",
					wantMessage: `There is no variable named "USAGE"`,
				},
			}
		},
	},
	{
		// What `ptah-compat schema inspect --exclude '*[type=role]'` renders:
		// the role blocks are gone and every grant to them is still here,
		// because a grant is a child of the object granted on rather than of the
		// grantee. This is the shape the `role` traversal used to be written in
		// unconditionally, and the one the pinned binary answered with
		// `There is no variable named "role"`.
		name: "a table whose roles were excluded",
		db: func(schema string) *goschema.Database {
			db := inspectedOracleTableDocument(schema)
			db.Roles = nil
			return db
		},
		wantContains: func(_ string) []string {
			return []string{"  to = \"app\"\n", "  to = \"reporting\"\n"}
		},
		unreadable: func(_ string) []inspectedOracleMutation {
			return []inspectedOracleMutation{
				{
					name:        "a grantee reference with no role block anywhere",
					from:        "to = \"app\"",
					to:          "to = role.app",
					wantMessage: `There is no variable named "role"`,
				},
			}
		},
	},
	{
		// What `ptah-compat schema inspect` renders for a database carrying a
		// VIEW, with no selection and no hand-editing: PostgreSQL reports the
		// owner's implicit privileges on a view exactly as it does on a table,
		// so the grant arrives in Grant.OnTable and the target used to be
		// written `for = table.v` against a document declaring `view "v"`.
		//
		// This is the reachable instance the other three miss. They are all
		// documents a filter left behind; this one is the DEFAULT invocation on
		// an ordinary database.
		name: "a table and a view with grants on both",
		db:   inspectedOracleViewDocument,
		wantContains: func(_ string) []string {
			return []string{"view \"v\" {\n", "  for = table.t\n", "  for = view.v\n"}
		},
		unreadable: func(_ string) []inspectedOracleMutation {
			return []inspectedOracleMutation{
				{
					// The whole refutation of the first version of this fix, as
					// one substitution: the block type is part of the spelling,
					// so naming a view under `table` resolves to nothing. The
					// document declares `table "t"`, so `table` is an object and
					// the missing label is reported as a missing attribute of
					// it -- the same message the sibling rows use, for the same
					// reason.
					name:        "a view named as a table",
					from:        "for = view.v",
					to:          "for = table.v",
					wantMessage: `This object does not have an attribute named "v"`,
				},
			}
		},
	},
	{
		name: "nothing but the grant every database has",
		db:   inspectedOracleGrantOnlyDocument,
		wantContains: func(schema string) []string {
			return []string{
				fmt.Sprintf("schema %q {\n}\n", schema),
				"  for = schema." + schema + "\n",
			}
		},
		unreadable: func(schema string) []inspectedOracleMutation {
			return []inspectedOracleMutation{
				{
					name:        "a schema reference with no block",
					from:        fmt.Sprintf("schema %q {\n}\n\n", schema),
					to:          "",
					wantMessage: `There is no variable named "schema"`,
				},
			}
		},
	},
}

// inspectedOracleTableDocument is the IR a database read produces for a table
// carrying the three grantee shapes that reach a `permission` body.
//
// Nothing declares a schema, which is what a catalog reports: the engine treats
// the read's own schema as implicit and does not repeat it. `reporting` is a
// grantee with no role block, which is what `--exclude '*[type=role]'` leaves
// behind -- grants are children of the object granted on, so excluding roles
// removes the blocks and keeps every reference to them.
func inspectedOracleTableDocument(schema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "integer", Primary: true},
			{StructName: "T", Name: "name", Type: "varchar(100)"},
		},
		Roles: []goschema.Role{{Name: "app"}},
		Grants: []goschema.Grant{
			{Role: "PUBLIC", OnSchema: schema, Privileges: []string{"USAGE"}},
			{Role: "app", OnTable: schema + ".t", Privileges: []string{"SELECT"}, WithOption: true},
			{Role: "reporting", OnTable: schema + ".t", Privileges: []string{"SELECT"}},
		},
	}
}

// inspectedOracleViewDocument adds a view, and a grant on it, to the same IR.
//
// The grant is in OnTable and unqualified, which is what a read produces: the
// catalog reports privileges on a view through the same table-grant path, and
// the reader drops the schema of everything in the read's own schema. So the
// renderer cannot learn the block type from the IR field or from the name -- it
// has to read it off the block the document declares.
func inspectedOracleViewDocument(schema string) *goschema.Database {
	db := inspectedOracleTableDocument(schema)
	db.Views = []goschema.View{{Name: "v", Body: "SELECT id FROM t"}}
	db.Grants = append(db.Grants, goschema.Grant{
		Role: "app", OnTable: "v", Privileges: []string{"SELECT"},
	})
	return db
}

// inspectedOracleGrantOnlyDocument is what an empty PostgreSQL database
// inspects to: one grant on the schema, and nothing else at all.
func inspectedOracleGrantOnlyDocument(schema string) *goschema.Database {
	return &goschema.Database{
		Grants: []goschema.Grant{
			{Role: "PUBLIC", OnSchema: schema, Privileges: []string{"USAGE"}},
		},
	}
}
