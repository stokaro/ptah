package atlashclrender_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// crossSchemaDocument builds a document whose every table reference points at
// `other.users` from `public.posts`, one reference per position that goes
// through the reference renderer.
//
// [withoutTargetTable] takes the referenced block back out. That pair is the
// single thing under test -- everything else is held fixed, so a difference
// between the two renders is attributable to nothing else.
func crossSchemaDocument() *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: "public"}, {Name: "other"}},
		Tables: []goschema.Table{
			{StructName: "Post", Name: "posts", Schema: "public"},
			{StructName: "User", Name: "users", Schema: "other"},
		},
		Fields: []goschema.Field{
			{
				StructName:     "Post",
				Name:           "author_id",
				Type:           "bigint",
				Foreign:        "other.users(id)",
				ForeignKeyName: "posts_author_fk",
			},
			{StructName: "User", Name: "id", Type: "bigint"},
		},
		Triggers: []goschema.Trigger{{
			Name:    "users_touch",
			Table:   "other.users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "RETURN NEW;",
		}},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "users_policy",
			Table:           "other.users",
			PolicyFor:       "SELECT",
			UsingExpression: "true",
		}},
		Roles: []goschema.Role{{Name: "app_user"}},
		Grants: []goschema.Grant{{
			Role:       "app_user",
			Privileges: []string{"SELECT"},
			OnTable:    "other.users",
		}},
		ManagedData: []goschema.ManagedData{{
			Table:  "users",
			Schema: "other",
			Keys:   []string{"id"},
			File:   "seed.csv",
		}},
	}
}

// withoutTargetTable removes the `other.users` block and its column, leaving
// every reference to it in place. That is a filtered export or an orphan
// trigger: the references are still meant, and nothing in the document says
// which schema they meant.
func withoutTargetTable(db *goschema.Database) *goschema.Database {
	db.Tables = slices.DeleteFunc(db.Tables, func(table goschema.Table) bool {
		return table.StructName == "User"
	})
	db.Fields = slices.DeleteFunc(db.Fields, func(field goschema.Field) bool {
		return field.StructName == "User"
	})
	return db
}

// TestRenderShortensTableReferenceWhenDocumentDeclaresTheTarget pins the
// spelling every position must use when the referenced table block is present.
//
// A reference in HCL names a BLOCK, and a block is named by its labels, so the
// schema belongs on the block and not in the reference. Measured on the pinned
// Atlas community binary v1.3.0 (PostgreSQL 17, realm-scope dev URL) against a
// document it accepts, one attribute varied at a time:
//
//	ref_columns = [table.users.column.id]        exit 0
//	ref_columns = [table.other.users.column.id]  exit 1  Unsupported attribute;
//	                                                     ... named "other"
//	on  = table.users        / table.other.users        (trigger)     0 / 1
//	on  = table.users        / table.other.users        (policy)      1 / 1 *
//	for = table.users        / table.other.users        (permission)  0 / 1
//
// (*) A policy never gets past that binary's own feature gap -- `postgres:
// policies are not supported by this version` for the short form against
// `Unsupported attribute` for the long one -- so the reachable target there is
// the same one #1255 used: the message stops being about Ptah's spelling.
//
// `data` is the one position where the spelling is unreachable: that binary
// models `data` as a labeled data source and refuses Ptah's unlabeled block
// with `data block "data" must have exactly 2 labels` before evaluating any
// attribute, identically for both spellings. It is rendered the same way anyway
// because Ptah's own reader is the one that has to agree with itself.
func TestRenderShortensTableReferenceWhenDocumentDeclaresTheTarget(t *testing.T) {
	c := qt.New(t)

	rendered, err := atlashclrender.Render(crossSchemaDocument())

	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	for _, want := range []string{
		`ref_columns = [table.users.column.id]`,
		`on = table.users`,
		`for = table.users`,
		`table = table.users`,
		`table "users" {`,
		`schema = schema.other`,
	} {
		c.Assert(hcl, qt.Contains, want, qt.Commentf("rendered HCL:\n%s", hcl))
	}
	c.Assert(hcl, qt.Not(qt.Contains), "table.other.users",
		qt.Commentf("rendered HCL:\n%s", hcl))
	// Two positions spell `on`, so counting is what says both moved rather than
	// one of them carrying the assertion for both.
	c.Assert(strings.Count(hcl, "on = table.users"), qt.Equals, 2,
		qt.Commentf("rendered HCL:\n%s", hcl))
}

// TestRenderCrossSchemaReferencesRoundTripToTheSameIR is the other half of the
// rule: the short form costs nothing because the schema is read back off the
// block the reference names.
//
// It asserts the IR after a render-and-parse rather than the text, because the
// text is what changed and the IR is what must not.
func TestRenderCrossSchemaReferencesRoundTripToTheSameIR(t *testing.T) {
	c := qt.New(t)
	db := crossSchemaDocument()

	rendered, err := atlashclrender.Render(db)
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", string(rendered.Data)))

	c.Assert(fieldByName(parsed.Fields, "author_id").Foreign, qt.Equals, "other.users(id)")
	c.Assert(parsed.Triggers, qt.HasLen, 1)
	c.Assert(parsed.Triggers[0].Table, qt.Equals, "other.users")
	c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
	c.Assert(parsed.RLSPolicies[0].Table, qt.Equals, "other.users")
	c.Assert(parsed.Grants, qt.HasLen, 1)
	c.Assert(parsed.Grants[0].OnTable, qt.Equals, "other.users")
	c.Assert(parsed.ManagedData, qt.HasLen, 1)
	c.Assert(parsed.ManagedData[0].Table, qt.Equals, "users")
	c.Assert(parsed.ManagedData[0].Schema, qt.Equals, "other")
}

// TestRenderKeepsQualifiedReferenceWhenDocumentCannotResolveIt covers the cases
// where dropping the schema would destroy it or point the reference somewhere
// else. Each row is a document the short form would be wrong for.
//
// The schema is kept in both spellings this rule can write, and which one a
// position gets is decided by what that position is allowed to be. A
// `ref_columns`, a `policy`'s `on` and a `data` block's `table` go on naming a
// table through a traversal: `ref_columns` has to reach a column through the
// same expression, so a quoted name is not a spelling available there at all,
// and the other two are rendered the same way because Ptah's reader has to
// agree with itself. A `permission` target and a `trigger`'s `on` are quoted,
// because the pinned Atlas community binary v1.3.0 evaluates those two without
// resolving them and reads a quoted name at exit 0 where it refuses every
// traversal to a block the document does not declare -- `for = "gone"` exit 0
// against `for = table.gone` exit 1 `Unsupported attribute; This object does
// not have an attribute named "gone"`, and `Unknown variable; There is no
// variable named "table"` where the document declares no table block at all.
func TestRenderKeepsQualifiedReferenceWhenDocumentCannotResolveIt(t *testing.T) {
	tests := []struct {
		name string
		// db returns a document referring to `other.users` from `public.posts`.
		db func() *goschema.Database
		// why records what the short form would cost here.
		why string
	}{
		{
			name: "target table absent",
			db:   func() *goschema.Database { return withoutTargetTable(crossSchemaDocument()) },
			why: "no block carries the schema, so dropping it loses it for good:" +
				" a filtered export or an orphan trigger has nothing to resolve against",
		},
		{
			name: "name declared in two schemas",
			db: func() *goschema.Database {
				db := crossSchemaDocument()
				db.Tables = append(db.Tables, goschema.Table{StructName: "PublicUser", Name: "users", Schema: "public"})
				db.Fields = append(db.Fields, goschema.Field{StructName: "PublicUser", Name: "id", Type: "bigint"})
				return db
			},
			why: "two tables of one name are legal, and the pinned binary refuses the short form" +
				" with `multiple reference tables found for \"users\"` rather than picking one;" +
				" goschema gives up on the same ambiguity, so the schema would be lost silently",
		},
		{
			name: "only a same-named table in another schema",
			db: func() *goschema.Database {
				db := withoutTargetTable(crossSchemaDocument())
				db.Tables = append(db.Tables, goschema.Table{StructName: "PublicUser", Name: "users", Schema: "public"})
				db.Fields = append(db.Fields, goschema.Field{StructName: "PublicUser", Name: "id", Type: "bigint"})
				return db
			},
			why: "the one `users` block this document declares is a DIFFERENT table," +
				" so the short form would resolve, quietly, to the wrong one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := atlashclrender.Render(test.db())

			c.Assert(err, qt.IsNil)
			hcl := string(rendered.Data)
			for _, want := range []string{
				`ref_columns = [table.other.users.column.id]`,
				`on = table.other.users`,
				`table = table.other.users`,
				`on = "other.users"`,
				`for = "other.users"`,
			} {
				c.Assert(hcl, qt.Contains, want, qt.Commentf("%s\nrendered HCL:\n%s", test.why, hcl))
			}
			// Two positions spell `on`, and this rule sends them to different
			// spellings, so counting is what says the policy kept the traversal
			// rather than the trigger's quoted name satisfying both.
			c.Assert(strings.Count(hcl, `on = table.other.users`), qt.Equals, 1,
				qt.Commentf("%s\nrendered HCL:\n%s", test.why, hcl))
		})
	}
}

// TestRenderKeepsQualifiedSequenceTarget is the control on the other axis: the
// rule is about TABLE references only.
//
// A permission naming a sequence keeps that sequence block in the document, and
// the pinned binary refuses any PostgreSQL file declaring one -- `postgres:
// sequences are not supported by this version` -- before a reference is ever
// resolved. So nothing measured says which spelling it would take, and a rule
// applied there would be a guess.
//
// The document deliberately declares a TABLE of the sequence's name in the
// sequence's schema. PostgreSQL could not produce it -- tables and sequences
// share one relation namespace -- and that is the point: without it, a table
// block is never a candidate for this name and the assertion holds no matter
// what the sequence path does. With it, pointing the sequence target at
// [renderer.tableRef] shortens the reference and this test says so.
func TestRenderKeepsQualifiedSequenceTarget(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Schemas:   []goschema.Schema{{Name: "app"}},
		Sequences: []goschema.Sequence{{Name: "order_seq", Schema: "app"}},
		Tables:    []goschema.Table{{StructName: "OrderSeq", Name: "order_seq", Schema: "app"}},
		Fields:    []goschema.Field{{StructName: "OrderSeq", Name: "id", Type: "bigint"}},
		Roles:     []goschema.Role{{Name: "app_user"}},
		Grants: []goschema.Grant{{
			Role:       "app_user",
			Privileges: []string{"USAGE"},
			OnSequence: "app.order_seq",
		}},
	}

	rendered, err := atlashclrender.Render(db)

	c.Assert(err, qt.IsNil)
	hcl := string(rendered.Data)
	c.Assert(hcl, qt.Contains, `for = sequence.app.order_seq`, qt.Commentf("rendered HCL:\n%s", hcl))

	parsed, err := atlashcl.Parse(rendered.Data, "schema.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered HCL:\n%s", hcl))
	c.Assert(parsed.Grants, qt.HasLen, 1)
	c.Assert(parsed.Grants[0].OnSequence, qt.Equals, "app.order_seq")
}
