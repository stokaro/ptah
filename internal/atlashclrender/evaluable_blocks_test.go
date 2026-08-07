package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderWritesEveryBlockBodyAsEvaluableHCL pins that no attribute Ptah
// writes is a bare word in an attribute position (stokaro/ptah#1251).
//
// A bare word there is an HCL variable reference with nothing behind it, and
// one of them makes the pinned Atlas community binary v1.3.0 refuse the WHOLE
// document -- Ptah's inspect of a PostgreSQL 17 database carrying all of these
// objects died on the first, `There is no variable named "bigint"`, and said
// nothing about the other five. Same class as the `to = PUBLIC` /
// `privileges = [USAGE]` defect fixed in #1248.
//
// Each row was measured against that binary by varying THAT attribute alone in
// a document the binary otherwise accepts, because peeling errors one at a time
// only ever identifies the first one. The per-attribute results are recorded
// beside the code that renders them, in objects.go and render.go.
//
// notWant carries the bare spelling for the same reason the inverse mutant
// exists elsewhere: `qt.Contains` on the quoted form alone still passes if the
// renderer emits both, or emits the bare form somewhere else in the block.
func TestRenderWritesEveryBlockBodyAsEvaluableHCL(t *testing.T) {
	tests := []struct {
		name    string
		db      func() *goschema.Database
		want    []string
		notWant []string
	}{
		{
			// `type = bigint` -> There is no variable named "bigint".
			name: "a sequence type",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Sequences = []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}}
				return db
			},
			want:    []string{"\n  type = \"bigint\"\n"},
			notWant: []string{"\n  type = bigint\n"},
		},
		{
			// lang/return/security/volatility were each refused on their own.
			// `return` is the one that looks like a type position and is not:
			// a bare `bigint` evaluates as a column type and fails here.
			name: "every function attribute",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Functions = []goschema.Function{{
					Name:       "touch_users",
					Language:   "plpgsql",
					Returns:    "trigger",
					Security:   "INVOKER",
					Volatility: "VOLATILE",
					Body:       "BEGIN RETURN NEW; END;",
				}}
				return db
			},
			want: []string{
				"  lang = \"PLpgSQL\"\n",
				"  return = \"trigger\"\n",
				"  security = \"INVOKER\"\n",
				"  volatility = \"VOLATILE\"\n",
			},
			notWant: []string{
				"\n  lang = PLpgSQL\n",
				"\n  return = trigger\n",
				"\n  security = INVOKER\n",
				"\n  volatility = VOLATILE\n",
			},
		},
		{
			// `for = ROW` -> There is no variable named "ROW".
			name: "a trigger for-each",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Triggers = []goschema.Trigger{{
					Name:    "users_touch",
					Table:   "users",
					Timing:  "BEFORE",
					Event:   "UPDATE",
					ForEach: "ROW",
					Body:    "BEGIN RETURN NEW; END;",
				}}
				return db
			},
			want:    []string{"\n  for = \"ROW\"\n"},
			notWant: []string{"\n  for = ROW\n"},
		},
		{
			// The default is rendered through the same attribute, so an IR that
			// carries no ForEach must not reintroduce the bare word.
			name: "a trigger for-each Ptah defaults",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Triggers = []goschema.Trigger{{
					Name:   "users_touch",
					Table:  "users",
					Timing: "BEFORE",
					Event:  "UPDATE",
					Body:   "BEGIN RETURN NEW; END;",
				}}
				return db
			},
			want:    []string{"\n  for = \"ROW\"\n"},
			notWant: []string{"\n  for = ROW\n"},
		},
		{
			// `for = ALL` -> There is no variable named "ALL".
			name: "a policy for",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Roles = []goschema.Role{{Name: "rdr"}}
				db.RLSPolicies = []goschema.RLSPolicy{{
					Name:            "users_read",
					Table:           "users",
					PolicyFor:       "ALL",
					ToRoles:         "rdr",
					UsingExpression: "true",
				}}
				return db
			},
			want:    []string{"\n  for = \"ALL\"\n"},
			notWant: []string{"\n  for = ALL\n"},
		},
		{
			// Not a bare word, a missing one: an enum block with no schema is
			// refused with `extract schema name from enum reference`.
			name: "an enum block declares its schema",
			db:   inspectedEnumTable,
			want: []string{"enum \"status\" {\n  schema = schema.public\n"},
		},
		{
			// `type = sql("status")` names a type the dev database does not
			// have; only the enum block creates it there.
			name: "an enum-typed column references the enum block",
			db:   inspectedEnumTable,
			want: []string{"    type = enum.status\n"},
			notWant: []string{
				"\n    type = sql(\"status\")\n",
				"\n    type = status\n",
				"\n    type = \"status\"\n",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db(), platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			rendered := string(result.Data)
			for _, want := range test.want {
				c.Assert(rendered, qt.Contains, want, qt.Commentf("rendered:\n%s", rendered))
			}
			for _, notWant := range test.notWant {
				c.Assert(rendered, qt.Not(qt.Contains), notWant, qt.Commentf("rendered:\n%s", rendered))
			}
		})
	}
}

// TestEvaluableBlocksRoundTripThroughPtahsOwnParser pins that quoting cost
// nothing on Ptah's own side.
//
// Ptah reads these blocks back, so a rendering it can no longer parse would be
// a regression wearing the costume of a fix. Without this the change would be
// measured only against the other binary. Three of the six attributes here
// (sequence type, policy for, and the enum schema) sit in blocks the pinned
// binary refuses as a feature gap whatever they say, so Ptah's own parser is
// the only thing that can tell those rows apart at all.
func TestEvaluableBlocksRoundTripThroughPtahsOwnParser(t *testing.T) {
	tests := []struct {
		name  string
		db    func() *goschema.Database
		check func(*qt.C, *goschema.Database)
	}{
		{
			name: "a sequence keeps its type",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Sequences = []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}}
				return db
			},
			check: func(c *qt.C, parsed *goschema.Database) {
				c.Assert(parsed.Sequences, qt.HasLen, 1)
				c.Assert(parsed.Sequences[0].AsType, qt.Equals, "bigint")
			},
		},
		{
			name: "a function keeps all four attributes",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Functions = []goschema.Function{{
					Name:       "touch_users",
					Language:   "plpgsql",
					Returns:    "trigger",
					Security:   "INVOKER",
					Volatility: "VOLATILE",
					Body:       "BEGIN RETURN NEW; END;",
				}}
				return db
			},
			check: func(c *qt.C, parsed *goschema.Database) {
				c.Assert(parsed.Functions, qt.HasLen, 1)
				function := parsed.Functions[0]
				// Canonicalize because the IR spells the language lowercase and
				// the rendered attribute carries Atlas's mixed-case name. That
				// detour predates this change -- the bare word was `PLpgSQL`
				// too -- and Canonicalize is what every consumer of the IR
				// already applies.
				function.Canonicalize()
				c.Assert(function.Language, qt.Equals, "plpgsql")
				c.Assert(function.Returns, qt.Equals, "trigger")
				c.Assert(function.Security, qt.Equals, "INVOKER")
				c.Assert(function.Volatility, qt.Equals, "VOLATILE")
			},
		},
		{
			name: "a trigger keeps its for-each",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Triggers = []goschema.Trigger{{
					Name:    "users_touch",
					Table:   "users",
					Timing:  "BEFORE",
					Event:   "UPDATE",
					ForEach: "ROW",
					Body:    "BEGIN RETURN NEW; END;",
				}}
				return db
			},
			check: func(c *qt.C, parsed *goschema.Database) {
				c.Assert(parsed.Triggers, qt.HasLen, 1)
				c.Assert(parsed.Triggers[0].ForEach, qt.Equals, "ROW")
				c.Assert(parsed.Triggers[0].Timing, qt.Equals, "BEFORE")
			},
		},
		{
			name: "a policy keeps its for",
			db: func() *goschema.Database {
				db := inspectedEnumTable()
				db.Roles = []goschema.Role{{Name: "rdr"}}
				db.RLSPolicies = []goschema.RLSPolicy{{
					Name:            "users_read",
					Table:           "users",
					PolicyFor:       "ALL",
					ToRoles:         "rdr",
					UsingExpression: "true",
				}}
				return db
			},
			check: func(c *qt.C, parsed *goschema.Database) {
				c.Assert(parsed.RLSPolicies, qt.HasLen, 1)
				c.Assert(parsed.RLSPolicies[0].PolicyFor, qt.Equals, "ALL")
				c.Assert(parsed.RLSPolicies[0].ToRoles, qt.Equals, "rdr")
			},
		},
		{
			// The reference spelling is the one the parser already understood:
			// columnTypeName strips the `enum.` prefix to the enum's name and
			// reports the type as NOT raw SQL, so a second render writes the
			// reference again rather than falling back to sql().
			name: "an enum-typed column keeps its type and stops being raw SQL",
			db:   inspectedEnumTable,
			check: func(c *qt.C, parsed *goschema.Database) {
				c.Assert(parsed.Enums, qt.HasLen, 1)
				c.Assert(parsed.Enums[0].Values, qt.DeepEquals, []string{"active", "inactive"})
				state := fieldByName(parsed.Fields, "state")
				c.Assert(state.Type, qt.Equals, "status")
				c.Assert(state.TypeRawSQL, qt.IsFalse)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(test.db(), platform.Postgres, "public")
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

			c.Assert(err, qt.IsNil, qt.Commentf("rendered:\n%s", result.Data))
			test.check(c, parsed)
		})
	}
}

// TestRenderedSchemaIsStableAcrossAParseAndRerender pins the property the
// field-by-field round trip cannot state on its own: what Ptah reads back
// renders to the same bytes.
//
// A per-attribute assertion passes even when the second render moves an
// attribute, drops one the parser silently discarded, or re-decides a column
// type from a different branch. This asserts the whole document instead, on the
// fixture that carries every construct this change touched.
//
// The comparison starts at the SECOND render, not the first, and that is a
// concession to a defect this change does not fix rather than a weakening of
// the property. Parsing resolves a trigger's `on = table.users` to the table's
// qualified name, so the next render writes `on = table.public.users` and every
// later one repeats it. That spelling is refused by the pinned Atlas community
// binary v1.3.0 -- `Unsupported attribute; This object does not have an
// attribute named "public"` -- and its own cross-schema references are
// unqualified: `ref_columns = [table.users.column.id]` reaches a table in
// another schema at exit 0 while the qualified spelling of the same reference
// is refused. That is a separate rendering defect in objectRef, in a different
// attribute from the six this change is about, and it belongs to whoever fixes
// it rather than being papered over here. Every attribute this change DID touch
// is already at its fixed point by the first render, so the rows above cover
// them from byte one.
func TestRenderedSchemaIsStableAcrossAParseAndRerender(t *testing.T) {
	c := qt.New(t)

	first, err := atlashclrender.RenderInspected(everyTouchedConstruct(), platform.Postgres, "public")
	c.Assert(err, qt.IsNil)

	parsedOnce, err := atlashcl.Parse(first.Data, "rendered.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered:\n%s", first.Data))
	second, err := atlashclrender.RenderInspected(parsedOnce, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)

	parsedTwice, err := atlashcl.Parse(second.Data, "rendered.hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("rendered:\n%s", second.Data))
	third, err := atlashclrender.RenderInspected(parsedTwice, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(third.Data), qt.Equals, string(second.Data))
	// The attributes this change owns survive from the first render, so the
	// fixed-point framing above cannot be hiding a regression in one of them.
	for _, want := range []string{
		"  type = \"bigint\"\n",
		"  lang = \"PLpgSQL\"\n",
		"  return = \"trigger\"\n",
		"  security = \"INVOKER\"\n",
		"  volatility = \"VOLATILE\"\n",
		"  for = \"ROW\"\n",
		"  for = \"ALL\"\n",
		"    type = enum.status\n",
		"  schema = schema.public\n",
	} {
		c.Assert(string(first.Data), qt.Contains, want)
		c.Assert(string(third.Data), qt.Contains, want)
	}
}

// TestEnumTypedColumnKeepsANameThatIsAlsoAnotherDeclaration pins that the enum
// reference is not written for a type name a domain, composite or range in the
// same document also declares.
//
// Those are separate blocks, and `enum.status` would point at the wrong one.
// The name collision is the only thing that separates these rows from the enum
// row above, so each keeps everything else fixed.
func TestEnumTypedColumnKeepsANameThatIsAlsoAnotherDeclaration(t *testing.T) {
	tests := []struct {
		name    string
		declare func(*goschema.Database)
	}{
		{
			name: "a domain",
			declare: func(db *goschema.Database) {
				db.Domains = []goschema.Domain{{Name: "status", BaseType: "text"}}
			},
		},
		{
			name: "a composite type",
			declare: func(db *goschema.Database) {
				db.CompositeTypes = []goschema.CompositeType{{
					Name:   "status",
					Fields: []goschema.CompositeTypeField{{Name: "a", Type: "text"}},
				}}
			},
		},
		{
			name: "a range type",
			declare: func(db *goschema.Database) {
				db.Ranges = []goschema.Range{{Name: "status", Subtype: "int4"}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := inspectedEnumTable()
			test.declare(db)

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Not(qt.Contains), "type = enum.status\n")
		})
	}
}

// TestRenderInspectedDeclaresTheSchemaAnEnumOnlyReadReferences pins that the
// enum's new schema reference always has a block to resolve to.
//
// referencedSchemas used to return nothing when the read matched no table,
// which is right for a document with nothing in it and wrong the moment an enum
// block starts naming a schema: the reference would dangle, and a dangling
// `schema.public` is refused by the pinned binary just as a missing one is.
func TestRenderInspectedDeclaresTheSchemaAnEnumOnlyReadReferences(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{Enums: []goschema.Enum{{Name: "status", Values: []string{"active"}}}}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "schema \"public\" {\n}\n")
	c.Assert(string(result.Data), qt.Contains, "  schema = schema.public\n")
}

// inspectedEnumTable builds the IR a PostgreSQL read produces for a table with
// an enum-typed column: the enum carries no schema of its own, and the column
// carries the type name with no record of how it was written.
func inspectedEnumTable() *goschema.Database {
	return &goschema.Database{
		Enums:  []goschema.Enum{{Name: "status", Values: []string{"active", "inactive"}}},
		Tables: []goschema.Table{{StructName: "Users", Name: "users"}},
		Fields: []goschema.Field{
			{StructName: "Users", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Users", Name: "state", Type: "status"},
		},
	}
}

// everyTouchedConstruct is inspectedEnumTable plus one of every block this
// change re-spelled, so the stability assertion covers all of them at once.
func everyTouchedConstruct() *goschema.Database {
	db := inspectedEnumTable()
	db.Roles = []goschema.Role{{Name: "rdr"}}
	db.Sequences = []goschema.Sequence{{Name: "order_seq", AsType: "bigint"}}
	db.Functions = []goschema.Function{{
		Name:       "touch_users",
		Language:   "plpgsql",
		Returns:    "trigger",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "BEGIN RETURN NEW; END;",
	}}
	db.Triggers = []goschema.Trigger{{
		Name:    "users_touch",
		Table:   "users",
		Timing:  "BEFORE",
		Event:   "UPDATE",
		ForEach: "ROW",
		Body:    "BEGIN RETURN NEW; END;",
	}}
	db.RLSPolicies = []goschema.RLSPolicy{{
		Name:            "users_read",
		Table:           "users",
		PolicyFor:       "ALL",
		ToRoles:         "rdr",
		UsingExpression: "true",
	}}
	return db
}
