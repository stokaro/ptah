package atlashclrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// nativeInspectedBlockTypes is every top-level block spelling an inspected
// render emits for [inspectedRichDatabase]. It is the list the native surface
// owes its reader: `ptah schema inspect` describes what Ptah models, and the
// compatibility argument for leaving something out belongs to `ptah-compat`
// alone (stokaro/ptah#1251).
var nativeInspectedBlockTypes = []string{
	"composite \"pair\"",
	"data {",
	"domain \"positive\"",
	"enum \"status\"",
	"extension \"pgcrypto\"",
	"function \"touch_accounts\"",
	"materialized \"account_counts\"",
	"permission {",
	"policy \"accounts_all\"",
	"range \"intrange\"",
	"role \"app_reader\"",
	"schema \"public\"",
	"sequence \"order_seq\"",
	"table \"accounts\"",
	"trigger \"accounts_touch\"",
	"view \"active_accounts\"",
}

// TestRenderInspectedKeepsEveryBlockTypeOnTheNativeSurface fails if a block
// stops being rendered natively.
//
// This is the guard on the other half of stokaro/ptah#1251: the compatibility
// surface omits three block types by default, and nothing about that decision
// may leak into the surface whose job is to describe the database completely. A
// native render that quietly lost a construct would still pass every
// compatibility test in this package, so it is pinned here on its own.
func TestRenderInspectedKeepsEveryBlockTypeOnTheNativeSurface(t *testing.T) {
	for _, block := range nativeInspectedBlockTypes {
		t.Run(block, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(
				inspectedRichDatabase(), platform.Postgres, "public",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, block,
				qt.Commentf("native inspect stopped emitting a block it models"))
		})
	}
}

// TestRenderInspectedForAtlasCLIOmitsTheBlocksTheBinaryRefuses pins the three
// block types the compatibility surface leaves out when nothing names them.
//
// Measured against the pinned Atlas community binary v1.3.0 on PostgreSQL 17,
// starting from a file it accepts and adding one bare block:
//
//	extension "pgcrypto" {}     exit 1  postgres: extensions are not supported by this version
//	sequence "order_seq" {}     exit 1  postgres: sequences are not supported by this version
//	policy "accounts_all" {}    exit 1  postgres: policies are not supported by this version
//
// One such block costs the whole file, so a compatibility surface that emits
// one is not compatible in any useful sense.
func TestRenderInspectedForAtlasCLIOmitsTheBlocksTheBinaryRefuses(t *testing.T) {
	for _, block := range []string{"extension \"", "sequence \"", "policy \""} {
		t.Run(block, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				inspectedRichDatabase(), platform.Postgres, "public",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Not(qt.Contains), block)
		})
	}
}

// TestRenderInspectedForAtlasCLIKeepsEverythingElse is the other direction, and
// it is what keeps the omission surgical.
//
// Every row here is a block type the pinned binary DROPS rather than refuses:
// measured off the same accepted base, each one exits 0, as does an invented
// `wibble "x" {}`. Exit 0 therefore says only "harmless to the file", which is
// the whole test the compatibility surface applies. Omitting these would cost
// the reader a description and buy nothing.
func TestRenderInspectedForAtlasCLIKeepsEverythingElse(t *testing.T) {
	kept := []string{
		"composite \"pair\"",
		"domain \"positive\"",
		"enum \"status\"",
		"function \"touch_accounts\"",
		"materialized \"account_counts\"",
		"permission {",
		"range \"intrange\"",
		"role \"app_reader\"",
		"schema \"public\"",
		"table \"accounts\"",
		"trigger \"accounts_touch\"",
		"view \"active_accounts\"",
	}

	for _, block := range kept {
		t.Run(block, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				inspectedRichDatabase(), platform.Postgres, "public",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, block)
		})
	}
}

// TestRenderInspectedForAtlasCLIReportsEveryOmission pins that nothing is
// dropped quietly, and that the message says how to get the block back.
//
// An inspect output that silently omitted an extension would describe a
// database that does not exist, and the operator reading it has no other way to
// learn what was left out. The omission rides the renderer's existing loss
// diagnostics, which `schema inspect` writes to standard error. Naming the
// environment variable in the message is what makes the capability reachable
// from the place the operator actually meets it.
func TestRenderInspectedForAtlasCLIReportsEveryOmission(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspectedForAtlasCLI(
		inspectedRichDatabase(), platform.Postgres, "public",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "extensions.pgcrypto",
			Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses" +
				" a postgres schema file that declares any extension block, and one of them makes the whole" +
				" document unreadable to it; set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "sequences.order_seq",
			Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses" +
				" a postgres schema file that declares any sequence block, and one of them makes the whole" +
				" document unreadable to it; set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "rls_policies.accounts_all",
			Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses" +
				" a postgres schema file that declares any policy block, and one of them makes the whole" +
				" document unreadable to it; set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models",
		},
	})
}

// TestRenderInspectedNativeReportsNoOmission pins the native counterpart: the
// surface that drops nothing has nothing to disclose, so an operator cannot
// mistake a native inspect for a filtered one.
func TestRenderInspectedNativeReportsNoOmission(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspected(
		inspectedRichDatabase(), platform.Postgres, "public",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}

// TestRenderInspectedForAtlasCLIOmitsNothingOnSQLite pins that the omission is
// scoped to the dialect it was measured on.
//
// The refusal is the PostgreSQL driver's, not the file format's: measured on
// the pinned binary with a SQLite dev database, `extension`, `sequence` and
// `policy` blocks are all accepted at exit 0, dropped exactly like an invented
// `wibble "x" {}`. Suppressing them there would lose description with no reader
// to please.
func TestRenderInspectedForAtlasCLIOmitsNothingOnSQLite(t *testing.T) {
	kept := []string{"extension \"pgcrypto\"", "sequence \"order_seq\"", "policy \"accounts_all\""}

	for _, block := range kept {
		t.Run(block, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				inspectedRichDatabase(), platform.SQLite, "main",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, block)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
		})
	}
}

// TestRenderInspectedForAtlasCLIKeepsABlockTheDocumentNames is the reference
// rule, and the case that forced it is the pinned binary's own defect.
//
// Measured on PostgreSQL 17 for
//
//	CREATE SEQUENCE order_seq;
//	CREATE TABLE orders (id integer NOT NULL DEFAULT nextval('order_seq'::regclass));
//
// that binary's inspect emits `default = sql("nextval('order_seq'::regclass)")`
// and no `sequence` block, exit 0 -- and refuses its own output when it is fed
// back: `pq: relation "order_seq" does not exist`, exit 1. Copying that hole
// would reproduce a defect, and it would take something away that worked:
// Ptah read the same document at exit 0 before any suppression existed.
//
// Each row is one shape in which the surviving document names the object, so a
// scan narrowed to one of them fails the others.
func TestRenderInspectedForAtlasCLIKeepsABlockTheDocumentNames(t *testing.T) {
	tests := []struct {
		name     string
		database func() *goschema.Database
		want     string
		wantPath string
	}{
		{
			name:     "a column default calls nextval on the sequence",
			database: sequenceBackedDefaultDatabase,
			want:     "sequence \"order_seq\"",
			wantPath: "sequences.order_seq",
		},
		{
			name: "a view body selects from the sequence",
			database: func() *goschema.Database {
				db := standaloneSequenceDatabase()
				db.Views = []goschema.View{{
					Name: "next_order",
					Body: "SELECT nextval('order_seq') AS n",
				}}
				return db
			},
			want:     "sequence \"order_seq\"",
			wantPath: "sequences.order_seq",
		},
		{
			name: "a permission block targets the sequence",
			database: func() *goschema.Database {
				db := standaloneSequenceDatabase()
				db.Roles = []goschema.Role{{Name: "app_reader", Inherit: true}}
				// PostgreSQL 17 introspection reports a GRANT on a sequence
				// with the sequence in OnTable, so the rendered `permission`
				// block carries a traversal naming it. Omitting the sequence
				// under it leaves a reference to a block nothing declares.
				db.Grants = []goschema.Grant{{
					Role:       "app_reader",
					OnTable:    "order_seq",
					Privileges: []string{"USAGE"},
				}}
				return db
			},
			want:     "sequence \"order_seq\"",
			wantPath: "sequences.order_seq",
		},
		{
			// citext is the easy case and, on its own, a false comfort: the
			// extension and the type it supplies are spelled the same word, so
			// matching the extension's own LABEL answers correctly by accident.
			// The two rows below separate the label from what the extension
			// supplies, which is what this rule actually has to get right.
			name: "a column type is the type the extension supplies",
			database: func() *goschema.Database {
				db := standaloneSequenceDatabase()
				db.Extensions = []goschema.Extension{{Name: "citext", Provides: []string{"citext"}}}
				db.Fields = append(db.Fields, goschema.Field{
					StructName: "Order", Name: "label", Type: "citext",
				})
				return db
			},
			want:     "extension \"citext\"",
			wantPath: "extensions.citext",
		},
		{
			// The regression this front exists for. `isn` supplies the type
			// `isbn`; the word "isn" appears nowhere in the document, so the
			// label rule omitted the extension and left `code isbn` behind.
			// Measured on PostgreSQL 17.10: neither Ptah nor the pinned binary
			// could then read the result -- `type "isbn" does not exist`, exit 1
			// from both -- so the omission was not even a compatibility win.
			name: "a column type is supplied by an extension spelled differently",
			database: func() *goschema.Database {
				db := standaloneSequenceDatabase()
				db.Extensions = []goschema.Extension{{
					Name:     "isn",
					Provides: []string{"ean13", "isbn", "isbn13", "issn", "upc"},
				}}
				db.Fields = append(db.Fields, goschema.Field{
					StructName: "Order", Name: "code", Type: "isbn",
				})
				return db
			},
			want:     "extension \"isn\"",
			wantPath: "extensions.isn",
		},
		{
			// The function-only shape: nothing in a document ever spells a type
			// supplied by pgcrypto, only a call. Measured on PostgreSQL 17.10,
			// gen_salt is pgcrypto's and has no core equivalent, so a document
			// that drops the extension stops resolving.
			name: "a column default calls a function the extension supplies",
			database: func() *goschema.Database {
				db := standaloneSequenceDatabase()
				db.Extensions = []goschema.Extension{{
					Name:     "pgcrypto",
					Provides: []string{"crypt", "digest", "gen_salt", "hmac"},
				}}
				db.Fields = append(db.Fields, goschema.Field{
					StructName: "Order", Name: "salt", Type: "text",
					DefaultExpr: "gen_salt('bf'::text)",
				})
				return db
			},
			want:     "extension \"pgcrypto\"",
			wantPath: "extensions.pgcrypto",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				test.database(), platform.Postgres, "public",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want,
				qt.Commentf("the document names this object, so omitting its block leaves a dangling reference"))
			c.Assert(diagnosticMessageFor(result.Diagnostics, test.wantPath), qt.Contains,
				"kept in Atlas-compatible schema inspect output because another object in this document depends on it",
				qt.Commentf("keeping a block the pinned binary refuses has to be reported, not silently done"))
		})
	}
}

// TestRenderInspectedForAtlasCLIOmitsAnUnreferencedSequence is the control for
// the rule above: without the reference, the same sequence goes.
//
// Without this row the reference rule could be satisfied by never omitting a
// sequence at all, which would make the whole front a no-op.
func TestRenderInspectedForAtlasCLIOmitsAnUnreferencedSequence(t *testing.T) {
	c := qt.New(t)

	result, err := atlashclrender.RenderInspectedForAtlasCLI(
		standaloneSequenceDatabase(), platform.Postgres, "public",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), "sequence \"order_seq\"")
	c.Assert(diagnosticMessageFor(result.Diagnostics, "sequences.order_seq"), qt.Contains,
		"omitted from Atlas-compatible schema inspect output")
}

// TestRenderInspectedForAtlasCLIOmitsAnExtensionNothingDependsOn is the control
// that keeps the provides-aware rule from collapsing into "never omit an
// extension", which would throw away what stokaro/ptah#1266 bought.
//
// The extension here supplies a full member list -- the same one that keeps the
// block alive two tests up -- and the document simply uses none of it. Widening
// the rule to keep any extension whose Provides is non-empty, or to keep
// extensions unconditionally, turns this red.
func TestRenderInspectedForAtlasCLIOmitsAnExtensionNothingDependsOn(t *testing.T) {
	c := qt.New(t)

	db := standaloneSequenceDatabase()
	db.Extensions = []goschema.Extension{{
		Name:     "isn",
		Provides: []string{"ean13", "isbn", "isbn13", "issn", "upc"},
	}}
	db.Fields = append(db.Fields, goschema.Field{
		StructName: "Order", Name: "label", Type: "text",
	})

	result, err := atlashclrender.RenderInspectedForAtlasCLI(db, platform.Postgres, "public")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), "extension \"isn\"",
		qt.Commentf("nothing in the document uses anything isn supplies, so the block still goes"))
	c.Assert(diagnosticMessageFor(result.Diagnostics, "extensions.isn"), qt.Contains,
		"omitted from Atlas-compatible schema inspect output")
}

// TestRenderInspectedForAtlasCLIOutputIsSelfConsistent is the invariant the
// whole reshape exists to hold: whatever the surface decides to omit, the
// document it emits still describes itself.
//
// The assertion is a real round trip rather than a re-run of the renderer's own
// reference scan: the output is parsed back by Ptah's HCL reader, and every
// sequence a column default calls nextval on must come back as a declared
// sequence. A document that omitted a referenced block parses into an IR whose
// table needs a sequence the IR does not have, which is exactly the state that
// made `ptah-compat schema inspect -u file://<its own output>` exit 1.
func TestRenderInspectedForAtlasCLIOutputIsSelfConsistent(t *testing.T) {
	tests := []struct {
		name     string
		database func() *goschema.Database
	}{
		{name: "one object of every construct", database: inspectedRichDatabase},
		{name: "a sequence behind a column default", database: sequenceBackedDefaultDatabase},
		{name: "a sequence nothing names", database: standaloneSequenceDatabase},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			result, err := atlashclrender.RenderInspectedForAtlasCLI(
				test.database(), platform.Postgres, "public",
			)
			c.Assert(err, qt.IsNil)

			parsed, err := atlashcl.Parse(result.Data, "compat.hcl")
			c.Assert(err, qt.IsNil, qt.Commentf("Ptah cannot read its own compatibility output"))

			declared := map[string]bool{}
			for _, sequence := range parsed.Sequences {
				declared[sequence.Name] = true
			}
			for _, field := range parsed.Fields {
				for _, named := range nextvalSequenceNames(field.DefaultExpr) {
					c.Assert(declared[named], qt.IsTrue,
						qt.Commentf("column %q defaults to a sequence the document does not declare", field.Name))
				}
			}
		})
	}
}

// diagnosticMessageFor returns the message reported for one diagnostic path, or
// the empty string when nothing was reported for it.
func diagnosticMessageFor(diagnostics []atlashclrender.Diagnostic, path string) string {
	for _, diagnostic := range diagnostics {
		if diagnostic.Path == path {
			return diagnostic.Message
		}
	}
	return ""
}

// nextvalSequenceNames extracts the sequence names a default expression calls
// nextval on. It reads the one shape a PostgreSQL catalog reports --
// `nextval('name'::regclass)` -- because that is the shape the round-trip
// assertion is about.
func nextvalSequenceNames(expr string) []string {
	var names []string
	rest := expr
	for {
		start := strings.Index(strings.ToLower(rest), "nextval('")
		if start < 0 {
			return names
		}
		rest = rest[start+len("nextval('"):]
		end := strings.Index(rest, "'")
		if end < 0 {
			return names
		}
		name := rest[:end]
		if dot := strings.LastIndex(name, "."); dot >= 0 {
			name = name[dot+1:]
		}
		names = append(names, name)
		rest = rest[end:]
	}
}

// sequenceBackedDefaultDatabase is the shape that forced the reference rule:
// a sequence, and a column whose default calls it.
func sequenceBackedDefaultDatabase() *goschema.Database {
	start := int64(1)
	return &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_seq", AsType: "bigint", Start: &start}},
		Tables:    []goschema.Table{{StructName: "Order", Name: "orders", Schema: "public"}},
		Fields: []goschema.Field{{
			StructName:  "Order",
			Name:        "id",
			Type:        "integer",
			DefaultExpr: "nextval('order_seq'::regclass)",
		}},
	}
}

// standaloneSequenceDatabase is the same database with the reference removed,
// so a test can vary exactly one operand against sequenceBackedDefaultDatabase.
func standaloneSequenceDatabase() *goschema.Database {
	db := sequenceBackedDefaultDatabase()
	db.Fields[0].DefaultExpr = ""
	return db
}

// inspectedRichDatabase builds an IR carrying one object of every construct the
// inspected renderer emits a top-level block for, so a block that stops being
// rendered is visible rather than absent for want of an input.
//
// Nothing in it names the extension, the sequence or the policy, which is what
// makes it the fixture for the omission tests: those three go because they are
// unreferenced, not because of their block type alone.
func inspectedRichDatabase() *goschema.Database {
	start := int64(1)
	return &goschema.Database{
		Extensions: []goschema.Extension{{Name: "pgcrypto", Version: "1.3"}},
		Sequences:  []goschema.Sequence{{Name: "order_seq", AsType: "bigint", Start: &start}},
		Domains:    []goschema.Domain{{Name: "positive", BaseType: "integer"}},
		CompositeTypes: []goschema.CompositeType{{
			Name:   "pair",
			Fields: []goschema.CompositeTypeField{{Name: "a", Type: "integer"}},
		}},
		Ranges: []goschema.Range{{Name: "intrange", Subtype: "integer"}},
		Enums:  []goschema.Enum{{Name: "status", Values: []string{"active", "inactive"}}},
		Roles:  []goschema.Role{{Name: "app_reader", Inherit: true}},
		Tables: []goschema.Table{{StructName: "Account", Name: "accounts", Schema: "public"}},
		Fields: []goschema.Field{{StructName: "Account", Name: "id", Type: "bigint"}},
		Functions: []goschema.Function{{
			Name:     "touch_accounts",
			Language: "plpgsql",
			Returns:  "trigger",
			Body:     "BEGIN RETURN NEW; END;",
		}},
		Views:             []goschema.View{{Name: "active_accounts", Body: "SELECT id FROM accounts"}},
		MaterializedViews: []goschema.MaterializedView{{Name: "account_counts", Body: "SELECT id FROM accounts"}},
		Triggers: []goschema.Trigger{{
			Name:   "accounts_touch",
			Table:  "accounts",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "BEGIN RETURN NEW; END;",
		}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "accounts"}},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "accounts_all",
			Table:           "accounts",
			PolicyFor:       "ALL",
			UsingExpression: "true",
		}},
		Grants: []goschema.Grant{{
			Role:       "app_reader",
			OnTable:    "accounts",
			Privileges: []string{"SELECT"},
		}},
		ManagedData: []goschema.ManagedData{{Table: "accounts", Keys: []string{"id"}, File: "accounts.csv"}},
	}
}
