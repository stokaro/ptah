package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
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
// surface omits three block types, and nothing about that decision may leak
// into the surface whose job is to describe the database completely. A native
// render that quietly lost a construct would still pass every compatibility
// test in this package, so it is pinned here on its own.
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
// block types the compatibility surface leaves out.
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
// dropped quietly.
//
// An inspect output that silently omitted an extension would describe a
// database that does not exist, and the operator reading it has no other way to
// learn what was left out. The omission rides the renderer's existing loss
// diagnostics, which `schema inspect` writes to standard error.
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
				" document unreadable to it",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "sequences.order_seq",
			Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses" +
				" a postgres schema file that declares any sequence block, and one of them makes the whole" +
				" document unreadable to it",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "rls_policies.accounts_all",
			Message: "omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses" +
				" a postgres schema file that declares any policy block, and one of them makes the whole" +
				" document unreadable to it",
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

// inspectedRichDatabase builds an IR carrying one object of every construct the
// inspected renderer emits a top-level block for, so a block that stops being
// rendered is visible rather than absent for want of an input.
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
