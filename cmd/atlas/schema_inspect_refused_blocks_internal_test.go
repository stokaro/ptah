package atlas

// White-box testing required: the subject is atlasInspectOmitsRefusedBlocks,
// the unexported expression that decides what the Atlas-compatible surface
// renders. Its polarity is the whole capability gate -- inverted, either every
// document becomes unreadable to the pinned binary or the opt-in stops
// restoring anything -- and it is not reachable from outside the package.
// Asserting it from a black-box test would need a live PostgreSQL database,
// which is where the end-to-end measurement runs instead.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// TestAtlasInspectRefusedBlockGate pins both states of the capability gate, and
// pins them by what comes OUT rather than by what was read.
//
// The default has to be the narrower document: `ptah-compat` stands in for a
// binary that refuses a whole schema file containing an extension, sequence or
// policy block, and unreadable output is a real defect. The opt-in has to bring
// the blocks back on this same surface, because a capability reachable only
// through native `ptah` is a rewrite rather than a migration path (AGENTS.md,
// "Compatibility never removes a capability"). A test that only checked the
// variable had been read would pass with an opt-in wired to nothing, so each
// row asserts the blocks themselves.
func TestAtlasInspectRefusedBlockGate(t *testing.T) {
	// The blocks the pinned binary refuses, the block it reads either way, and
	// the notice that has to travel with an omission. A row states all four
	// lists, so "false" is measured against the same document as "unset"
	// instead of against a shorter claim that would also hold for a gate stuck
	// half-open.
	refusedBlocks := []string{"extension \"pgcrypto\"", "sequence \"lonely_seq\""}
	readEitherWay := []string{"table \"accounts\""}
	omissionNotice := []string{
		"extensions.pgcrypto",
		"omitted from Atlas-compatible",
		"set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models",
	}

	tests := []struct {
		name                  string
		env                   func(testing.TB)
		wantHCL               []string
		wantHCLAbsent         []string
		wantDiagnostics       []string
		wantDiagnosticsAbsent []string
	}{
		{
			name:            "unset omits the blocks the pinned binary refuses",
			env:             envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar),
			wantHCL:         readEitherWay,
			wantHCLAbsent:   refusedBlocks,
			wantDiagnostics: omissionNotice,
		},
		{
			name: "the opt-in puts every block back",
			env:  envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "1"),
			wantHCL: []string{
				"extension \"pgcrypto\"",
				"sequence \"lonely_seq\"",
				"table \"accounts\"",
			},
			wantDiagnosticsAbsent: omissionNotice,
		},
		{
			name:            "a false value keeps the compatible default",
			env:             envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "false"),
			wantHCL:         readEitherWay,
			wantHCLAbsent:   refusedBlocks,
			wantDiagnostics: omissionNotice,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)

			omit, gateErr := atlasInspectOmitsRefusedBlocks()
			c.Assert(gateErr, qt.IsNil)
			var diagnostics bytes.Buffer
			report := atlasreport.NewSchemaInspectReport(
				refusedBlockGateDatabase(),
				&types.DBSchema{},
				types.DBInfo{Dialect: "postgres", Schema: "public"},
				&diagnostics,
				atlasreport.SchemaInspectReportOptions{
					OmitAtlasRefusedBlocks: omit,
					DescribeSchemas:        true,
				},
			)

			hcl, err := report.MarshalHCL()

			c.Assert(err, qt.IsNil)
			assertRefusedBlockText(c, hcl, test.wantHCL, test.wantHCLAbsent)
			assertRefusedBlockText(c, diagnostics.String(),
				test.wantDiagnostics, test.wantDiagnosticsAbsent)
		})
	}
}

// assertRefusedBlockText pins what a rendered document says and what it must
// not say. Both directions matter to this gate: a document that names a block
// it did not render, and one that omits a block without saying so, are the two
// ways the capability goes missing quietly.
func assertRefusedBlockText(c *qt.C, got string, want, absent []string) {
	c.Helper()

	for _, fragment := range want {
		c.Assert(got, qt.Contains, fragment)
	}
	for _, fragment := range absent {
		c.Assert(got, qt.Not(qt.Contains), fragment)
	}
}

// TestAtlasInspectKeepsAReferencedBlockInEitherState pins the one thing neither
// state may do: emit a document that names an object it did not declare.
//
// Measured on PostgreSQL 17, the pinned binary's own inspect of a sequence-backed
// column default emits `default = sql("nextval('order_seq'::regclass)")` with no
// `sequence` block and then refuses that output itself --
// `pq: relation "order_seq" does not exist`. Reproducing it would be copying a
// defect, so the sequence stays whichever way the gate is set.
func TestAtlasInspectKeepsAReferencedBlockInEitherState(t *testing.T) {
	states := []struct {
		name string
		env  func(testing.TB)
	}{
		{name: "unset", env: envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar)},
		{name: "1", env: envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "1")},
	}
	for _, state := range states {
		t.Run("PTAH_ATLAS_INSPECT_ALL_BLOCKS="+state.name, func(t *testing.T) {
			c := qt.New(t)
			state.env(t)

			omit, gateErr := atlasInspectOmitsRefusedBlocks()
			c.Assert(gateErr, qt.IsNil)
			var diagnostics bytes.Buffer
			db := refusedBlockGateDatabase()
			db.Fields = append(db.Fields, goschema.Field{
				StructName:  "Account",
				Name:        "seq_id",
				Type:        "integer",
				DefaultExpr: "nextval('lonely_seq'::regclass)",
			})
			report := atlasreport.NewSchemaInspectReport(
				db,
				&types.DBSchema{},
				types.DBInfo{Dialect: "postgres", Schema: "public"},
				&diagnostics,
				atlasreport.SchemaInspectReportOptions{
					OmitAtlasRefusedBlocks: omit,
					DescribeSchemas:        true,
				},
			)

			hcl, err := report.MarshalHCL()

			c.Assert(err, qt.IsNil)
			c.Assert(hcl, qt.Contains, "sequence \"lonely_seq\"")
			c.Assert(hcl, qt.Contains, "nextval('lonely_seq'::regclass)",
				qt.Commentf("the reference and the block it names have to travel together"))
		})
	}
}

// refusedBlockGateDatabase carries one object of each refused block type with
// nothing naming it, plus a table so the document is not empty.
func refusedBlockGateDatabase() *goschema.Database {
	start := int64(1)
	return &goschema.Database{
		Extensions:       []goschema.Extension{{Name: "pgcrypto", Version: "1.3"}},
		Sequences:        []goschema.Sequence{{Name: "lonely_seq", AsType: "bigint", Start: &start}},
		Tables:           []goschema.Table{{StructName: "Account", Name: "accounts", Schema: "public"}},
		Fields:           []goschema.Field{{StructName: "Account", Name: "id", Type: "bigint"}},
		RLSEnabledTables: []goschema.RLSEnabledTable{{Table: "accounts"}},
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "accounts_all",
			Table:           "accounts",
			PolicyFor:       "ALL",
			UsingExpression: "true",
		}},
	}
}
