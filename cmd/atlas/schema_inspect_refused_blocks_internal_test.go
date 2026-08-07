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
	tests := []struct {
		name   string
		value  string
		assert func(c *qt.C, hcl, diagnostics string)
	}{
		{
			name:  "unset omits the blocks the pinned binary refuses",
			value: "",
			assert: func(c *qt.C, hcl, diagnostics string) {
				c.Assert(hcl, qt.Not(qt.Contains), "extension \"pgcrypto\"")
				c.Assert(hcl, qt.Not(qt.Contains), "sequence \"lonely_seq\"")
				c.Assert(hcl, qt.Contains, "table \"accounts\"")
				c.Assert(diagnostics, qt.Contains, "extensions.pgcrypto")
				c.Assert(diagnostics, qt.Contains,
					"set PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 to keep every block Ptah models")
			},
		},
		{
			name:  "the opt-in puts every block back",
			value: "1",
			assert: func(c *qt.C, hcl, diagnostics string) {
				c.Assert(hcl, qt.Contains, "extension \"pgcrypto\"")
				c.Assert(hcl, qt.Contains, "sequence \"lonely_seq\"")
				c.Assert(hcl, qt.Contains, "table \"accounts\"")
				c.Assert(diagnostics, qt.Not(qt.Contains), "omitted from Atlas-compatible")
			},
		},
		{
			name:  "a false value keeps the compatible default",
			value: "false",
			assert: func(c *qt.C, hcl, diagnostics string) {
				c.Assert(hcl, qt.Not(qt.Contains), "extension \"pgcrypto\"")
				c.Assert(diagnostics, qt.Contains, "omitted from Atlas-compatible")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashclrender.KeepAtlasRefusedBlocksEnvVar, test.value)

			var diagnostics bytes.Buffer
			report := atlasreport.NewSchemaInspectReport(
				refusedBlockGateDatabase(),
				&types.DBSchema{},
				types.DBInfo{Dialect: "postgres", Schema: "public"},
				&diagnostics,
				atlasInspectOmitsRefusedBlocks(),
				true,
			)

			hcl, err := report.MarshalHCL()

			c.Assert(err, qt.IsNil)
			test.assert(c, hcl, diagnostics.String())
		})
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
	for _, value := range []string{"", "1"} {
		t.Run("PTAH_ATLAS_INSPECT_ALL_BLOCKS="+value, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlashclrender.KeepAtlasRefusedBlocksEnvVar, value)

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
				atlasInspectOmitsRefusedBlocks(),
				true,
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
