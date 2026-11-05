package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/goannotationexport"
)

// TestExport_KeepsAnOwnersLimitInTheHeader exports a Go source's declaration
// that it does not describe the PostgreSQL range type price_range, a kind the
// type owner reads rather than the frontend, into the HCL header. The header
// reads back as the same claim: that range type is unknown, and every other
// one is still described.
func TestExport_KeepsAnOwnersLimitInTheHeader(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	output := filepath.Join(root, "schema.hcl")
	c.Assert(os.WriteFile(filepath.Join(root, "model.go"), []byte(`package models

//ptah:schema:notdescribed kind="range" name="price_range"
//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	_, err := goannotationexport.Export(t.Context(), goannotationexport.Options{HCL: builtintest.Runtime().HCL(), Annotations: builtintest.Annotations(),
		RootDir: root, OutputPath: output})

	c.Assert(err, qt.IsNil)
	data, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Matches, `(?s)(// [^\n]*\n)*// ptah:not-described range "price_range"\n.*`)
	read, err := atlashcl.ParseWithOptions(c.Context(), data, output, atlashcl.Options{HCL: builtintest.Runtime().HCL()})
	c.Assert(err, qt.IsNil)
	c.Assert(read.FeatureCoverage.Lookup(pgtype.RangeKind, pgtype.Ref(pgtype.RangeKind, "", "price_range")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(read.FeatureCoverage.Lookup(pgtype.RangeKind, pgtype.Ref(pgtype.RangeKind, "", "other_range")).State, qt.Equals, schemaext.Complete)
}
