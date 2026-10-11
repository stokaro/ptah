package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/goannotationexport"
)

// TestExport_KeepsAnOwnersLimitInTheHeader exports a Go source's declaration
// that it does not describe the YDB changefeeds of a table into the HCL
// header, through the vocabulary of the annotations it read, and reads it
// back the same.
func TestExport_KeepsAnOwnersLimitInTheHeader(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	output := filepath.Join(root, "schema.hcl")
	c.Assert(os.WriteFile(filepath.Join(root, "model.go"), []byte(`package models

//ptah:schema:notdescribed kind="changefeed" name="events"
//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	_, err := goannotationexport.Export(goannotationexport.Options{HCL: builtintest.Runtime().HCL(), Annotations: builtintest.Annotations(),
		RootDir: root, OutputPath: output})

	c.Assert(err, qt.IsNil)
	data, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, "// ptah:not-described changefeed")
}
