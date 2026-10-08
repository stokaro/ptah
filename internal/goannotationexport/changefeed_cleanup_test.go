package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/goannotationexport"
)

// HCL has no block for a YDB changefeed, so an export that cleans up the Go
// annotations afterwards would delete the only place one is declared. The
// export refuses and leaves both files as they were.
func TestExport_FailurePath_ChangefeedRefusesCleanupAndPreservesFiles(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "model.go")
	output := filepath.Join(root, "schema.hcl")
	sourceData := []byte(`package models

//ptah:schema:table name="events"
//ptah:schema:changefeed name="updates" mode="UPDATES" format="JSON"
type Event struct {
	//ptah:schema:field name="id" type="BIGINT UNSIGNED" primary="true"
	ID uint64
}
`)
	outputData := []byte("previous schema\n")
	c.Assert(os.WriteFile(source, sourceData, 0o600), qt.IsNil)
	c.Assert(os.WriteFile(output, outputData, 0o600), qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
		Cleanup:    true,
	})

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)
	c.Assert(err.Error(), qt.Contains, "feature object ptah.run/ydb/changefeed events.updates of kind ptah.run/ydb/changefeed is not represented in HCL")
	c.Assert(result, qt.DeepEquals, goannotationexport.Result{})
	assertFileBytes(c, source, sourceData)
	assertFileBytes(c, output, outputData)
}
