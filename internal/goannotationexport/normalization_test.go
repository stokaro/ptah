package goannotationexport_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/goannotationexport"
)

// decomposedAccent is "e" followed by U+0301 COMBINING ACUTE ACCENT. Written as
// an escape on purpose: an editor or filesystem that normalizes source files
// would silently compose a literal and make these tests vacuous.
const decomposedAccent = "e\u0301"

// composedAccent is the same grapheme as the single code point U+00E9.
const composedAccent = "\u00e9"

// modelWithFunctionBody writes a minimal annotated package whose function body
// carries the given text, and returns the directory holding it.
func modelWithFunctionBody(c *qt.C, body string) string {
	c.Helper()
	dir := c.TempDir()
	source := `package models

//ptah:schema:function name="greet" params="" returns="text" language="sql" body="SELECT '` + body + `'"
const _ = 0

//ptah:schema:table name="items"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

func exportOptions(dir, out string, cleanup bool) goannotationexport.Options {
	return goannotationexport.Options{RootDir: dir, OutputPath: out, Cleanup: cleanup}
}

func TestExportReportsNonNFCAttributeValue(t *testing.T) {
	c := qt.New(t)

	dir := modelWithFunctionBody(c, "Caf"+decomposedAccent)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	result, err := goannotationexport.Export(exportOptions(dir, out, false))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.Not(qt.HasLen), 0)
	c.Assert(diagnosticMessages(result), qt.Any(qt.Contains), "attribute body is not Unicode NFC")
	c.Assert(diagnosticMessages(result), qt.Any(qt.Contains), "models.go")
}

func TestExportNonNFCDiagnosticNeverCarriesTheValue(t *testing.T) {
	c := qt.New(t)

	// A password is the case that matters: the diagnostic must name the
	// attribute without ever echoing what it holds.
	dir := c.TempDir()
	source := `package models

//ptah:schema:role name="app" login="true" password="Caf` + decomposedAccent + `-secret"
const _ = 0

//ptah:schema:table name="items"
type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(source), 0o600), qt.IsNil)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	result, err := goannotationexport.Export(exportOptions(dir, out, false))

	c.Assert(err, qt.IsNil)
	c.Assert(diagnosticMessages(result), qt.Any(qt.Contains), "attribute password is not Unicode NFC")
	for _, message := range diagnosticMessages(result) {
		c.Assert(message, qt.Not(qt.Contains), "secret")
	}
}

func TestExportRefusesDestructiveCleanupOnNonNFCValue(t *testing.T) {
	c := qt.New(t)

	dir := modelWithFunctionBody(c, "Caf"+decomposedAccent)
	source := filepath.Join(dir, "models.go")
	before, err := os.ReadFile(source)
	c.Assert(err, qt.IsNil)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	_, err = goannotationexport.Export(exportOptions(dir, out, true))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)

	// The source still holds the only copy of the exact bytes, and nothing was
	// published over the output path.
	after, readErr := os.ReadFile(source)
	c.Assert(readErr, qt.IsNil)
	c.Assert(after, qt.DeepEquals, before)
	_, statErr := os.Stat(out)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestExportAcceptsComposedAndASCIIValues(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "composed", body: "Caf" + composedAccent},
		{name: "ascii", body: "Cafe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			dir := modelWithFunctionBody(c, tt.body)
			out := filepath.Join(c.TempDir(), "schema.hcl")

			result, err := goannotationexport.Export(exportOptions(dir, out, true))

			c.Assert(err, qt.IsNil)
			c.Assert(normalizationMessages(result), qt.HasLen, 0)
		})
	}
}

func diagnosticMessages(result goannotationexport.Result) []string {
	messages := make([]string, 0, len(result.Diagnostics))
	for _, diagnostic := range result.Diagnostics {
		messages = append(messages, diagnostic.Path+": "+diagnostic.Message)
	}
	return messages
}

func normalizationMessages(result goannotationexport.Result) []string {
	var messages []string
	for _, diagnostic := range result.Diagnostics {
		messages = appendIfNormalization(messages, diagnostic.Message)
	}
	return messages
}

// appendIfNormalization keeps the branch out of the test body, which teststyle
// forbids.
func appendIfNormalization(messages []string, message string) []string {
	if strings.Contains(message, "is not Unicode NFC") {
		return append(messages, message)
	}
	return messages
}
