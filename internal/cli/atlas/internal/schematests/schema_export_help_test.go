package schematests_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// runSchemaHelp renders one schema verb's long help.
func runSchemaHelp(c *qt.C, verb string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", verb, "--help"})
	err := cmd.Execute()
	return out.String(), err
}

// TestSchemaExportHelpDoesNotCallTheFlagUnimplemented is the guard for the
// long-form help of the two verbs that share `--export`.
//
// `schema diff`'s long help said the flag was "registered and refused" and that
// Ptah does not evaluate an `exporter` block, while
// TestSchemaDiffExportRendersTheDeclaredTemplate rendered a diff through one at
// exit 0. Prose has no generator behind it, so nothing compared the sentence
// with the behavior and the reader was told a working flag does nothing
// (stokaro/ptah#3109).
//
// The assertion is on the capability rather than on wording: either verb may
// describe the flag however it likes, and neither may call it unimplemented
// while the other implements it.
func TestSchemaExportHelpDoesNotCallTheFlagUnimplemented(t *testing.T) {
	rows := []struct {
		name string
		verb string
	}{
		{name: "schema diff", verb: "diff"},
		{name: "schema inspect", verb: "inspect"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			out, err := runSchemaHelp(c, row.verb)

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, "--export")
			c.Assert(out, qt.Not(qt.Contains), "--export is registered and refused",
				qt.Commentf("help calls --export refused while the flag renders an exporter"))
		})
	}
}
