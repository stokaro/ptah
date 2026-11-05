package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
)

// indentedThrough writes sql for dialect with the `sql` helper's tab indent.
func indentedThrough(c *qt.C, dialect, sql string) string {
	c.Helper()
	var output bytes.Buffer
	report := atlasreport.NewSchemaDiff(atlasreport.SchemaDiffOptions{Dialect: dialect, Statements: []string{sql}})
	c.Assert(atlasreport.WriteMigrateDiff(&output, `{{ sql . "\t" }}`, report), qt.IsNil)
	return output.String()
}

// An engine whose provider scans its text keeps a line that starts inside one
// of its strings as written, here an @@...@@ string the shared rules do not
// read, and still indents the line after it. Read without the provider's
// scanner, the same text moves the string's second line.
func TestMigrateDiffIndentReadsAProvidersStrings(t *testing.T) {
	c := qt.New(t)
	const sql = "SELECT @@first\n  second@@,\n  2;"
	release, err := lexerext.Provide(providerscan.New(scantest.Scan), "indent-scanned")
	c.Assert(err, qt.IsNil)
	t.Cleanup(release)

	c.Assert(indentedThrough(c, "indent-scanned", sql), qt.Equals, "SELECT @@first\n  second@@,\n\t2;\n")
	c.Assert(indentedThrough(c, "indent-unscanned", sql), qt.Equals, "SELECT @@first\n\tsecond@@,\n\t2;\n")
}
