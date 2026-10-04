package viz_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/viz"
)

// ydbModel is a model `ptah introspect` writes for a YDB table in a
// directory: YDB type names and a Serial key.
const ydbModel = `package models

//ptah:schema:table name="orders" schema="shop"
type Order struct {
	//ptah:schema:field name="id" type="Int64" not_null="true" primary="true" auto_increment="true"
	ID int64
	//ptah:schema:field name="customer" type="Utf8" not_null="true"
	Customer string
	//ptah:schema:field name="created" type="Timestamp"
	Created *string
}
`

// A diagram of a YDB model draws its tables and YDB types, and with
// --security runs the rules under the named YDB line's capabilities: YDB has
// no row-level security, so the rule that needs it says it was not checked
// rather than passing quietly.
func TestCommand_YDBDialectDrawsAndChecksUnderItsLine(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "orders.go"), []byte(ydbModel), 0o600), qt.IsNil)
	cmd := viz.NewCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--root-dir", dir, "--format", "mermaid", "--include-columns", "--security",
		"--dialect", "ydb", "--server-version", "25.1.4.7",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(stdout.String(), qt.Contains, "  shop_orders {\n    Int64 id PK\n    Utf8 customer\n    Timestamp created\n  }\n")
	c.Assert(stdout.String(), qt.Contains, "  %% PRV01 not checked here: the target does not model row-level security\n")
}
