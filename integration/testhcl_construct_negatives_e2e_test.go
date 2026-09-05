//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/root"
)

// runSchemaTestDocument writes one `.test.hcl` document and runs
// `ptah schema test` over it, returning the command's error and its output.
//
// It takes the checker rather than a testing.TB for the reason AGENTS.md gives:
// *qt.C already is one, and widening buys nothing.
func runSchemaTestDocument(c *qt.C, document string) (error, string) {
	dir := c.TB.(*testing.T).TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "n.test.hcl"), []byte(document), 0o600), qt.IsNil)

	models := c.TB.(*testing.T).TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "m.go"), []byte(`package models

//ptah:schema:table name="t"
type T struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}
`), 0o600), qt.IsNil)

	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"schema", "test", "--dir", dir, "--root-dir", models})

	return cmd.Execute(), out.String()
}

// TestSchemaTest_EveryConstructHasANegativeEndToEndFixture is the half the
// suite was short of.
//
// Each construct had a positive fixture reaching the command, and a positive
// fixture alone cannot tell a working check from one that always passes. Every
// row here is a document whose construct SHOULD fail, driven through the real
// verb, and asserted on the detail rather than only on the verdict -- a case
// that failed for an unrelated reason would satisfy a verdict-only assertion.
func TestSchemaTest_EveryConstructHasANegativeEndToEndFixture(t *testing.T) {
	tests := []struct {
		name       string
		document   string
		wantDetail string
	}{
		{
			name:       "exec output disagrees with the result",
			document:   "test \"schema\" \"a\" {\n  exec {\n    sql    = \"SELECT 1\"\n    output = \"999\"\n  }\n}\n",
			wantDetail: `expected result set "999", got "1"`,
		},
		{
			name:       "exec match does not match",
			document:   "test \"schema\" \"a\" {\n  exec {\n    sql   = \"SELECT 'ada'\"\n    match = \"^zz\"\n  }\n}\n",
			wantDetail: `to match "^zz"`,
		},
		{
			name:       "catch on a statement that succeeds",
			document:   "test \"schema\" \"a\" {\n  catch { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "expected an error, but the query succeeded",
		},
		{
			name:       "catch whose message does not match",
			document:   "test \"schema\" \"a\" {\n  catch {\n    sql   = \"SELECT * FROM missing_table\"\n    error = \"^nothing like it$\"\n  }\n}\n",
			wantDetail: "to match",
		},
		{
			name:       "a boolean assert on a false value",
			document:   "test \"schema\" \"a\" {\n  assert {\n    sql           = \"SELECT 1 = 2\"\n    error_message = \"the invariant\"\n  }\n}\n",
			wantDetail: "the invariant",
		},
		{
			name:       "a cleanup that cannot run",
			document:   "test \"schema\" \"a\" {\n  exec { sql = \"SELECT 1\" }\n  cleanup { sql = \"DROP TABLE missing_table\" }\n}\n",
			wantDetail: "cleanup step",
		},
		{
			name:       "for_each over something that cannot be iterated",
			document:   "test \"schema\" \"a\" {\n  for_each = \"one\"\n  exec { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "`for_each` must be a collection or a mapping",
		},
		{
			name:       "skip that is not a boolean",
			document:   "test \"schema\" \"a\" {\n  skip = \"yes\"\n  exec { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "`skip` must be a boolean",
		},
		{
			name:       "parallel that is not a boolean",
			document:   "test \"schema\" \"a\" {\n  parallel = 7\n  exec { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "`parallel` must be a boolean",
		},
		{
			name:       "a variable with no default",
			document:   "variable \"n\" {\n}\ntest \"schema\" \"a\" {\n  exec { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "has no `default`",
		},
		{
			name:       "file() reaching outside its directory",
			document:   "test \"schema\" \"a\" {\n  exec { sql = \"SELECT '${file(\"../escape.txt\")}'\" }\n}\n",
			wantDetail: "reads only inside the directory that holds it",
		},
		{
			name:       "an external step without the authorization",
			document:   "test \"schema\" \"a\" {\n  external {\n    program = [\"/bin/echo\", \"hi\"]\n  }\n}\n",
			wantDetail: "PTAH_ALLOW_EXTERNAL_TEST_COMMAND",
		},
		{
			name:       "an unknown case attribute",
			document:   "test \"schema\" \"a\" {\n  paralel = true\n  exec { sql = \"SELECT 1\" }\n}\n",
			wantDetail: "not [paralel]",
		},
		{
			name:       "an unknown step block",
			document:   "test \"schema\" \"a\" {\n  frobnicate { sql = \"SELECT 1\" }\n}\n",
			wantDetail: `unsupported step "frobnicate"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err, output := runSchemaTestDocument(c, test.document)

			c.Assert(err, qt.IsNotNil, qt.Commentf("output: %s", output))
			c.Assert(err.Error()+output, qt.Contains, test.wantDetail)
		})
	}
}
