//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// oracleCollationSources declare the same column, `c VARCHAR2(20)` under
// BINARY_CI, in each source a schema can come from. Go annotations bind a
// column's collation to a target, so the Go source says
// `platform.oracle.collate`; SQL, YAML and HCL say `collate` or COLLATE.
var oracleCollationSources = []struct {
	name     string
	file     string
	document string
	// flag and value select the source: a schema file, or a directory of Go
	// packages.
	flag  string
	value string
}{
	{
		name:     "SQL",
		file:     "schema.sql",
		document: "CREATE TABLE t (id NUMBER PRIMARY KEY, c VARCHAR2(20) COLLATE BINARY_CI);\n",
		flag:     "--schema-file",
		value:    "schema.sql",
	},
	{
		name: "YAML",
		file: "schema.yaml",
		document: "tables:\n  t:\n    columns:\n      id:\n        type: NUMBER\n        primary: true\n" +
			"      c:\n        type: VARCHAR2(20)\n        collate: BINARY_CI\n",
		flag:  "--schema-file",
		value: "schema.yaml",
	},
	{
		name: "HCL",
		file: "schema.hcl",
		document: "table \"t\" {\n  column \"id\" {\n    type = NUMBER\n  }\n" +
			"  column \"c\" {\n    type = VARCHAR2(20)\n    collate = \"BINARY_CI\"\n  }\n  primary_key {\n    columns = [column.id]\n  }\n}\n",
		flag:  "--schema-file",
		value: "schema.hcl",
	},
	{
		name: "Go annotations",
		file: "schema.go",
		document: "package schema\n\n//ptah:schema:table name=\"t\"\ntype T struct {\n" +
			"\t//ptah:schema:field name=\"id\" type=\"NUMBER\" primary=\"true\"\n\tID int\n" +
			"\t//ptah:schema:field name=\"c\" type=\"VARCHAR2(20)\" platform.oracle.collate=\"BINARY_CI\"\n\tC string\n}\n",
		flag:  "--root-dir",
		value: ".",
	},
}

// TestSchemaRenderWritesAnOracleColumnCollation_HappyPathE2E renders the same
// column from each source and finds the collation after the type, with
// nothing reported as skipped.
func TestSchemaRenderWritesAnOracleColumnCollation_HappyPathE2E(t *testing.T) {
	for _, test := range oracleCollationSources {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(root, test.file), []byte(test.document), 0o600), qt.IsNil)

			result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
				"schema", "render", "--dialect", "oracle", test.flag, test.value)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", result.Stderr))
			c.Assert(result.Stdout, qt.Contains, "\n  c VARCHAR2(20) COLLATE BINARY_CI")
			c.Assert(result.Stdout+result.Stderr, qt.Not(qt.Contains), "skipped")
		})
	}
}

// TestSchemaRenderRefusesAnOracleColumnCollationOnANumber_FailurePathE2E turns
// ORA-43911 into a refusal before any statement is written. Oracle takes a
// collation on character columns only.
func TestSchemaRenderRefusesAnOracleColumnCollationOnANumber_FailurePathE2E(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.sql"),
		[]byte("CREATE TABLE t (id NUMBER PRIMARY KEY, n NUMBER COLLATE BINARY_CI);\n"), 0o600), qt.IsNil)

	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()},
		"schema", "render", "--dialect", "oracle", "--schema-file", "schema.sql")

	c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(result.Stderr, qt.Contains, `column "n" is NUMBER`)
	c.Assert(result.Stderr, qt.Contains, "ORA-43911")
	c.Assert(result.Stdout, qt.Not(qt.Contains), "CREATE TABLE")
}
