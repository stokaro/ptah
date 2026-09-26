//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// mysqlFamilyColumnCheckYAML declares table e in YAML: columns a and b each
// carry a CHECK without check_name, and the table carries a `checks` entry.
const mysqlFamilyColumnCheckYAML = `tables:
  e:
    columns:
      id:
        type: INT
        primary: true
      a:
        type: INT
        check: a > 0
      b:
        type: INT
        check: b > 0
    checks:
      - a < b
`

// mysqlFamilyColumnCheckGo declares the same table with Go annotations.
const mysqlFamilyColumnCheckGo = `package model

//ptah:schema:table name="e"
type E struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
	//ptah:schema:field name="a" type="INT" check="a > 0"
	A int
	//ptah:schema:field name="b" type="INT" check="b > 0"
	B int
}
`

// TestSchemaApplyOfUnnamedColumnChecksConvergesOnTheMySQLFamilyE2E applies a
// YAML file and a Go-annotation package whose column CHECKs carry no name, and
// applies them again. The renderer writes each CHECK unnamed, and the server
// names it: `e_chk_1` and `e_chk_2` on MySQL, `a` and `b` on MariaDB. A
// comparison looking for `e_a_check` would drop both and add them back on
// every apply after the first (stokaro/ptah#3792).
//
// The CHECKs read back are compared with a database the equivalent SQL built,
// so the names asserted are the server's.
func TestSchemaApplyOfUnnamedColumnChecksConvergesOnTheMySQLFamilyE2E(t *testing.T) {
	tests := []struct {
		name       string
		engine     mysqlFamilyEngine
		flag       string
		file       string
		source     string
		sourcePath string
		equivalent string
	}{
		{
			name: "mysql: YAML", engine: mysqlCheckEngine,
			flag: "--schema-file", file: "schema.yaml", source: mysqlFamilyColumnCheckYAML, sourcePath: "schema.yaml",
			equivalent: `CREATE TABLE e (id INT PRIMARY KEY, a INT CHECK (a > 0), b INT CHECK (b > 0),
  CONSTRAINT e_check CHECK (a < b));`,
		},
		{
			name: "mysql: Go annotations", engine: mysqlCheckEngine,
			flag: "--root-dir", file: "model.go", source: mysqlFamilyColumnCheckGo, sourcePath: ".",
			equivalent: `CREATE TABLE e (id INT PRIMARY KEY, a INT CHECK (a > 0), b INT CHECK (b > 0));`,
		},
		{
			name: "mariadb: YAML", engine: mariaDBCheckEngine,
			flag: "--schema-file", file: "schema.yaml", source: mysqlFamilyColumnCheckYAML, sourcePath: "schema.yaml",
			equivalent: `CREATE TABLE e (id INT PRIMARY KEY, a INT CHECK (a > 0), b INT CHECK (b > 0),
  CONSTRAINT e_check CHECK (a < b));`,
		},
		{
			name: "mariadb: Go annotations", engine: mariaDBCheckEngine,
			flag: "--root-dir", file: "model.go", source: mysqlFamilyColumnCheckGo, sourcePath: ".",
			equivalent: `CREATE TABLE e (id INT PRIMARY KEY, a INT CHECK (a > 0), b INT CHECK (b > 0));`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newCheckScratch(c, test.engine)
			builtName, _ := scratch.builtFrom(c, "colchk_want", test.equivalent)
			want := scratch.checks(c, builtName)
			targetName, target := scratch.database(c, "colchk_target")
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, test.file), []byte(test.source), 0o600), qt.IsNil)
			source := filepath.Join(dir, test.sourcePath)

			runPtahNative(c, "schema", "apply", "--db-url", target, test.flag, source, "--auto-approve")

			c.Assert(scratch.checks(c, targetName), qt.DeepEquals, want)
			out := runPtahNative(c, "schema", "apply", "--db-url", target, test.flag, source, "--dry-run")
			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}
