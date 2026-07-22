package generate

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
)

func outputStatementContaining(output, marker string) string {
	for statement := range strings.SplitSeq(output, "-- Statement ") {
		if strings.Contains(statement, marker) {
			return statement
		}
	}
	return ""
}

func TestLoadSchemaFile_YAML(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.yaml")
	c.Assert(
		os.WriteFile(path, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600),
		qt.IsNil,
	)

	db, err := loadSchema("", path)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
}

func TestLoadSchemaFile_AtlasHCL(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`), 0o600), qt.IsNil)

	db, err := loadSchema("", path)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields[0].Primary, qt.IsTrue)
}

func TestLoadSchemaFile_SQL(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
`), 0o600), qt.IsNil)

	db, err := loadSchema("", path)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 2)
}

func TestLoadSchemaFile_RejectsUnsupportedExtension(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.json")
	c.Assert(os.WriteFile(path, []byte(`{}`), 0o600), qt.IsNil)

	_, err := loadSchema("", path)
	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension ".json": only .yaml, .yml, .hcl, and .sql are supported`)
}

func TestGenerateCommandUnsupportedDialectExits2WithoutPanicTrace(t *testing.T) {
	c := qt.New(t)

	cmd := NewGenerateCommand()
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"--root-dir", filepath.Join("..", "..", "stubs"), "--dialect", "oracle"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNotNil)
	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(errOut.String(), qt.Contains, "error: error rendering oracle schema: unsupported database dialect: oracle")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "panic:")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "goroutine")
	c.Assert(errOut.String(), qt.Not(qt.Contains), "Usage:")
}

func TestGenerateCommand_MutualForeignKeysAreTwoPhase(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "029-roundtrip-mutual-cycle")
	cmd := exec.Command("go", "run", "../main.go", "schema", "render", "--root-dir", fixtureDir, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("generate output:\n%s", output))

	sql := string(output)
	leftCreate := outputStatementContaining(sql, `CREATE TABLE "left_nodes"`)
	rightCreate := outputStatementContaining(sql, `CREATE TABLE "right_nodes"`)
	c.Assert(leftCreate, qt.Not(qt.Equals), "")
	c.Assert(rightCreate, qt.Not(qt.Equals), "")
	c.Assert(leftCreate, qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(rightCreate, qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(sql, qt.Contains, `ALTER TABLE "left_nodes" ADD CONSTRAINT "fk_left_nodes_right_id" FOREIGN KEY ("right_id") REFERENCES "right_nodes"("id")`)
	c.Assert(sql, qt.Contains, `ALTER TABLE "right_nodes" ADD CONSTRAINT "fk_right_nodes_left_id" FOREIGN KEY ("left_id") REFERENCES "left_nodes"("id")`)
}

func TestGenerateCommand_JsonEmbeddedFieldRendersOnce(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "023-go-annotations-objects")
	cmd := exec.Command("go", "run", "../main.go", "schema", "render", "--root-dir", fixtureDir, "--dialect", "postgres")
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("generate output:\n%s", output))

	usersCreate := outputStatementContaining(string(output), `CREATE TABLE "users"`)
	c.Assert(usersCreate, qt.Not(qt.Equals), "")
	c.Assert(strings.Count(usersCreate, `"metadata" JSONB NOT NULL`), qt.Equals, 1)
}
