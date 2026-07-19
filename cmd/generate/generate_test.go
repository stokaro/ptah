package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
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

func TestLoadSchemaFile_RejectsUnsupportedExtension(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.json")
	c.Assert(os.WriteFile(path, []byte(`{}`), 0o600), qt.IsNil)

	_, err := loadSchema("", path)
	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension ".json": only .yaml, .yml, and .hcl are supported`)
}

func TestGenerateCommand_MutualForeignKeysAreTwoPhase(t *testing.T) {
	c := qt.New(t)

	fixtureDir := filepath.Join("..", "..", "integration", "fixtures", "entities", "029-roundtrip-mutual-cycle")
	cmd := exec.Command("go", "run", "../main.go", "generate", "--root-dir", fixtureDir, "--dialect", "postgres")
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
