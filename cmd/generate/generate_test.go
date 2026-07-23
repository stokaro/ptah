package generate_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/generate"
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

func TestGenerateCommandUnsupportedDialectExits2WithoutPanicTrace(t *testing.T) {
	c := qt.New(t)

	cmd := generate.NewGenerateCommand()
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
