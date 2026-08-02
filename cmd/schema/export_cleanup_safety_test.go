package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/schema"
)

// cliSecret is the credential planted in the compiled-command tests. Assertions
// against it are boolean so a regression never prints it into CI output.
const cliSecret = `s3cr3t "quoted" \pass`

// decomposedAccentCLI is "e" followed by U+0301, written as an escape so a
// normalizing editor cannot silently compose it and make the test vacuous.
const decomposedAccentCLI = "e\u0301"

// secretLeaked keeps the credential out of failure output: quicktest prints
// matcher arguments, so qt.Not(qt.Contains) with the password would leak it.
func secretLeaked(text, password string) bool {
	return strings.Contains(text, password)
}

func writeRoleModel(c *qt.C, dir, password string) string {
	c.Helper()
	path := filepath.Join(dir, "model.go")
	source := `package models

//ptah:schema:role name="app_user" login="true" password="` + password + `"
const _ = 0

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
	return path
}

func runSchemaExportCleanup(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := schema.NewSchemaCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"export"}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func TestSchemaExportCleanupDiffRedactsRolePassword(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRoleModel(c, dir, cliSecret)
	outPath := filepath.Join(dir, "schema.hcl")

	stdout, stderr, err := runSchemaExportCleanup(c,
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
		"--cleanup-diff",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(secretLeaked(stdout, cliSecret), qt.IsFalse)
	c.Assert(secretLeaked(stderr, cliSecret), qt.IsFalse)
	// cliSecret carries a raw quote, so the delimiters are ambiguous and the
	// mask correctly runs to end of line. Asserting the exact line means a
	// partial mask cannot pass, and the expectation holds no fixture bytes.
	c.Assert(stdout, qt.Contains, `-//ptah:schema:role name="app_user" login="true" password=***`)
}

func TestSchemaExportRefusesCleanupOnNonNFCValue(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	source := `package models

//ptah:schema:function name="greet" params="" returns="text" language="sql" body="SELECT 'Caf` +
		decomposedAccentCLI + `'"
const _ = 0

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
	c.Assert(os.WriteFile(modelPath, []byte(source), 0o600), qt.IsNil)
	outPath := filepath.Join(dir, "schema.hcl")

	_, stderr, err := runSchemaExportCleanup(c,
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains, "is not Unicode NFC")

	// The source keeps the only copy of the exact bytes and no HCL was published.
	content, readErr := os.ReadFile(modelPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Equals, source)
	_, statErr := os.Stat(outPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
