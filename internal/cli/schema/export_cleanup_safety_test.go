package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/schema"
	"ptah.run/internal/goannotationcleanup"
	"ptah.run/internal/goannotationexport"
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

//ptah:schema:role name="app_user" login="true" password=` + strconv.Quote(password) + `
type AppRole struct{}

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

// The HCL names a declared password by its variable and holds no value
// (ADR 0071), so the Go annotation holds the only copy, and a cleanup that
// would delete it is refused. Nothing the run prints holds the password, and
// the source keeps it.
func TestSchemaExportCleanupKeepsTheOnlyCopyOfARolePassword_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := writeRoleModel(c, dir, cliSecret)
	before, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	outPath := filepath.Join(dir, "schema.hcl")

	stdout, stderr, err := runSchemaExportCleanup(c,
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
		"--cleanup-diff",
	)

	c.Assert(err, qt.ErrorIs, goannotationexport.ErrLossyCleanup)
	c.Assert(secretLeaked(stdout, cliSecret), qt.IsFalse)
	c.Assert(secretLeaked(stderr, cliSecret), qt.IsFalse)
	c.Assert(secretLeaked(err.Error(), cliSecret), qt.IsFalse)
	c.Assert(err.Error(), qt.Contains, "role app_user: the password is written as $PTAH_SECRET_PASSWORD_APP_USER")
	after, err := os.ReadFile(modelPath)
	c.Assert(err, qt.IsNil)
	c.Assert(bytes.Equal(after, before), qt.IsTrue)
}

// A password the source already names by its variable is no value, so the
// cleanup removes the annotation and the HCL names the same variable; the diff
// masks the attribute all the same.
func TestSchemaExportCleanupDiffRedactsRolePassword_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeRoleModel(c, dir, "$PTAH_SECRET_APP_USER_PASSWORD")
	outPath := filepath.Join(dir, "schema.hcl")

	stdout, _, err := runSchemaExportCleanup(c,
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
		"--cleanup-diff",
	)

	c.Assert(err, qt.IsNil)
	// Asserting the exact line means a partial mask cannot pass.
	c.Assert(stdout, qt.Contains, `-//ptah:schema:role name="app_user" login="true" password=***`)
	exported, err := os.ReadFile(outPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(exported), qt.Contains, `password = "$PTAH_SECRET_APP_USER_PASSWORD"`)
}

func TestSchemaExportRefusesCleanupOnNonNFCValue(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	source := `package models

//ptah:schema:function name="greet" params="" returns="text" language="sql" body="SELECT 'Caf` +
		decomposedAccentCLI + `'"
type GreetingFunction struct{}

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

func TestSchemaExportCleanupRejectsDetachedRecognizedDirective(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.go")
	source := `package models

//ptah:schema:role name="detached_role" login="true"
const marker = 0

//ptah:schema:table name="users"
type User struct{}
`
	c.Assert(os.WriteFile(modelPath, []byte(source), 0o600), qt.IsNil)
	outPath := filepath.Join(dir, "schema.hcl")
	originalOutput := []byte("existing schema\n")
	c.Assert(os.WriteFile(outPath, originalOutput, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExportCleanup(c,
		"--root-dir", dir,
		"--out", outPath,
		"--cleanup-go-annotations",
	)

	c.Assert(err, qt.ErrorIs, goannotationcleanup.ErrUnexportedAnnotation)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, "ptah:schema:role")
	c.Assert(stderr, qt.Contains, "has file scope")
	content, readErr := os.ReadFile(modelPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(content), qt.Equals, source)
	output, readErr := os.ReadFile(outPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(output, qt.DeepEquals, originalOutput)
}
