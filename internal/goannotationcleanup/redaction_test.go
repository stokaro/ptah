package goannotationcleanup_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// secretValue is the credential these tests plant. Assertions never compare
// against it with a matcher that would echo it into failure output; they assert
// a boolean instead, so a regression reports "value is not false" rather than
// printing the password into CI logs.
//
//nolint:gosec // G101: fixture password, deliberately fake.
const secretValue = `p@ss w"rd\ń`

// leaks reports whether text still contains the credential. It exists so the
// assertion can be boolean: quicktest prints matcher arguments on failure, so
// asserting qt.Not(qt.Contains) with the password would defeat the very
// property under test.
func leaks(text, password string) bool {
	return strings.Contains(text, password)
}

func roleModelSource(password string) string {
	return `package models

//ptah:schema:role name="app_user" login="true" password="` + password + `" superuser="false"
const _ = 0

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`
}

func planRoleModel(c *qt.C, password string) string {
	c.Helper()
	dir := c.TempDir()
	path := filepath.Join(dir, "model.go")
	c.Assert(os.WriteFile(path, []byte(roleModelSource(password)), 0o600), qt.IsNil)

	plan, err := planDir(dir)
	c.Assert(err, qt.IsNil)
	results := plan.DiffResults()
	c.Assert(results, qt.HasLen, 1)
	return results[0].Diff
}

func TestCleanupDiffRedactsRolePassword(t *testing.T) {
	//nolint:gosec // G101: fixture passwords, deliberately fake, never real credentials.
	tests := []struct {
		name     string
		password string
	}{
		{name: "plain", password: "SUPERSECRET"},
		{name: "escaped", password: `p@ss w"rd`},
		{name: "unicode", password: "pass-é-é"},
		{name: "mixed", password: secretValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := planRoleModel(c, tt.password)

			// leaks() rather than qt.Not(qt.Contains): that matcher prints its
			// argument on failure, which would put the credential in CI output.
			c.Assert(leaks(diff, tt.password), qt.IsFalse)
			c.Assert(diff, qt.Contains, "password=***")
		})
	}
}

func TestCleanupDiffKeepsNonSensitiveAttributesReadable(t *testing.T) {
	c := qt.New(t)

	diff := planRoleModel(c, "SUPERSECRET")

	// The diff must still identify the file, directive and the other
	// attributes; only the credential is masked.
	c.Assert(diff, qt.Contains, "model.go")
	c.Assert(diff, qt.Contains, `-//ptah:schema:role name="app_user" login="true" password=*** superuser="false"`)
	c.Assert(diff, qt.Contains, `-//ptah:schema:table name="users"`)
}

func TestCleanupWritesOriginalBytesDespiteRedactedDiff(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	path := filepath.Join(dir, "model.go")
	original := roleModelSource("SUPERSECRET")
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := planDir(dir)
	c.Assert(err, qt.IsNil)

	// Planning is display-only: the source keeps the exact bytes, and the
	// redaction never reaches the file that cleanup rewrites.
	before, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(before), qt.Equals, original)

	c.Assert(plan.Apply(), qt.IsNil)

	after, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(after), qt.Not(qt.Contains), "***")
	c.Assert(string(after), qt.Not(qt.Contains), "ptah:schema:role")
}
