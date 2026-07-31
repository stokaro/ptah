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

// redactedRoleLine is the removed line a well-formed fixture produces: only the
// value is masked and the remaining attributes stay readable.
const redactedRoleLine = `-//ptah:schema:role name="app_user" login="true" password=*** superuser="false"`

// redactedToEndOfLine is what an ambiguously delimited value produces. A raw
// quote inside the value makes the attribute end where the value does not, so
// everything after it may still be the secret and the mask runs to end of line.
const redactedToEndOfLine = `-//ptah:schema:role name="app_user" login="true" password=***`

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
		want     string
	}{
		{name: "plain", password: "SUPERSECRET", want: redactedRoleLine},
		// A properly escaped quote keeps the attribute well-formed, so the
		// trailing attributes survive the mask.
		{name: "escaped", password: `p@ss w\"rd`, want: redactedRoleLine},
		{name: "unicode", password: "pass-é-é", want: redactedRoleLine},
		{name: "backslash", password: `trailing\\`, want: redactedRoleLine},
		// A raw quote makes the delimiters ambiguous, so masking must not stop
		// at the fragment the attribute regex happened to recognize.
		{name: "unescaped quote", password: `p@ss w"rd`, want: redactedToEndOfLine},
		{name: "mixed", password: secretValue, want: redactedToEndOfLine},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := planRoleModel(c, tt.password)

			// leaks() rather than qt.Not(qt.Contains): that matcher prints its
			// argument on failure, which would put the credential in CI output.
			c.Assert(leaks(diff, tt.password), qt.IsFalse)
			// Exact line, not just "contains ***": a partially masked value
			// leaves a residue that a containment check happily accepts. The
			// expected text carries no fixture-derived bytes, so a regression
			// cannot print the secret.
			c.Assert(diff, qt.Contains, tt.want)
		})
	}
}

// TestCleanupDiffRedactsUnrecognizedDirectiveSpellings pins the rule that
// redaction is directive-INDEPENDENT. core/goschema builds a live role from a
// bare "//ptah:schema:role" prefix, so these spellings still export a role with
// its password; gating the mask on directive recognition printed them in clear.
func TestCleanupDiffRedactsUnrecognizedDirectiveSpellings(t *testing.T) {
	tests := []struct {
		name string
		line string
	}{
		{name: "suffixed", line: `//ptah:schema:roles name="a" password="%s"`},
		{name: "colon suffixed", line: `//ptah:schema:role:app name="a" password="%s"`},
		{name: "text before slashes", line: `/* ops */ //ptah:schema:role name="a" password="%s"`},
		{name: "non-ascii space", line: "//\u00a0ptah:schema:role name=\"a\" password=\"%s\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			source := "package models\n\n" +
				strings.Replace(tt.line, "%s", "hunter2", 1) + "\ntype AppRoles struct{}\n\n" +
				`//ptah:schema:table name="users"` + "\ntype User struct {\n" +
				"\t" + `//ptah:schema:field name="id" type="SERIAL" primary="true"` + "\n\tID int64\n}\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)

			plan, err := planDir(dir)
			c.Assert(err, qt.IsNil)

			for _, result := range plan.DiffResults() {
				c.Assert(leaks(result.Diff, "hunter2"), qt.IsFalse)
			}
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

	// A role annotation inside a raw string literal is not schema intent, so
	// cleanup leaves it alone. That makes the assertion load-bearing: the
	// credential must survive in the file byte-for-byte even though the diff
	// masked it. Asserting only on a removed line would be tautological, since
	// removed text is absent whatever redaction does.
	original := "package models\n\nconst doc = `\n" +
		`//ptah:schema:role name="app_user" login="true" password="` + secretValue + `" superuser="false"` +
		"\n`\n\n" +
		`//ptah:schema:table name="users"` + "\ntype User struct {\n" +
		"\t" + `//ptah:schema:field name="id" type="SERIAL" primary="true"` + "\n\tID int64\n}\n"
	c.Assert(os.WriteFile(path, []byte(original), 0o600), qt.IsNil)

	plan, err := planDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Apply(), qt.IsNil)

	after, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	// The untouched literal keeps the exact original bytes, credential included.
	c.Assert(leaks(string(after), secretValue), qt.IsTrue)
	c.Assert(string(after), qt.Not(qt.Contains), "***")
	// The real annotations were still removed.
	c.Assert(string(after), qt.Not(qt.Contains), `//ptah:schema:table`)
}
