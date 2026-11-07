//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
)

// sqlLexicalSpec builds the fixture command whose engine reads its SQL
// documents with the shared grammar, from the SDK version the host pins:
// lexical "postgres" states a PostgreSQL-family engine's lexical rules in its
// query dialect, and "none" states none.
func sqlLexicalSpec(c *qt.C, lexical string) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "sql-lexical")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"FIXTURE_LEXICAL=" + lexical}}
}

// lexicalSchema is a SQL schema whose first comment holds a comment and a
// semicolon after it: one comment to a PostgreSQL-family engine, and a
// comment that ends at the first */ by the SQL standard's rules.
const lexicalSchema = "/* a /* nested */ comment; with a semicolon */\nCREATE TABLE notes (id INTEGER NOT NULL, note TEXT);\n"

// TestNativeCLIReadsAProviderEnginesTextByItsLexicalRules plans and applies a
// SQL schema with a nested block comment for an engine whose provider states
// a PostgreSQL-family engine's lexical rules: the shared grammar reads the
// comment whole, and the table the schema declares is created and read back.
func TestNativeCLIReadsAProviderEnginesTextByItsLexicalRules(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlLexicalSpec(c, "postgres"))
	dir := filepath.Dir(f.schemaPath)
	writeSQLSchema(c, dir, map[string]string{"schema.sql": lexicalSchema})

	planned := f.success(c, "plan", "--schema-file", filepath.Join(dir, "schema.sql"), "--output", f.planPath, "--json")
	applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")

	c.Assert(planned, qt.Contains, `"outcome": "changes"`)
	c.Assert(applied, qt.Contains, `"outcome": "applied"`)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "notes"`)
}

// TestNativeCLIReadsAProviderEnginesTextByTheStandard_FailurePath plans the
// same schema for an engine whose provider states no lexical rules: the
// comment ends at the first */, the text after it is read as a statement and
// refused, and nothing is planned.
func TestNativeCLIReadsAProviderEnginesTextByTheStandard_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlLexicalSpec(c, "none"))
	dir := filepath.Dir(f.schemaPath)
	writeSQLSchema(c, dir, map[string]string{"schema.sql": lexicalSchema})

	_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", filepath.Join(dir, "schema.sql"), "--output", f.planPath, "--json")

	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, "schema.sql:1:26: expected ON after COMMENT")
	_, err := os.Stat(f.planPath)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
}
