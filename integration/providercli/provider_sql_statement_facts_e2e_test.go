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

// sqlStatementsSpec builds the fixture command whose engine reads its SQL
// documents with the shared grammar and states its server's statement facts,
// from the SDK version the host pins: facts "skips" states that DROP VIEW IF
// EXISTS of a table's name does nothing, and "none" states every fact false.
func sqlStatementsSpec(c *qt.C, facts string) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "sql-statements")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture",
		Environment: []string{"FIXTURE_STATEMENT_FACTS=" + facts}}
}

// TestNativeCLIReadsADropViewIfExistsTheServerSkips plans and applies a SQL
// schema that drops a view under the name of a table it declares, with IF
// EXISTS, for an engine whose provider states that its server does nothing
// for that statement: the table stays, as the server leaves it, and is
// created.
func TestNativeCLIReadsADropViewIfExistsTheServerSkips(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, sqlStatementsSpec(c, "skips"))
	dir := filepath.Dir(f.schemaPath)
	writeSQLSchema(c, dir, map[string]string{"schema.sql": "CREATE TABLE t (id INTEGER NOT NULL);\nDROP VIEW IF EXISTS t;\n"})
	schema := filepath.Join(dir, "schema.sql")

	planned := f.success(c, "plan", "--schema-file", schema, "--output", f.planPath, "--json")
	applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")

	c.Assert(planned, qt.Contains, `"outcome": "changes"`)
	c.Assert(applied, qt.Contains, `"outcome": "applied"`)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "t"`)
}

// TestNativeCLIRefusesADropViewTheServerRefuses_FailurePath plans a SQL schema
// that drops a view under the name of a table it declares. The engine whose
// provider states every statement fact false refuses it with IF EXISTS, as
// its server does, and every engine refuses it without IF EXISTS; nothing is
// planned.
func TestNativeCLIRefusesADropViewTheServerRefuses_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, facts, drop, want string
	}{
		{"IF EXISTS on an engine that refuses it", "none", "DROP VIEW IF EXISTS t;", "DROP VIEW t: the schema declares t as another kind of object"},
		{"no IF EXISTS on an engine that skips the guarded statement", "skips", "DROP VIEW t;", "DROP VIEW t: the schema declares t as another kind of object"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, sqlStatementsSpec(c, test.facts))
			dir := filepath.Dir(f.schemaPath)
			writeSQLSchema(c, dir, map[string]string{"schema.sql": "CREATE TABLE t (id INTEGER NOT NULL);\n" + test.drop + "\n"})

			_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", filepath.Join(dir, "schema.sql"), "--output", f.planPath, "--json")

			c.Assert(code, qt.Equals, 2)
			c.Assert(diagnostics, qt.Contains, test.want)
			_, err := os.Stat(f.planPath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}
