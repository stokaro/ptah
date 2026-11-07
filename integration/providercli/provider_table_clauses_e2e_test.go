//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/pathguard"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/sqlschema"
	"ptah.run/internal/testutils"
)

// tableClausesSpec builds the fixture command that reads the TTL clause of a
// CREATE TABLE in its SQL documents itself, from the SDK version the host
// pins.
func tableClausesSpec(c *qt.C) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "table-clauses")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"}
}

// TestProviderReadsATableClauseOfASQLDocument reads a SQL document of the
// fixture engine with the shared grammar: the provider reads the TTL clause of
// each table, the clauses of one document in one call, and the facet it
// decodes equals the retention the same table declares in YAML, bound to the
// engine. A table without the clause holds none, and the document claims the
// retention model whole, which is what makes a clause removed from it a change.
func TestProviderReadsATableClauseOfASQLDocument(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-clauses")
	text := "CREATE TABLE items (id INTEGER PRIMARY KEY) TTL 2 MINUTES;\nCREATE TABLE kept (id INTEGER PRIMARY KEY);\n" +
		"CREATE TABLE logs (id INTEGER PRIMARY KEY)\n  ttl 30 seconds;\n"

	database, _, err := sqlschema.ReadWith(c.Context(), []byte(text), "fixture", nil, sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 3)
	declared, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, primary: true}\n"+
		"    retention: {seconds: 120}\n  logs:\n    columns:\n      id: {type: INTEGER, primary: true}\n    retention: {seconds: 30}\n"))
	c.Assert(err, qt.IsNil)
	items, kept, logs := database.Tables[0].Facets, database.Tables[1].Facets, database.Tables[2].Facets
	c.Assert(items.Kinds(), qt.DeepEquals, []schemaext.Kind{retentionFacet})
	c.Assert(items.TargetScope(retentionFacet), qt.DeepEquals, []string{"fixture"})
	c.Assert(items.Equal(declared.Tables[0].Facets), qt.IsTrue)
	c.Assert(logs.Equal(declared.Tables[1].Facets), qt.IsTrue)
	c.Assert(kept.Len(), qt.Equals, 0)
	records := database.FeatureCoverage.KindRecords()
	c.Assert(records, qt.HasLen, 1)
	c.Assert(records[0].Model.Kind, qt.Equals, retentionFacet)
	c.Assert(records[0].Model.Representation, qt.Equals, schemaext.Desired)
	c.Assert(records[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Complete)
}

// TestTableClauseOfAnotherTargetReadsAsBefore reads the same TTL clause in a
// document that names no engine, where no claim applies: the shared grammar
// reads it as ClickHouse's table option, and the table holds no facet of the
// provider's.
func TestTableClauseOfAnotherTargetReadsAsBefore(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "table-clauses")

	database, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE TABLE items (id INTEGER PRIMARY KEY) TTL 2 MINUTES;\n"), "", nil, sqlschema.Options{Owners: runtime.SQL()})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Facets.Len(), qt.Equals, 0)
	c.Assert(database.FeatureCoverage.KindRecords(), qt.HasLen, 0)
}

// TestNativeCLIRefusesATableClauseTheProviderRefuses_FailurePath plans a SQL
// schema whose TTL clause the provider refuses: the refusal names the file,
// the line and the column of the clause, as the document's reader counts
// them, and the provider's reason, and nothing is planned. The second row puts
// characters of two bytes before the clause, where a column counted in bytes
// and one counted in characters differ; the third ends its lines with CRLF.
func TestNativeCLIRefusesATableClauseTheProviderRefuses_FailurePath(t *testing.T) {
	spec := tableClausesSpec(qt.New(t))
	for _, test := range []struct {
		name, text, at string
	}{
		{"a unit the provider does not read", "CREATE TABLE items (id INTEGER NOT NULL)\n  TTL 2 HOURS;\n", ":2:3:"},
		{"after characters of two bytes", "CREATE TABLE заказы (id INTEGER NOT NULL) TTL 2 HOURS;\n", ":1:43:"},
		{"lines that end with CRLF", "CREATE TABLE kept (id INTEGER NOT NULL) TTL 2 MINUTES;\r\nCREATE TABLE items (id INTEGER NOT NULL)\r\n TTL 0 SECONDS;\r\n", ":3:2:"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			schema := filepath.Join(filepath.Dir(f.schemaPath), "schema.sql")
			writeSQLSchema(c, filepath.Dir(schema), map[string]string{"schema.sql": test.text})
			resolved, err := pathguard.ResolveCLIPath(schema)
			c.Assert(err, qt.IsNil)

			_, diagnostics, code := f.run(c, c.Context(), "plan", "--schema-file", schema, "--output", f.planPath, "--json")

			c.Assert(code, qt.Equals, 2)
			c.Assert(diagnostics, qt.Contains, "read SQL schema file "+resolved+test.at+" table clause of ")
			c.Assert(diagnostics, qt.Contains, "a TTL clause is TTL <n> SECONDS or TTL <n> MINUTES")
			_, err = os.Stat(f.planPath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}
