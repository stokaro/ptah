package providercatalog_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
)

func TestCatalogRecognizesExplicitSelectors(t *testing.T) {
	for _, test := range []struct{ selector, engine string }{
		{" YDB ", "ydb"}, {"ydbs://user:secret@host/db", "ydb"},
		{"docker://ydb/26", "ydb"}, {"duckdb:/tmp/example.db", "duckdb"},
		{"duckdb://file.db", "duckdb"}, {"docker://duckdb/latest", "duckdb"},
		{"cloudspanner", "spanner"}, {"spanner://user:secret@host/db", "spanner"},
		{"docker://spanner/emulator", "spanner"}, {"github.com/stokaro/ptah-spanner-plugin", "spanner"},
		{"postgres://host/spanner", ""}, {"file://ydb/schema.sql", ""},
		{"CREATE TABLE ydb (id INT)", ""}, {"unknown", ""}, {"duckdb.sql", ""},
	} {
		t.Run(test.selector, func(t *testing.T) {
			c := qt.New(t)
			entry, found := providercatalog.Lookup(test.selector)
			c.Assert(found, qt.Equals, test.engine != "")
			c.Assert(entry.EngineID, qt.Equals, test.engine)
		})
	}
}

func TestCatalogOwnershipAndCopies(t *testing.T) {
	c := qt.New(t)
	c.Assert(providercatalog.CheckOwnership("example.org/third-party", "custom", []string{"customs"}), qt.IsNil)
	c.Assert(providercatalog.CheckOwnership("github.com/stokaro/ptah-ydb-plugin", "ydb", []string{"ydbs"}), qt.IsNil)
	c.Assert(providercatalog.CheckOwnership("example.org/third-party", "ydb", []string{"ydbs"}), qt.ErrorMatches, `.*official owner.*ptah-ydb-plugin.*`)
	first, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	first.Aliases[0] = "changed"
	all := providercatalog.Entries()
	all[0].URLSchemes[0] = "changed"
	ydb, found := providercatalog.Lookup("ydbs")
	c.Assert(found, qt.IsTrue)
	c.Assert(ydb.Aliases, qt.DeepEquals, []string{"ydbs"})
	duckdb, found := providercatalog.Lookup("duckdb://example.db")
	c.Assert(found, qt.IsTrue)
	c.Assert(duckdb.URLSchemes, qt.DeepEquals, []string{"duckdb"})
}

func TestMissingDiagnosticPreservesGuidance(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		t.Run(entry.EngineID, func(t *testing.T) {
			c := qt.New(t)
			diagnostic := providercatalog.Missing(entry)
			data, err := json.Marshal(diagnostic)
			c.Assert(err, qt.IsNil)
			var decoded providerdiagnostic.Diagnostic
			c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, *diagnostic)
			c.Assert(decoded.Code, qt.Equals, "provider_missing")
			c.Assert(decoded.EngineID, qt.Equals, entry.EngineID)
			c.Assert(decoded.ProviderID, qt.Equals, entry.ProviderID)
			c.Assert(decoded.RepositoryURL, qt.Equals, entry.RepositoryURL)
			c.Assert(decoded.SetupURL, qt.Equals, entry.SetupURL)
			c.Assert(decoded.Error(), qt.Contains, "supported through")
			c.Assert(decoded.Error(), qt.Contains, "no configured installation")
			c.Assert(decoded.Error(), qt.Contains, entry.SetupURL)
		})
	}
}

func TestOfficialOwnersRegisterWithTheEngineRuntime(t *testing.T) {
	c := qt.New(t)
	for _, entry := range providercatalog.Entries() {
		_, err := engine.New(engine.Provider{ID: entry.ProviderID, Targets: []engine.Target{{Name: entry.EngineID}}})
		c.Assert(err, qt.IsNil, qt.Commentf("%s", entry.EngineID))
	}
	c.Assert(providercatalog.CheckOwnership("stokaro/ptah-ydb-plugin", "ydb", nil), qt.IsNotNil)
}
