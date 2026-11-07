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
			c.Assert(decoded.Error(), qt.Contains, "installed separately, and no installation was found")
			c.Assert(decoded.Error(), qt.Contains, "Run `ptah providers install "+entry.EngineID+"`")
			c.Assert(decoded.Error(), qt.Contains, entry.SetupURL)
		})
	}
}

func TestEveryEntryNamesAGitHubReleaseSource(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		t.Run(entry.EngineID, func(t *testing.T) {
			c := qt.New(t)
			owner, repo, found := entry.GitHubRepository()
			c.Assert(found, qt.IsTrue)
			c.Assert("github.com/"+owner+"/"+repo, qt.Equals, entry.ProviderID)
		})
	}
}

func TestGitHubRepository_FailurePath(t *testing.T) {
	c := qt.New(t)
	for _, address := range []string{"", "https://gitlab.com/stokaro/ptah-ydb-plugin", "https://github.com/stokaro", "https://github.com/stokaro/ptah/sub", "http://github.com/stokaro/ptah"} {
		owner, repo, found := providercatalog.Entry{RepositoryURL: address}.GitHubRepository()
		c.Assert(found, qt.IsFalse, qt.Commentf("%q", address))
		c.Assert(owner+repo, qt.Equals, "")
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

// An official engine is provider-only while no built-in dialect serves it, and
// no built-in dialect serves any of the three.
func TestProviderOnly(t *testing.T) {
	for _, test := range []struct{ selector, engine string }{
		{"duckdb", "duckdb"}, {" DuckDB ", "duckdb"}, {"duckdb:///tmp/app.db", "duckdb"}, {"docker://duckdb/1", "duckdb"},
		{"github.com/stokaro/ptah-duckdb-plugin", "duckdb"},
		{"ydb", "ydb"}, {"ydbs://host/db", "ydb"}, {"docker://ydb/26", "ydb"}, {"github.com/stokaro/ptah-ydb-plugin", "ydb"},
		{"spanner", "spanner"}, {"cloudspanner://host/db", "spanner"}, {"google_spanner", "spanner"},
		{"github.com/stokaro/ptah-spanner-plugin", "spanner"},
		{"postgres", ""}, {"sqlite:///tmp/app.db", ""}, {"unknown", ""}, {"", ""},
	} {
		t.Run(test.selector, func(t *testing.T) {
			c := qt.New(t)
			entry, found := providercatalog.ProviderOnly(test.selector)
			c.Assert(found, qt.Equals, test.engine != "")
			c.Assert(entry.EngineID, qt.Equals, test.engine)
		})
	}
}

// A banner names Spanner by its token in any case, and nothing else is
// identified: PostgreSQL, CockroachDB and YugabyteDB banners are the built-in
// engines' to read, and the emulator's plain PostgreSQL banner names no
// product.
func TestIdentifyBanner(t *testing.T) {
	for _, test := range []struct{ banner, engine string }{
		{"PostgreSQL 14.1 on Cloud Spanner", "spanner"},
		{"Cloud Spanner PostgreSQL interface", "spanner"},
		{"SPANNER", "spanner"},
		{"PostgreSQL 14.1", ""},
		{"PostgreSQL 17.5 (Debian 17.5-1.pgdg120+1)", ""},
		{"CockroachDB CCL v25.4.0", ""},
		{"PostgreSQL 15.12-YB-2025.2.0.0-b0", ""},
		{"", ""},
	} {
		t.Run(test.banner, func(t *testing.T) {
			c := qt.New(t)
			entry, found := providercatalog.IdentifyBanner(test.banner)
			c.Assert(found, qt.Equals, test.engine != "")
			c.Assert(entry.EngineID, qt.Equals, test.engine)
		})
	}
}

// The diagnostic a connection gives a Spanner server behind a postgres:// URL
// carries the catalog's identities and links in JSON, names the URL scheme
// that selects the plugin, and never says the plugin is missing: the
// connection cannot know.
func TestIdentifiedDiagnosticPreservesGuidance(t *testing.T) {
	c := qt.New(t)
	entry, found := providercatalog.Lookup("spanner")
	c.Assert(found, qt.IsTrue)

	diagnostic := providercatalog.Identified(entry, "postgres")
	data, err := json.Marshal(diagnostic)
	c.Assert(err, qt.IsNil)
	var decoded providerdiagnostic.Diagnostic
	c.Assert(json.Unmarshal(data, &decoded), qt.IsNil)

	c.Assert(decoded, qt.DeepEquals, *diagnostic)
	c.Assert(decoded.Code, qt.Equals, "provider_engine_identified")
	c.Assert(decoded.EngineID, qt.Equals, "spanner")
	c.Assert(decoded.ProviderID, qt.Equals, "github.com/stokaro/ptah-spanner-plugin")
	c.Assert(decoded.RepositoryURL, qt.Equals, entry.RepositoryURL)
	c.Assert(decoded.SetupURL, qt.Equals, entry.SetupURL)
	c.Assert(decoded.Error(), qt.Equals, "The server behind this postgres:// URL reports itself as Spanner. "+
		"Spanner is supported through the official Ptah Spanner plugin, never through the engine a postgres:// URL selects. "+
		"Connect with a spanner:// URL, which selects the plugin; if it is not installed, run `ptah providers install spanner`. "+
		"Setup: https://docs.ptah.run/edge/extend/providers/spanner/. Plugin repository: https://github.com/stokaro/ptah-spanner-plugin.")
}
