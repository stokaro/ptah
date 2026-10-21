// Package providercatalog identifies official executable providers without
// loading implementations, opening databases, or granting execution permission.
package providercatalog

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// Entry contains recognition and setup metadata, never engine capabilities.
// BundledByDefault describes distribution policy, not an installed artifact.
type Entry struct {
	EngineID         string   `json:"engine_id"`
	DisplayName      string   `json:"display_name"`
	Aliases          []string `json:"aliases"`
	URLSchemes       []string `json:"url_schemes"`
	ProviderID       string   `json:"provider_id"`
	RepositoryURL    string   `json:"repository_url"`
	SetupURL         string   `json:"setup_url"`
	BundledByDefault bool     `json:"bundled_by_default"`
}

var entries = []Entry{
	{
		EngineID: "duckdb", DisplayName: "DuckDB", Aliases: make([]string, 0), URLSchemes: []string{"duckdb"},
		ProviderID: "github.com/stokaro/ptah-duckdb-plugin", RepositoryURL: "https://github.com/stokaro/ptah-duckdb-plugin",
		SetupURL: "https://docs.ptah.run/edge/extend/providers/duckdb/",
	},
	{
		EngineID: "spanner", DisplayName: "Spanner", Aliases: []string{"cloudspanner", "google-spanner", "google_spanner"},
		URLSchemes: []string{"spanner", "cloudspanner", "google-spanner"},
		ProviderID: "github.com/stokaro/ptah-spanner-plugin", RepositoryURL: "https://github.com/stokaro/ptah-spanner-plugin",
		SetupURL: "https://docs.ptah.run/edge/extend/providers/spanner/",
	},
	{
		EngineID: "ydb", DisplayName: "YDB", Aliases: []string{"ydbs"}, URLSchemes: []string{"ydb", "ydbs"},
		ProviderID: "github.com/stokaro/ptah-ydb-plugin", RepositoryURL: "https://github.com/stokaro/ptah-ydb-plugin",
		SetupURL: "https://docs.ptah.run/edge/extend/providers/ydb/", BundledByDefault: true,
	},
}

// Entries returns private copies of the official recognition records.
func Entries() []Entry {
	result := make([]Entry, len(entries))
	for i, entry := range entries {
		result[i] = clone(entry)
	}
	return result
}

// Lookup recognizes an explicit engine name, provider ID, database URL, or
// docker dev-database URL. It does not infer an engine from SQL or a filename.
func Lookup(selector string) (Entry, bool) {
	name := strings.ToLower(strings.TrimSpace(selector))
	for _, entry := range entries {
		if name == entry.EngineID || name == entry.ProviderID || slices.Contains(entry.Aliases, name) {
			return clone(entry), true
		}
	}
	parsed, err := url.Parse(name)
	if err != nil || parsed.Scheme == "" {
		return Entry{}, false
	}
	if parsed.Scheme == "docker" || strings.HasPrefix(parsed.Scheme, "docker+") {
		return Lookup(parsed.Hostname())
	}
	for _, entry := range entries {
		if slices.Contains(entry.URLSchemes, parsed.Scheme) {
			return clone(entry), true
		}
	}
	return Entry{}, false
}

// CheckOwnership refuses another provider's claim to an official engine or
// alias. Third-party engine names need no catalog entry or host release.
func CheckOwnership(providerID, engineID string, aliases []string) error {
	names := append([]string{engineID}, aliases...)
	for _, name := range names {
		entry, known := Lookup(name)
		if known && engineID != entry.EngineID {
			return fmt.Errorf("official engine name %q must resolve to %q", name, entry.EngineID)
		}
		if known && entry.ProviderID != providerID {
			return fmt.Errorf("provider %q cannot claim %q: the official owner is %q", providerID, name, entry.ProviderID)
		}
	}
	return nil
}

func clone(entry Entry) Entry {
	entry.Aliases = slices.Clone(entry.Aliases)
	entry.URLSchemes = slices.Clone(entry.URLSchemes)
	return entry
}
