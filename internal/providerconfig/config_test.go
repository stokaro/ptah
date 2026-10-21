package providerconfig_test

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
)

func installation(c *qt.C, id, target string) providerconfig.Installation {
	c.Helper()
	executable, err := filepath.Abs("provider")
	c.Assert(err, qt.IsNil)
	return providerconfig.Installation{Spec: providerhost.Spec{ProviderID: id, Executable: executable, SHA256: strings.Repeat("a", 64)}, Engines: []providerconfig.Engine{{ID: target}}}
}

func TestManifestRejectsAmbiguousOrUntrustedDeclarations(t *testing.T) {
	tests := []struct {
		name   string
		change func(*providerconfig.Configuration)
		want   string
	}{
		{"old format", func(v *providerconfig.Configuration) { v.FormatVersion = 1 }, "format_version 2"},
		{"relative executable", func(v *providerconfig.Configuration) { v.Providers[0].Executable = "provider" }, "absolute executable"},
		{"missing checksum", func(v *providerconfig.Configuration) { v.Providers[0].SHA256 = "" }, "SHA-256"},
		{"invalid checksum", func(v *providerconfig.Configuration) { v.Providers[0].SHA256 = strings.Repeat("z", 64) }, "SHA-256"},
		{"missing engines", func(v *providerconfig.Configuration) { v.Providers[0].Engines = nil }, "declare its engines"},
		{"duplicate provider", func(v *providerconfig.Configuration) { v.Providers = append(v.Providers, v.Providers[0]) }, "unique nonempty provider"},
		{"empty provider", func(v *providerconfig.Configuration) { v.Providers[0].ProviderID = "" }, "unique nonempty provider"},
		{"duplicate alias", func(v *providerconfig.Configuration) { v.Providers[0].Engines[0].Aliases = []string{"custom"} }, "multiple declarations"},
		{"reserved alias", func(v *providerconfig.Configuration) { v.Providers[0].Engines[0].Aliases = []string{"file"} }, "invalid or reserved"},
		{"uppercase name", func(v *providerconfig.Configuration) { v.Providers[0].Engines[0].ID = "Custom" }, "invalid or reserved"},
		{"official impersonation", func(v *providerconfig.Configuration) { v.Providers[0].Engines[0].ID = "ydb" }, "official owner"},
		{"official alias remapping", func(v *providerconfig.Configuration) {
			v.Providers[0].ProviderID = "github.com/stokaro/ptah-ydb-plugin"
			v.Providers[0].Engines[0].Aliases = []string{"ydbs"}
		}, "must resolve to"},
		{"unused disabled conflict", func(v *providerconfig.Configuration) {
			other := v.Providers[0]
			other.ProviderID = "example.org/unused"
			other.Disabled = true
			v.Providers = append(v.Providers, other)
		}, "multiple declarations"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			config := providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion, Providers: []providerconfig.Installation{installation(c, "example.org/custom", "custom")}}
			test.change(&config)
			data, err := json.Marshal(config)
			c.Assert(err, qt.IsNil)
			_, err = providerconfig.Decode(data)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
		})
	}
}

func TestManifestRequiresOneStrictBoundedDocument(t *testing.T) {
	for _, test := range []struct{ document, want string }{
		{`{"format_version":2,"providers":[],"extra":true}`, "unknown field"},
		{`{"format_version":2,"providers":[]} {}`, "one JSON document"},
		{`{"format_version":2,"providers":[]}` + strings.Repeat(" ", 1024*1024), "exceeds 1 MiB"},
		{`{"format_version":`, "parse provider manifest"},
	} {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			_, err := providerconfig.Decode([]byte(test.document))
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
		})
	}
}

func TestSelectionDistinguishesMissingDisabledAndUnknown(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		t.Run(entry.EngineID, func(t *testing.T) {
			c := qt.New(t)
			config := providerconfig.Configuration{}
			_, _, err := config.Select(entry.EngineID)
			diagnostic, ok := errors.AsType[*providerdiagnostic.Diagnostic](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(diagnostic.Code, qt.Equals, "provider_missing")
			c.Assert(diagnostic.SetupURL, qt.Equals, entry.SetupURL)
			selected := installation(c, entry.ProviderID, entry.EngineID)
			selected.Engines[0].Aliases = entry.Aliases
			config.Providers = []providerconfig.Installation{selected}
			actual, target, err := config.Select(entry.ProviderID)
			c.Assert(err, qt.IsNil)
			c.Assert(actual.ProviderID, qt.Equals, entry.ProviderID)
			c.Assert(target, qt.Equals, entry.EngineID)
			config.Providers[0].Disabled = true
			_, _, err = config.Select(entry.EngineID)
			diagnostic, ok = errors.AsType[*providerdiagnostic.Diagnostic](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(diagnostic.Code, qt.Equals, "provider_disabled")
		})
	}
	c := qt.New(t)
	_, _, err := (providerconfig.Configuration{}).Select("unknown://private-secret")
	c.Assert(err, qt.ErrorMatches, "no configured provider owns the selected engine")
}

func TestThirdPartySelectionNeedsNoCatalogEntry(t *testing.T) {
	c := qt.New(t)
	selected := installation(c, "example.org/custom", "custom")
	selected.Engines[0].Aliases = []string{"customs"}
	config := providerconfig.Configuration{Providers: []providerconfig.Installation{selected}}
	actual, target, err := config.Select("customs")
	c.Assert(err, qt.IsNil)
	c.Assert(target, qt.Equals, "custom")
	c.Assert(actual.ProviderID, qt.Equals, selected.ProviderID)
}
