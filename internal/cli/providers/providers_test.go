package providers_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/providers"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
)

func TestProviderCatalogWithoutInstallations(t *testing.T) {
	c := qt.New(t)
	command := providers.NewCommand()
	var out bytes.Buffer
	command.SetOut(&out)
	command.SetArgs([]string{"list", "--json"})
	c.Assert(command.ExecuteContext(c.Context()), qt.IsNil)
	var report struct {
		Catalog       []providercatalog.Entry
		Installations []any
	}
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Catalog, qt.DeepEquals, providercatalog.Entries())
	c.Assert(report.Installations, qt.HasLen, 0)
}

func TestProviderCheckReportsStructuredMissingInstallation(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		t.Run(entry.EngineID, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("XDG_CACHE_HOME", c.TempDir())
			t.Setenv("HOME", c.TempDir())
			t.Setenv("LocalAppData", c.TempDir())
			command := providers.NewCommand()
			var out, stderr bytes.Buffer
			command.SetOut(&out)
			command.SetErr(&stderr)
			command.SetArgs([]string{"check", "--provider-engine", entry.EngineID, "--json"})
			c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
			var report struct {
				Status     string
				Diagnostic providerdiagnostic.Diagnostic
			}
			c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
			c.Assert(report.Status, qt.Equals, "failed")
			c.Assert(report.Diagnostic.Code, qt.Equals, "provider_missing")
			c.Assert(report.Diagnostic.EngineID, qt.Equals, entry.EngineID)
			c.Assert(report.Diagnostic.SetupURL, qt.Equals, entry.SetupURL)
			c.Assert(stderr.String(), qt.Contains, entry.SetupURL)
		})
	}
}

// Each case is refused before any request, so the shipped command needs no
// release server.
func TestProviderInstallRefusesBeforeDownloading(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		code string
	}{
		{"unknown engine", []string{"oracle"}, "provider_unknown_engine"},
		{"a mirror without a version", []string{"ydb", "--download-base-url", "http://127.0.0.1:1/mirror"}, "provider_version_required"},
		{"a version that is not semantic", []string{"ydb", "--version", "latest"}, "provider_version_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("XDG_CACHE_HOME", c.TempDir())
			t.Setenv("HOME", c.TempDir())
			t.Setenv("LocalAppData", c.TempDir())
			command := providers.NewCommand()
			var out, stderr bytes.Buffer
			command.SetOut(&out)
			command.SetErr(&stderr)
			command.SetArgs(append(append([]string{"install"}, test.args...), "--json"))
			c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
			var report struct {
				Status     string
				Diagnostic providerdiagnostic.Diagnostic
			}
			c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
			c.Assert(report.Status, qt.Equals, "failed")
			c.Assert(report.Diagnostic.Code, qt.Equals, test.code)
		})
	}
}

func TestProviderInstallTakesExactlyOneEngine(t *testing.T) {
	c := qt.New(t)
	for _, args := range [][]string{{"install"}, {"install", "ydb", "duckdb"}} {
		command := providers.NewCommand()
		command.SetOut(&bytes.Buffer{})
		command.SetErr(&bytes.Buffer{})
		command.SetArgs(args)
		c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil, qt.Commentf("%v", args))
	}
}
