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

func TestProviderInstallRefusesEnginesThatCannotBeInstalled(t *testing.T) {
	for _, test := range []struct{ name, engine, code string }{
		{"duckdb has no release", "duckdb", "provider_no_release_pinned"},
		{"spanner has no release", "spanner", "provider_no_release_pinned"},
		{"unknown engine", "oracle", "provider_unknown_engine"},
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
			command.SetArgs([]string{"install", test.engine, "--json"})
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

func TestProviderInstallTakesExactlyOneEngineAndNoVersion(t *testing.T) {
	for _, args := range [][]string{{"install"}, {"install", "ydb", "duckdb"}, {"install", "ydb", "--version", "1.0.0"}} {
		c := qt.New(t)
		command := providers.NewCommand()
		command.SetOut(&bytes.Buffer{})
		command.SetErr(&bytes.Buffer{})
		command.SetArgs(args)
		c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil, qt.Commentf("%v", args))
	}
}
