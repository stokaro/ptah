package migrations_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/cli/migrations"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
)

func TestProviderSelectionBelongsToNativeMigrations(t *testing.T) {
	c := qt.New(t)
	native := migrations.NewMigrationsCommand()
	for _, verb := range []string{"up", "down", "status", "log", "tag", "baseline", "repair", "set"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			command, _, err := native.Find([]string{verb})
			c.Assert(err, qt.IsNil)
			c.Assert(command.Flags().Lookup("provider-config"), qt.IsNotNil)
			c.Assert(command.Flags().Lookup("provider-engine"), qt.IsNotNil)
		})
	}
	shared := migrateup.NewMigrateUpCommand()
	c.Assert(shared.Flags().Lookup("provider-config"), qt.IsNil)
	c.Assert(shared.Flags().Lookup("provider-engine"), qt.IsNil)
	create, _, err := native.Find([]string{"create"})
	c.Assert(err, qt.IsNil)
	c.Assert(create.Flags().Lookup("provider-config"), qt.IsNil)
}

func TestProviderSetupFailurePreservesMigrationJSON(t *testing.T) {
	for _, verb := range []string{"up", "status", "log"} {
		t.Run(verb, func(t *testing.T) {
			c := qt.New(t)
			command := migrations.NewMigrationsCommand()
			var out, diagnostics bytes.Buffer
			command.SetOut(&out)
			command.SetErr(&diagnostics)
			command.SetArgs([]string{verb, "--provider-engine=", "--json"})
			c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
			c.Assert(diagnostics.String(), qt.Contains, "requires an engine ID")
			var result map[string]any
			c.Assert(json.Unmarshal(out.Bytes(), &result), qt.IsNil)
			c.Assert(out.String(), qt.Contains, "requires an engine ID")
		})
	}
}

func TestProviderMigrationSetupReportsOfficialInstallation(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		for _, verb := range []string{"up", "status", "log"} {
			t.Run(entry.EngineID+"/"+verb, func(t *testing.T) {
				c := qt.New(t)
				command := migrations.NewMigrationsCommand()
				var out, diagnostics bytes.Buffer
				command.SetOut(&out)
				command.SetErr(&diagnostics)
				command.SetArgs([]string{verb, "--provider-engine", entry.EngineID, "--json"})
				c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
				var report struct {
					Diagnostic providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
				}
				c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
				c.Assert(report.Diagnostic.Code, qt.Equals, "provider_missing")
				c.Assert(report.Diagnostic.EngineID, qt.Equals, entry.EngineID)
				c.Assert(report.Diagnostic.SetupURL, qt.Equals, entry.SetupURL)
			})
		}
	}
}
