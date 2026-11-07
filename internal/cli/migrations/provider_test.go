package migrations_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
	"ptah.run/internal/cli/migrations"
	"ptah.run/internal/errorkind"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/migration/migrator"
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
	for _, verb := range []string{"up", "down", "status", "log"} {
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

// TestProviderMigrationSetupReportsOfficialInstallation reports
// provider_missing for each official engine, with its setup page. The user
// cache, where `ptah providers install` stores a provider, is an empty
// directory of the test's own: a provider installed on the machine that runs
// the test would otherwise be selected and the run would not report it
// missing.
func TestProviderMigrationSetupReportsOfficialInstallation(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		for _, verb := range []string{"up", "down", "status", "log"} {
			t.Run(entry.EngineID+"/"+verb, func(t *testing.T) {
				c := qt.New(t)
				c.Setenv("XDG_CACHE_HOME", c.TempDir())
				c.Setenv("HOME", c.TempDir())
				c.Setenv("LocalAppData", c.TempDir())
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

// TestProviderSetupFailureIsTheVerbsFailureDocument holds a setup failure to
// the failure document of the verb it stopped: a run that never started for
// up and down, and the status contract's failure document for status. Neither names a
// kind, because no target connection was opened.
func TestProviderSetupFailureIsTheVerbsFailureDocument(t *testing.T) {
	c := qt.New(t)
	run := func(verb string) string {
		command := migrations.NewMigrationsCommand()
		var out bytes.Buffer
		command.SetOut(&out)
		command.SetErr(&bytes.Buffer{})
		command.SetArgs([]string{verb, "--provider-engine=", "--json"})
		c.Assert(command.ExecuteContext(c.Context()), qt.IsNotNil)
		return out.String()
	}

	var up struct {
		migrator.RunResult
		ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
	}
	c.Assert(json.Unmarshal([]byte(run("up")), &up), qt.IsNil)
	c.Assert(up.ContractVersion, qt.Equals, migrator.RunContractVersion)
	c.Assert(up.Command, qt.Equals, "migrations up")
	c.Assert(up.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(up.Error, qt.IsNotNil)
	c.Assert(up.Error.Kind, qt.Equals, "")

	var down struct {
		migrator.RunResult
		ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
	}
	c.Assert(json.Unmarshal([]byte(run("down")), &down), qt.IsNil)
	c.Assert(down.Command, qt.Equals, "migrations down")
	c.Assert(down.Direction, qt.Equals, migrator.MigrationDirectionDown)
	c.Assert(down.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(down.Error, qt.IsNotNil)

	var status struct {
		errorkind.Failure
		ProviderDiagnostic *providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
	}
	c.Assert(json.Unmarshal([]byte(run("status")), &status), qt.IsNil)
	c.Assert(status.ContractVersion, qt.Equals, migrator.StatusContractVersion)
	c.Assert(status.Command, qt.Equals, "migrations status")
	c.Assert(status.Error, qt.IsNotNil)
	c.Assert(status.Error.Kind, qt.Equals, errorkind.Kind(""))
}
