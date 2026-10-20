//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
)

func providerSelectionCases(t *testing.T, spec providerhost.Spec) {
	t.Helper()
	t.Run("listing verifies bytes without executing", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		out, stderr, code := executeProviderCLI(c.Context(), []string{"providers", "list", "--provider-config", f.manifestPath, "--json"})
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(json.Valid([]byte(out)), qt.IsTrue)
		c.Assert(out, qt.Contains, `"status": "integrity_verified"`)
		c.Assert(out, qt.Contains, `"protocol_status": "not_checked"`)
		_, err := os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("check negotiates and closes without database", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		out, stderr, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
		c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
		c.Assert(json.Valid([]byte(out)), qt.IsTrue)
		c.Assert(out, qt.Contains, `"status": "compatible"`)
		c.Assert(out, qt.Contains, `"version": "0.0.1"`)
		f.assertStopped(c)
		_, err := os.Stat(f.databasePath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("unused missing provider never starts", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		changeManifest(c, f, func(config *providerconfig.Configuration) {
			unused := providerconfig.Installation{Spec: providerhost.Spec{ProviderID: "example.org/unused", Executable: filepath.Join(c.TempDir(), "missing"), SHA256: strings.Repeat("a", 64)}, Engines: []providerconfig.Engine{{ID: "unused"}}}
			config.Providers = append(config.Providers, unused)
		})
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
	})
	t.Run("startup failure differs from protocol incompatibility", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		invalid := []byte("not an executable image")
		path := filepath.Join(c.TempDir(), "invalid.exe")
		c.Assert(os.WriteFile(path, invalid, 0o600), qt.IsNil)
		digest := sha256.Sum256(invalid)
		changeManifest(c, f, func(config *providerconfig.Configuration) {
			config.Providers[0].Executable = path
			config.Providers[0].SHA256 = hex.EncodeToString(digest[:])
		})
		out, _, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
		c.Assert(code, qt.Equals, 2)
		c.Assert(json.Valid([]byte(out)), qt.IsTrue)
		c.Assert(out, qt.Contains, `"code": "provider_start_failed"`)
		_, err := os.Stat(f.databasePath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	t.Run("target mismatch refuses before startup", func(t *testing.T) {
		c := qt.New(t)
		f := newProviderCLI(c, spec)
		f.address = "sqlite:///:memory:"
		out, _, code := executeProviderCLI(c.Context(), f.arguments("inspect", "--provider-engine", "fixture", "--format", "json"))
		c.Assert(code, qt.Equals, 2)
		c.Assert(out, qt.Contains, `"code": "provider_target_mismatch"`)
		_, err := os.Stat(f.pidPath)
		c.Assert(os.IsNotExist(err), qt.IsTrue)
	})
	for _, test := range []struct {
		name, code string
		change     func(*providerconfig.Configuration)
		started    bool
	}{
		{"disabled", "provider_disabled", func(v *providerconfig.Configuration) { v.Providers[0].Disabled = true }, false},
		{"changed bytes", "provider_untrusted", func(v *providerconfig.Configuration) { v.Providers[0].SHA256 = strings.Repeat("a", 64) }, false},
		{"missing executable", "provider_executable_missing", func(v *providerconfig.Configuration) { v.Providers[0].Executable += ".missing" }, false},
		{"incompatible identity", "provider_incompatible", func(v *providerconfig.Configuration) { v.Providers[0].ProviderID = "example.org/wrong" }, true},
		{"mismatched declarations", "provider_identity_mismatch", func(v *providerconfig.Configuration) { v.Providers[0].Engines[0].Aliases = []string{"extra"} }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			changeManifest(c, f, test.change)
			out, _, code := executeProviderCLI(c.Context(), f.arguments("plan", "--schema-file", f.schemaPath, "--json"))
			c.Assert(code, qt.Equals, 2)
			c.Assert(json.Valid([]byte(out)), qt.IsTrue)
			c.Assert(out, qt.Contains, `"code": "`+test.code+`"`)
			if test.started {
				f.assertStopped(c)
			} else {
				_, err := os.Stat(f.pidPath)
				c.Assert(os.IsNotExist(err), qt.IsTrue)
			}
			_, err := os.Stat(f.databasePath)
			c.Assert(os.IsNotExist(err), qt.IsTrue)
		})
	}
}

func changeManifest(c *qt.C, f providerCLI, change func(*providerconfig.Configuration)) {
	c.Helper()
	data, err := os.ReadFile(f.manifestPath)
	c.Assert(err, qt.IsNil)
	var config providerconfig.Configuration
	c.Assert(json.Unmarshal(data, &config), qt.IsNil)
	change(&config)
	data, err = json.Marshal(config)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifestPath, data, 0o600), qt.IsNil)
}

func TestMissingOfficialProviderDiagnosticsAcrossNativeCommands(t *testing.T) {
	for _, entry := range providercatalog.Entries() {
		for _, command := range []struct {
			verb string
			args []string
		}{
			{"plan", []string{"--json"}}, {"apply", []string{"--json"}}, {"inspect", []string{"--format", "json"}},
		} {
			t.Run(entry.EngineID+"/"+command.verb, func(t *testing.T) {
				c := qt.New(t)
				args := append([]string{"schema", command.verb, "--provider-engine", entry.EngineID}, command.args...)
				out, stderr, code := executeProviderCLI(c.Context(), args)
				c.Assert(code, qt.Equals, 2, qt.Commentf("%s", stderr))
				var report struct {
					Diagnostic providerdiagnostic.Diagnostic `json:"provider_diagnostic"`
				}
				c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
				c.Assert(report.Diagnostic.Code, qt.Equals, "provider_missing")
				c.Assert(report.Diagnostic.EngineID, qt.Equals, entry.EngineID)
				c.Assert(report.Diagnostic.ProviderID, qt.Equals, entry.ProviderID)
				c.Assert(report.Diagnostic.SetupURL, qt.Equals, entry.SetupURL)
			})
		}
	}
}

func TestProviderSetupLinksMatchPublishedDocumentation(t *testing.T) {
	c := qt.New(t)
	site := filepath.Join("..", "..", "docs", "site")
	declaration, err := os.ReadFile(filepath.Join(site, "src", "lib", "docs-origin.mjs"))
	c.Assert(err, qt.IsNil)
	origin := regexp.MustCompile(`(?m)^export const Origin = '([^']+)';$`).FindSubmatch(declaration)
	c.Assert(origin, qt.HasLen, 2)
	ledger, err := os.ReadFile(filepath.Join(site, "scripts", "data", "published-routes.json"))
	c.Assert(err, qt.IsNil)
	for _, entry := range providercatalog.Entries() {
		parsed, err := url.Parse(entry.SetupURL)
		c.Assert(err, qt.IsNil)
		c.Assert(parsed.Scheme+"://"+parsed.Host, qt.Equals, string(origin[1]))
		c.Assert(strings.HasPrefix(parsed.Path, "/edge/"), qt.IsTrue)
		route := strings.TrimPrefix(parsed.Path, "/edge")
		c.Assert(string(ledger), qt.Contains, `"`+route+`"`)
		page := filepath.Join(site, "src", "content", "docs", strings.Trim(route, "/")+".md")
		content, err := os.ReadFile(page)
		c.Assert(err, qt.IsNil)
		c.Assert(string(content), qt.Contains, entry.ProviderID)
	}
}
