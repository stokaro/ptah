//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestProviderWithoutCommonTablesNegotiatesAndRefusesSchemaPlan(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, migrationProviderCommandSpec(c, "native-migrations"))
	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"providers", "check", "--provider-config", f.manifestPath, "--provider-engine", "fixture", "--json"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	c.Assert(json.Valid([]byte(out)), qt.IsTrue)
	c.Assert(out, qt.Contains, `"status": "compatible"`)
	f.assertStopped(c)
	_, err := os.Stat(f.databasePath)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	_, diagnostics, code = f.run(c, c.Context(), "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
	c.Assert(code, qt.Equals, 2)
	c.Assert(diagnostics, qt.Contains, "no table creation projection service")
	f.assertStopped(c)
	_, err = os.Stat(f.planPath)
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), "items")
}
