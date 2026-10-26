//go:build integration

package providercli_test

import (
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The catalog keeps the opened database binding even when a scope contains no
// objects. Planning must not infer that binding from a table or a URL later.
func TestProviderInspectionRetainsEachSessionDatabase(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	for range 2 {
		connection, address := transactionConnection(c, host, "")
		parsed, err := url.Parse(address)
		c.Assert(err, qt.IsNil)
		for _, scopes := range [][]string{nil, {"main"}} {
			observed, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), connection, scopes)
			c.Assert(err, qt.IsNil)
			c.Assert(observed.Tables, qt.HasLen, 0)
			c.Assert(observed.DatabasePath, qt.Equals, parsed.Path)
		}
	}
}
