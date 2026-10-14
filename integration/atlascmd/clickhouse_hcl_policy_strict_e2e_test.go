//go:build integration

package atlas_test

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// Strict mode refuses the authored policy before another dev dialect can omit it.
func TestStrictCompatRefusesClickHouseHCLPolicy(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`row_policy "p" { on = table.orders }`), 0o600), qt.IsNil)
	source := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
	stdout, stderr, code := runAtlasBinary(compat, strictCompat, "schema", "inspect", "--url", source, "--dev-url", "sqlite://:memory:")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Contains, `strict compatibility does not support database dialect "clickhouse"`)
}
