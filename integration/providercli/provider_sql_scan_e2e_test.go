//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providerconfig"
	"ptah.run/internal/schemalineage"
)

// scanShop declares a table in the directory shop and views over it, written
// as the sql-scan fixture's engine writes its text: a table is one backticked path, a
// double-quoted text is a string, and so is an @@...@@ text, whose quote
// opens no other string.
const scanShop = "tables:\n  items:\n    schema: shop\n    columns:\n      id: { type: INTEGER, primary: true }\n" +
	"      label: { type: TEXT }\n      qty: { type: INTEGER }\n" +
	"views:\n" +
	"  shop.active: { body: \"SELECT id, label AS name FROM `shop/items` WHERE qty > 0\" }\n" +
	"  shop.active_names: { body: \"SELECT name FROM `shop/active`\" }\n" +
	"  shop.everything: { body: \"SELECT * FROM `shop/items`\" }\n" +
	"  shop.tagged: { body: \"SELECT id, \\\"fixed\\\" AS tag FROM `shop/items`\" }\n" +
	"  shop.noted: { body: \"SELECT id, @@it's@@ AS note, label FROM `shop/items`\" }\n"

// lineageThrough traces scanShop on engine with the provider command
// selected, offline.
func lineageThrough(c *qt.C, command, engine string) schemalineage.Result {
	c.Helper()
	dir := c.TempDir()
	manifest := filepath.Join(dir, "providers.json")
	data, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: fixtureSpec(c, command), Engines: []providerconfig.Engine{{ID: engine}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(manifest, data, 0o600), qt.IsNil)
	schema := filepath.Join(dir, "shop.yaml")
	c.Assert(os.WriteFile(schema, []byte(scanShop), 0o600), qt.IsNil)
	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"schema", "lineage", "--provider-config", manifest,
		"--dialect", engine, "--schema-file", schema, "--format", "json"})
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
	var result schemalineage.Result
	c.Assert(json.Unmarshal([]byte(out), &result), qt.IsNil)
	return result
}

// A provider that scans its engine's text gives the lineage its own tokens and
// its quoting: each source is the table at its path, the star resolves to the
// table's columns, and the strings feed no column.
func TestProviderLineageReadsTheEnginesText(t *testing.T) {
	c := qt.New(t)

	result := lineageThrough(c, "sql-scan", "fixture-scan")

	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Edges, qt.DeepEquals, []schemalineage.Edge{
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.active", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.active", ToColumn: "name"},
		{FromTable: "shop.active", FromColumn: "name", ToView: "shop.active_names", ToColumn: "name"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.everything", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.everything", ToColumn: "label"},
		{FromTable: "shop.items", FromColumn: "qty", ToView: "shop.everything", ToColumn: "qty"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.noted", ToColumn: "id"},
		{FromTable: "shop.items", FromColumn: "label", ToView: "shop.noted", ToColumn: "label"},
		{FromTable: "shop.items", FromColumn: "id", ToView: "shop.tagged", ToColumn: "id"},
	})
}

// The same views through a provider that does not scan are read by the shared
// rules: the path is a table name nobody declares, and the quote inside the
// @@ string opens a string that swallows the FROM clause.
func TestProviderLineageWithoutAScannerReadsTheSharedRules(t *testing.T) {
	c := qt.New(t)

	result := lineageThrough(c, "provider", "fixture")

	c.Assert(result.Undecided, qt.DeepEquals, []schemalineage.Undecided{
		{View: "shop.everything", Reason: `the select list is * and table "shop/items" declares no columns here, so its names are unknown`},
		{View: "shop.noted", Reason: "the body has no top-level FROM"},
	})
	c.Assert(result.Edges, qt.HasLen, 4)
}
