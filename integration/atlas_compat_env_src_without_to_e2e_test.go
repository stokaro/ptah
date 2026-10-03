//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasregistry"
	"ptah.run/internal/schemaartifacttest"
)

// envSrcRegistryOrders is the desired schema the registry row pulls: one
// `orders` table.
func envSrcRegistryOrders() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
	}
}

// TestCompatSchemaApplyWithoutToReadsTheEnvSourceE2E applies an env's desired
// state with no --to, for the source kinds that are not local files, and reads
// the target back. An omitted --to takes the env's source, and a kind it did
// not recognize was read as a local file and refused as one
// (stokaro/ptah#4061). The pinned community binary v1.3.0 applies a database
// `src` the same way: measured on SQLite, `schema apply --env local
// --auto-approve` plans and applies the source database's table, exit 0.
func TestCompatSchemaApplyWithoutToReadsTheEnvSourceE2E(t *testing.T) {
	tests := []struct {
		name string
		// data is the atlas.hcl text before the env.
		data string
		// src is the env's `src` expression, {desired} standing for a scratch
		// PostgreSQL database holding the source row's table.
		src string
		// table is the table the desired state creates.
		table string
	}{
		{
			name: "data.remote_schema",
			data: `data "remote_schema" "app" {
  name = "app"
  tag  = "prod"
}
`,
			src:   `data.remote_schema.app.url`,
			table: "orders",
		},
		{
			name:  "a database URL",
			src:   `"{desired}"`,
			table: "live_orders",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(atlasregistry.PlainHTTP.Name(), "1")
			host := schemaartifacttest.StartSchemaArtifactRegistry(c, "acme/app", "prod", envSrcRegistryOrders())
			t.Setenv(atlasregistry.NamespaceEnvVar, host+"/acme")
			devURL, _ := scratchReplayDatabase(c)
			targetURL, _ := scratchReplayDatabase(c)
			desiredURL, _ := scratchReplayDatabase(c)
			desired, err := dbschema.ConnectToDatabase(c.Context(), desiredURL)
			c.Assert(err, qt.IsNil)
			_, err = desired.ExecContext(c.Context(), "CREATE TABLE live_orders (id integer PRIMARY KEY)")
			c.Assert(err, qt.IsNil)
			dbschema.CloseAndWarn(desired)
			dir := c.TempDir()
			config := filepath.Join(dir, "atlas.hcl")
			src := strings.ReplaceAll(test.src, "{desired}", desiredURL)
			c.Assert(os.WriteFile(config, []byte(test.data+`
env "local" {
  url = "`+targetURL+`"
  dev = "`+devURL+`"
  src = `+src+`
}
`), 0o600), qt.IsNil)
			project := []string{"--config", "file://" + filepath.ToSlash(config), "--env", "local"}

			diff, diffErr := runCompatVerb(append([]string{"schema", "diff", "--from", targetURL}, project...)...)
			out, applyErr := runCompatVerb(append([]string{"schema", "apply", "--auto-approve"}, project...)...)

			c.Assert(diffErr, qt.IsNil, qt.Commentf("%s", diff))
			c.Assert(diff, qt.Contains, test.table)
			c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", out))
			target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(target)
			var tables string
			c.Assert(target.QueryRowContext(c.Context(), `
				SELECT coalesce(string_agg(table_name, ',' ORDER BY table_name), '')
				FROM information_schema.tables
				WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, test.table)
		})
	}
}
