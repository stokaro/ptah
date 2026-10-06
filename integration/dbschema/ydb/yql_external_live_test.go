//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

func TestYDBDesiredYQL_ExternalObjects(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			for _, mode := range []struct {
				name  string
				flags []clusterFlag
			}{
				{name: "recreate", flags: []clusterFlag{externalSourcesOn}},
				{name: "replace", flags: []clusterFlag{externalSourcesOn, externalReplaceOn}},
			} {
				t.Run(mode.name, func(t *testing.T) {
					c := qt.New(t)
					setClusterFlags(c, line, mode.flags...)
					conn := connect(c, enterRealm(c, line))
					path := filepath.Join(c.TempDir(), "schema.sql")
					for _, source := range []string{
						"CREATE EXTERNAL DATA SOURCE bucket WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://storage.invalid/old/', AUTH_METHOD = 'NONE'); CREATE EXTERNAL TABLE events (id Int64 NOT NULL, amount Decimal(22,9)) WITH (DATA_SOURCE = 'bucket', LOCATION = '/', FORMAT = 'csv_with_names', CSV_DELIMITER = ';');",
						"CREATE OR REPLACE EXTERNAL DATA SOURCE bucket WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://storage.invalid/new/', AUTH_METHOD = 'NONE'); CREATE OR REPLACE EXTERNAL TABLE events (id Int64 NOT NULL, amount Decimal(22,9), body Utf8) WITH (DATA_SOURCE = 'bucket', LOCATION = '/', FORMAT = 'csv_with_names', CSV_DELIMITER = ' ');",
						"",
					} {
						c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
						desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
						c.Assert(err, qt.IsNil)
						statements := planAgainst(c, conn, desired, nil)
						c.Assert(statements, qt.Not(qt.HasLen), 0)
						apply(c, conn, statements)
						c.Assert(planAgainst(c, conn, desired, nil), qt.HasLen, 0)
						described := readScoped(c, conn, nil)
						c.Assert(described.ExternalTables, qt.HasLen, len(desired.ExternalTables))
						for _, table := range described.ExternalTables {
							c.Assert(table.Options["CSV_DELIMITER"], qt.Equals, desired.ExternalTables[0].Options["CSV_DELIMITER"])
						}
					}
				})
			}
		})
	}
}
