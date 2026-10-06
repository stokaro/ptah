package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

func TestReadYQLExternalObjects(t *testing.T) {
	c := qt.New(t)
	source := "CREATE EXTERNAL DATA SOURCE `a.b` WITH (SOURCE_TYPE = @@ObjectStorage@@, LOCATION = 'https://storage.invalid/root/', AUTH_METHOD = \"NONE\");" +
		"CREATE EXTERNAL DATA SOURCE `a/b` WITH (SOURCE_TYPE = 'PostgreSQL', LOCATION = 'pg.invalid:5432', AUTH_METHOD = 'BASIC', DATABASE_NAME = 'app', LOGIN = 'reader', PASSWORD_SECRET_PATH = 'secrets/password');" +
		"CREATE EXTERNAL TABLE `a/t` (id Int64 NOT NULL, amount Decimal(22,9), body Utf8) WITH (DATA_SOURCE = 'a.b', LOCATION = '2026/', FORMAT = 'csv_with_names', CSV_DELIMITER = ';', PARTITIONED_BY = '[\"id\"]');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.ExternalDataSources, qt.DeepEquals, []schemamodel.ExternalDataSource{
		{Name: "a.b", SourceType: "ObjectStorage", Location: "https://storage.invalid/root/", AuthMethod: "NONE", Options: make(map[string]string)},
		{Name: "b", Schema: "a", SourceType: "PostgreSQL", Location: "pg.invalid:5432", AuthMethod: "BASIC", Options: map[string]string{
			"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "secrets/password",
		}},
	})
	c.Assert(database.ExternalTables, qt.DeepEquals, []schemamodel.ExternalTable{{
		Name: "t", Schema: "a", DataSource: "a.b", Location: "2026/",
		Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}, {Name: "amount", Type: "Decimal(22,9)"}, {Name: "body", Type: "Utf8"}},
		Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": ";", "PARTITIONED_BY": `["id"]`},
	}})
	c.Assert(database.NotDescribed.Describes(coverage.ExternalDataSource), qt.IsTrue)
	c.Assert(database.NotDescribed.Describes(coverage.ExternalTable), qt.IsTrue)
}

func TestReadYQLExternalObjectsRoundTrip(t *testing.T) {
	c := qt.New(t)
	source := "CREATE OR REPLACE EXTERNAL DATA SOURCE `dir/bucket` WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://storage.invalid/root/', AUTH_METHOD = 'NONE');" +
		"CREATE OR REPLACE EXTERNAL TABLE `dir/data.v1` (`d` Date, `body` Utf8) WITH (DATA_SOURCE = 'dir/bucket', LOCATION = '/', FORMAT = 'csv_with_names', CSV_DELIMITER = ' ', PARTITIONED_BY = '[\"d\"]');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.ExternalTables[0].Options["CSV_DELIMITER"], qt.Equals, " ")
	for _, caps := range []capability.Capabilities{capability.YDB251(), capability.YDB262()} {
		statements, renderErr := renderer.GetOrderedCreateStatementsWithCapabilities(&database, "ydb", caps.With(capability.ExternalDataSources, true))
		c.Assert(renderErr, qt.IsNil)
		again, _, readErr := sqlschema.Read([]byte(strings.Join(statements, "\n")), "ydb")
		c.Assert(readErr, qt.IsNil)
		c.Assert(again.ExternalDataSources, qt.DeepEquals, database.ExternalDataSources)
		c.Assert(again.ExternalTables, qt.DeepEquals, database.ExternalTables)
	}
}

func TestReadYQLExternalObjectsRefusals(t *testing.T) {
	for _, source := range []string{
		"CREATE EXTERNAL DATA SOURCE s WITH (AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'ObjectStorage');",
		"CREATE EXTERNAL DATA SOURCE `/local/s` WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE `dir/` WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'NONE', REFERENCES = 'table');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'NONE', auth_method = 'BASIC');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'ObjectStorage'u, AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'ObjectStorage's, AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = $kind, AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL DATA SOURCE s WITH (SOURCE_TYPE = 'Object' || 'Storage', AUTH_METHOD = 'NONE');",
		"CREATE EXTERNAL TABLE t () WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64, id Utf8) WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64 DEFAULT 1l) WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64 FAMILY f) WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64, PRIMARY KEY (id)) WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64, FOREIGN KEY (id) REFERENCES other(id)) WITH (DATA_SOURCE = 's', LOCATION = '/');",
		"CREATE EXTERNAL TABLE t (id Int64) WITH (DATA_SOURCE = 's');",
		"CREATE EXTERNAL TABLE t (id Int64) WITH (LOCATION = '/');",
		"CREATE OR REPLACE TABLE t (id Int64, PRIMARY KEY (id));",
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.ErrorMatches, "YQL schema at position .*")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.ExternalDataSources, qt.HasLen, 0)
			c.Assert(database.ExternalTables, qt.HasLen, 0)
		})
	}
}
