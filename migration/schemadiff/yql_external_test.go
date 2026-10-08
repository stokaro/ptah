package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

func TestCompare_YQLExternalObjectsDeclaredAndOmitted(t *testing.T) {
	for _, test := range []struct {
		name           string
		source         string
		removedSources []string
		removedTables  []string
	}{
		{name: "declared", source: "CREATE EXTERNAL DATA SOURCE bucket WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://storage.invalid/', AUTH_METHOD = 'NONE'); CREATE EXTERNAL TABLE events (id Int64) WITH (DATA_SOURCE = 'bucket', LOCATION = '/');"},
		{name: "omitted", removedSources: []string{"bucket"}, removedTables: []string{"events"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.source), "ydb")
			c.Assert(err, qt.IsNil)
			held := &catalog.Database{
				ExternalDataSources: []catalog.ExternalDataSource{{Name: "bucket", SourceType: "ObjectStorage", Location: "https://storage.invalid/", AuthMethod: "NONE"}},
				ExternalTables:      []catalog.ExternalTable{{Name: "events", DataSource: "bucket", Location: "/", Columns: []catalog.ExternalColumn{{Name: "id", Type: "Int64"}}}},
			}
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))
			var removedSources, removedTables []string
			for _, source := range diff.ExternalDataSourcesRemoved {
				removedSources = append(removedSources, source.QualifiedName())
			}
			for _, table := range diff.ExternalTablesRemoved {
				removedTables = append(removedTables, table.QualifiedName())
			}
			c.Assert(removedSources, qt.DeepEquals, test.removedSources)
			c.Assert(removedTables, qt.DeepEquals, test.removedTables)
			c.Assert(diff.ExternalDataSourcesAdded, qt.HasLen, 0)
			c.Assert(diff.ExternalTablesAdded, qt.HasLen, 0)
			c.Assert(diff.ExternalDataSourcesChanged, qt.HasLen, 0)
			c.Assert(diff.ExternalTablesChanged, qt.HasLen, 0)
		})
	}
}
