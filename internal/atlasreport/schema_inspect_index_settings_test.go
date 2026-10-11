package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasreport"
)

// TestSchemaInspectHCLWritesAnIndexsOwnerSettingsAsAttributes writes an
// index's settings an HCL owner writes as the index block's attributes,
// rather than as the platform properties the export encodes other index
// settings as: a YDB index's partitioning, and a MySQL FULLTEXT index's
// parser bound to mysql alone, which has a property spelling there.
func TestSchemaInspectHCLWritesAnIndexsOwnerSettingsAsAttributes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		facets  schemaext.Facets
		want    string
	}{
		{name: "a YDB index's partitioning", dialect: "ydb",
			facets: must.Must(must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{MinPartitions: 2}})).
				WithTargetScope(ydbschema.IndexPartitioningKind, "ydb")),
			want: "    auto_partitioning_min_partitions_count = 2\n"},
		{name: "a MySQL index's parser", dialect: "mysql",
			facets: must.Must(must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndex{Parser: "ngram"})).WithTargetScope(mysqlschema.IndexKind, "mysql")),
			want:   "    parser = \"ngram\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "events", StructName: "Event"}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "Int64", Primary: true}, {Name: "body", StructName: "Event", Type: "Utf8"}},
				Indexes: []schemamodel.Index{{Name: "k", StructName: "Event", TableName: "events", Type: "FULLTEXT", Fields: []string{"body"},
					Facets: test.facets}},
			}
			var diagnostics strings.Builder
			report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: test.dialect}, &diagnostics, atlasreport.SchemaInspectReportOptions{})

			document, err := report.MarshalHCL()

			c.Assert(err, qt.IsNil)
			c.Assert(document, qt.Contains, test.want)
			c.Assert(document, qt.Not(qt.Contains), "platform \"")
		})
	}
}
