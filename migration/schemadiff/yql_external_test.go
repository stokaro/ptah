package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// TestCompare_YQLExternalObjectsDeclaredAndOmitted finds nothing to do
// between a YQL document and a read of the objects it declares, and drops
// both when the document claims the namespaces and declares neither.
func TestCompare_YQLExternalObjectsDeclaredAndOmitted(t *testing.T) {
	bucket := ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://storage.invalid/", AuthMethod: "NONE"}
	events := ydbexternal.Table{DataSource: "bucket", Location: "/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	for _, test := range []struct {
		name   string
		source string
		want   []schemaext.Kind
	}{
		{name: "declared", source: "CREATE EXTERNAL DATA SOURCE bucket WITH (SOURCE_TYPE = 'ObjectStorage', LOCATION = 'https://storage.invalid/', AUTH_METHOD = 'NONE'); CREATE EXTERNAL TABLE events (id Int64) WITH (DATA_SOURCE = 'bucket', LOCATION = '/');"},
		{name: "omitted", want: []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, _, err := sqlschema.Read([]byte(test.source), "ydb")
			c.Assert(err, qt.IsNil)
			held := &catalog.Database{FeatureCoverage: completeYDBFixtureCoverage(), FeatureObjects: must.Must(schemaext.NewObjects(
				ydbexternal.ObservedSourceObject("", "bucket", bucket), ydbexternal.ObservedTableObject("", "events", events)))}

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &desired, held, "ydb", must.Must(builtin.New())))

			var removed []schemaext.Kind
			for _, record := range diff.FeatureChanges {
				removed = append(removed, schemaext.Kind(record.Subject.Kind))
			}
			c.Assert(removed, qt.DeepEquals, test.want)
		})
	}
}
