package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/coverage"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/dbschematogo"
)

// TestReader_HCLExportKeepsWhatTheReadDidNotDescribe exports a read's account
// of what it did not describe -- a TTL run interval, storage settings, a
// column store and a sequence -- into an HCL document and reads the document
// back. The limits survive the round trip in the document's header, so the
// next plan made from the document does not read its silence about them as
// complete. Read with the bundled vocabulary, as a run that selects the YDB
// owner reads it.
func TestReader_HCLExportKeepsWhatTheReadDidNotDescribe(t *testing.T) {
	c := qt.New(t)
	settings := plainTable(&Ydb_Table.ColumnMeta{Name: "ts", Type: optional(primitive(Ydb.Type_TIMESTAMP))})
	settings.TtlSettings = dateTTL("ts", 86400)
	settings.TtlSettings.RunIntervalSeconds = 1800
	settings.StorageSettings = &Ydb_Table.StorageSettings{StoreExternalBlobs: Ydb.FeatureFlag_ENABLED}
	read := readFrom(c, fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local":     {entry("olap", Ydb_Scheme.Entry_COLUMN_STORE), entry("seq", Ydb_Scheme.Entry_SEQUENCE), entry("app", Ydb_Scheme.Entry_DIRECTORY)},
			"/local/app": {entry("t", Ydb_Scheme.Entry_TABLE)},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{"/local/app/t": settings},
	})
	runtime := must.Must(builtin.New())
	described := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), read, "ydb", runtime))

	exported, err := atlashclrender.RenderInspected(described, "ydb", "")
	c.Assert(err, qt.IsNil)
	parsed, err := atlashcl.ParseWithOptions(exported.Data, "schema.hcl", atlashcl.Options{CoverageVocabulary: runtime.CoverageVocabulary()})

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.NotDescribed.Directives(), qt.DeepEquals, read.NotDescribed.Directives())
	c.Assert(parsed.NotDescribed.Describes(ydbschema.CoverageTTL, "app.t"), qt.IsFalse)
	c.Assert(parsed.NotDescribed.Describes(ydbschema.CoverageTableOption, "app.t"), qt.IsFalse)
	c.Assert(parsed.NotDescribed.Describes(ydbschema.CoverageColumnTable, "olap"), qt.IsFalse)
	c.Assert(parsed.NotDescribed.Describes(coverage.Sequence, "seq"), qt.IsFalse)
}
