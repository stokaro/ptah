package ydb_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

type streamingSource struct {
	fakeSource
	queries map[string]ydbstreaming.Spec
	failure error
}

func (s streamingSource) DescribeStreamingQuery(_ context.Context, name string) (ydbstreaming.Spec, error) {
	if s.failure != nil {
		return ydbstreaming.Spec{}, s.failure
	}
	spec, ok := s.queries[name]
	if !ok {
		return spec, fmt.Errorf("streaming query %s is absent", name)
	}
	return spec.Clone(), nil
}

func streamingFixture() streamingSource {
	return streamingSource{fakeSource: fakeSource{directories: map[string][]*Ydb_Scheme.Entry{
		"/local":      {entry("jobs", Ydb_Scheme.Entry_DIRECTORY), entry("root", ydbschema.EntryStreamingQuery)},
		"/local/jobs": {entry("copy", ydbschema.EntryStreamingQuery)},
	}}, queries: map[string]ydbstreaming.Spec{
		"/local/root":      {Text: "SELECT 1;", Run: new(false)},
		"/local/jobs/copy": {Text: "INSERT INTO output SELECT * FROM input;", Run: new(true), ResourcePool: "batch"},
	}}
}

func TestReader_StreamingQueriesScopedAndCopied(t *testing.T) {
	c := qt.New(t)
	source := streamingFixture()
	reader := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.StreamingQueries, true))
	reader.SetSchemas([]string{"jobs"})
	db, err := reader.ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
	object, found, err := db.FeatureObjects.Get(ydbstreaming.Ref("jobs", "copy"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	query := object.Value.(*ydbstreaming.Observed)
	c.Assert(query.Spec, qt.DeepEquals, source.queries["/local/jobs/copy"])
	*query.Spec.Run = false
	c.Assert(*source.queries["/local/jobs/copy"].Run, qt.IsTrue)
}

func TestReader_StreamingMetadataRefusalIsNotAnEmptySchema(t *testing.T) {
	c := qt.New(t)
	source := streamingFixture()
	source.failure = ydbschema.ErrPrincipalsRefused
	db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.StreamingQueries, true)).ReadSchemaContext(context.Background())
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(db.FeatureCoverage.Lookup(ydbstreaming.Kind, ydbstreaming.Ref("jobs", "copy")).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(db.FeatureCoverage.Lookup(ydbstreaming.Kind, ydbstreaming.Ref("", "root")).State, qt.Equals, schemaext.Unrepresentable)
}

func TestReader_StreamingMetadataFailureStopsTheRead(t *testing.T) {
	c := qt.New(t)
	source := streamingFixture()
	source.failure = fmt.Errorf("streaming metadata unavailable")
	_, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.StreamingQueries, true)).ReadSchemaContext(context.Background())
	c.Assert(err, qt.ErrorIs, source.failure)
}
