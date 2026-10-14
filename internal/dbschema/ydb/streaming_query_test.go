package ydb_test

import (
	"context"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/catalog"
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

// streamingFixture holds a streaming query at the root and one in a
// directory, each with the entry its path answers with: a streaming query
// has no description that carries one.
func streamingFixture() streamingSource {
	return streamingSource{fakeSource: fakeSource{directories: map[string][]*Ydb_Scheme.Entry{
		"/local":      {entry("jobs", Ydb_Scheme.Entry_DIRECTORY), entry("root", ydbschema.EntryStreamingQuery)},
		"/local/jobs": {entry("copy", ydbschema.EntryStreamingQuery)},
	}, paths: map[string]*Ydb_Scheme.Entry{
		"/local/root":      owned("root@builtin"),
		"/local/jobs/copy": owned("app", permission("operators", "ydb.generic.manage"), permission("readers", "ydb.granular.describe_schema")),
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

// TestReader_ReadsTheStreamingQueryPermissions reads the owner and the
// permission entries of a streaming query in the read's scope, asked for by
// path, and none of one outside it.
func TestReader_ReadsTheStreamingQueryPermissions(t *testing.T) {
	c := qt.New(t)
	reader := ydbschema.NewReaderFromSource(streamingFixture(), "/local", capability.YDB262().With(capability.StreamingQueries, true))
	reader.SetSchemas([]string{"jobs"})

	db, err := reader.ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "operators", Privilege: "ydb.generic.manage", ObjectType: "STREAMING QUERY", Schema: "jobs", ObjectName: "copy"},
		{Role: "readers", Privilege: "ydb.granular.describe_schema", ObjectType: "STREAMING QUERY", Schema: "jobs", ObjectName: "copy"},
	})
	c.Assert(db.ObjectOwners, qt.DeepEquals, []catalog.ObjectOwner{{Kind: "streaming query", Schema: "jobs", Name: "copy", Owner: "app"}})
}

// TestReader_StreamingQueryPermissions_FailurePath fails a read whose request
// for a streaming query's entry fails, as a failed description fails it, and
// asks for none where the line has no streaming queries.
func TestReader_StreamingQueryPermissions_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := streamingFixture()
	source.paths = nil

	_, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.StreamingQueries, true)).
		ReadSchemaContext(context.Background())
	unkeyed, unkeyedErr := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262().With(capability.StreamingQueries, false)).
		ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `described path /local/jobs/copy, which the fixture does not hold`)
	c.Assert(unkeyedErr, qt.IsNil)
	c.Assert(unkeyed.Grants, qt.HasLen, 0)
}
