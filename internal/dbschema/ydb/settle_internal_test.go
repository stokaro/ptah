package ydb

// White-box testing required: the reader tells a table that is still being
// created or dropped from a refusal by the status the table service decodes
// into statusError, which only grpcSource builds from a server's answer. A
// source written outside the package cannot answer SCHEME_ERROR, so the
// black-box reader tests cannot reach the wait; the live tests drive it
// against a server.

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/platform/capability"
	"ptah.run/dialect/ydb/ydbschema"
)

// settlingSource answers each listing and each description of a table from a
// sequence, one answer per call, and then the last answer again.
type settlingSource struct {
	Source
	listings     [][]*Ydb_Scheme.Entry
	descriptions []describeAnswer
	listed       *int
	described    *int
}

// describeAnswer is one answer DescribeTable gives.
type describeAnswer struct {
	result *Ydb_Table.DescribeTableResult
	err    error
}

func (s settlingSource) ListDirectory(context.Context, string) (*Ydb_Scheme.Entry, []*Ydb_Scheme.Entry, error) {
	entries := s.listings[min(*s.listed, len(s.listings)-1)]
	*s.listed++
	return &Ydb_Scheme.Entry{Type: Ydb_Scheme.Entry_DIRECTORY}, entries, nil
}

func (s settlingSource) DescribeTable(context.Context, string) (*Ydb_Table.DescribeTableResult, error) {
	answer := s.descriptions[min(*s.described, len(s.descriptions)-1)]
	*s.described++
	return answer.result, answer.err
}

func (s settlingSource) Principals(context.Context) (Principals, error) { return Principals{}, nil }

func (s settlingSource) ResourcePools(context.Context) (ResourcePools, error) {
	return ResourcePools{}, nil
}

// notFound is how 25.1.4.7 answers DescribeTable for a path that does not
// exist: SCHEME_ERROR with no issue.
var notFound = describeAnswer{err: &statusError{status: Ydb.StatusIds_SCHEME_ERROR, issues: "no issue text"}}

// replicaListing is a database root that lists the replica table rep.
var replicaListing = []*Ydb_Scheme.Entry{{Name: "rep", Type: Ydb_Scheme.Entry_TABLE}}

// replicaDescription is how the table service describes a replica.
func replicaDescription() describeAnswer {
	return describeAnswer{result: &Ydb_Table.DescribeTableResult{
		Columns:    []*Ydb_Table.ColumnMeta{{Name: "id", Type: &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT64}}}},
		PrimaryKey: []string{"id"},
		Attributes: map[string]string{"__async_replica": "true"},
	}}
}

// newSettlingSource is a source answering from the two sequences.
func newSettlingSource(listings [][]*Ydb_Scheme.Entry, descriptions ...describeAnswer) settlingSource {
	return settlingSource{listings: listings, descriptions: descriptions, listed: new(int), described: new(int)}
}

// A table the directory lists and the table service does not describe yet is
// one another operation is creating: the read waits until it is described,
// and a replica then reads as a replica. A table the directory stops listing
// is one another operation dropped, and the read leaves it out.
func TestReader_SettlesATableInTransition_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		source   settlingSource
		recorded bool
	}{
		{name: "created during the read",
			source:   newSettlingSource([][]*Ydb_Scheme.Entry{replicaListing}, notFound, notFound, replicaDescription()),
			recorded: true},
		{name: "dropped during the read",
			source:   newSettlingSource([][]*Ydb_Scheme.Entry{replicaListing, nil}, notFound),
			recorded: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			reader := NewReaderFromSource(test.source, "/local", capability.YDB251())

			db, err := reader.ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 0)
			c.Assert(db.NotDescribed.Describes(ydbschema.CoverageReplicaTable, "rep"), qt.Equals, !test.recorded)
		})
	}
}

// A table still listed and still not described fails the read with the
// server's answer, once the wait ends: here the context's deadline, before
// the budget.
func TestReader_SettlesATableInTransition_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := newSettlingSource([][]*Ydb_Scheme.Entry{replicaListing}, notFound)
	reader := NewReaderFromSource(source, "/local", capability.YDB251())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	db, err := reader.ReadSchemaContext(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s)SCHEME_ERROR: no issue text; the read stopped waiting for the table YDB `+
		`lists and does not describe: context deadline exceeded`)
	c.Assert(db, qt.IsNil)
	c.Assert(*source.described > 1, qt.IsTrue, qt.Commentf("the read asked once and did not wait"))
}
