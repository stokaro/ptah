package ydb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	ydbschema "ptah.run/internal/dbschema/ydb"
)

// fakeOperations answers the operation service from fixed pages of build
// operations, and reports each build's state from a script, read once per
// GetOperation.
type fakeOperations struct {
	pages     [][]*Ydb_Operations.Operation
	listErr   error
	states    map[string][]*Ydb_Operations.Operation
	canceled  []string
	listCalls int
}

func (f *fakeOperations) ListOperations(
	_ context.Context, in *Ydb_Operations.ListOperationsRequest, _ ...grpc.CallOption,
) (*Ydb_Operations.ListOperationsResponse, error) {
	f.listCalls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	if in.GetKind() != "buildindex" {
		return nil, errors.New("listed a kind other than buildindex")
	}
	// Like the server, the fake numbers its pages from 2, answers page "0"
	// with the first page, and ends a listing with the token "0".
	page := 0
	if in.GetPageToken() == "2" {
		page = 1
	}
	response := &Ydb_Operations.ListOperationsResponse{Status: Ydb.StatusIds_SUCCESS, NextPageToken: "0"}
	if page < len(f.pages) {
		response.Operations = f.pages[page]
	}
	if page+1 < len(f.pages) {
		response.NextPageToken = "2"
	}
	return response, nil
}

func (f *fakeOperations) CancelOperation(
	_ context.Context, in *Ydb_Operations.CancelOperationRequest, _ ...grpc.CallOption,
) (*Ydb_Operations.CancelOperationResponse, error) {
	f.canceled = append(f.canceled, in.GetId())
	return &Ydb_Operations.CancelOperationResponse{Status: Ydb.StatusIds_SUCCESS}, nil
}

func (f *fakeOperations) GetOperation(
	_ context.Context, in *Ydb_Operations.GetOperationRequest, _ ...grpc.CallOption,
) (*Ydb_Operations.GetOperationResponse, error) {
	script := f.states[in.GetId()]
	if len(script) == 0 {
		return nil, errors.New("read a build the fixture does not hold")
	}
	state := script[0]
	if len(script) > 1 {
		f.states[in.GetId()] = script[1:]
	}
	return &Ydb_Operations.GetOperationResponse{Operation: state}, nil
}

func (f *fakeOperations) ForgetOperation(
	context.Context, *Ydb_Operations.ForgetOperationRequest, ...grpc.CallOption,
) (*Ydb_Operations.ForgetOperationResponse, error) {
	return nil, errors.New("forget is not used")
}

// build is a build operation on path, running or ended with status.
func build(c *qt.C, id, path string, ready bool, status Ydb.StatusIds_StatusCode) *Ydb_Operations.Operation {
	c.Helper()
	metadata, err := anypb.New(&Ydb_Table.IndexBuildMetadata{
		Description: &Ydb_Table.IndexBuildDescription{Path: path},
	})
	c.Assert(err, qt.IsNil)
	return &Ydb_Operations.Operation{Id: id, Ready: ready, Status: status, Metadata: metadata}
}

var quickWait = ydbschema.BuildWait{Appear: 30 * time.Millisecond, Settle: 30 * time.Millisecond, Poll: time.Millisecond}

// The build canceled is the one running on the table, found on any page and
// matched by its absolute path, and the outcome is what the build reported
// after the cancellation.
func TestBuilds_CancelRunning_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		table string
		after Ydb.StatusIds_StatusCode
		want  ydbschema.BuildOutcome
	}{
		{name: "canceled", table: "dir/items", after: Ydb.StatusIds_CANCELLED, want: ydbschema.BuildStopped},
		{name: "failed", table: "dir/items", after: Ydb.StatusIds_GENERIC_ERROR, want: ydbschema.BuildStopped},
		{name: "completed first", table: "dir/items", after: Ydb.StatusIds_SUCCESS, want: ydbschema.BuildCompleted},
		{name: "absolute path", table: "/local/dir/items", after: Ydb.StatusIds_CANCELLED, want: ydbschema.BuildStopped},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operations := &fakeOperations{
				pages: [][]*Ydb_Operations.Operation{
					{
						build(c, "done", "/local/dir/items", true, Ydb.StatusIds_SUCCESS),
						build(c, "other", "/local/dir/other", false, Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
					},
					{build(c, "ours", "/local/dir/items", false, Ydb.StatusIds_STATUS_CODE_UNSPECIFIED)},
				},
				states: map[string][]*Ydb_Operations.Operation{
					"ours": {
						build(c, "ours", "/local/dir/items", false, Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
						build(c, "ours", "/local/dir/items", true, test.after),
					},
				},
			}

			outcome, err := ydbschema.NewBuilds(operations, "/local").CancelRunning(c.Context(), test.table, quickWait)

			c.Assert(err, qt.IsNil)
			c.Assert(outcome, qt.Equals, test.want)
			c.Assert(operations.canceled, qt.DeepEquals, []string{"ours"})
		})
	}
}

// A build that appears after the first look is still found.
func TestBuilds_CancelRunning_WaitsForTheBuildToAppear(t *testing.T) {
	c := qt.New(t)
	operations := &appearingOperations{
		fakeOperations: fakeOperations{
			states: map[string][]*Ydb_Operations.Operation{
				"late": {build(c, "late", "/local/items", true, Ydb.StatusIds_CANCELLED)},
			},
		},
		late: build(c, "late", "/local/items", false, Ydb.StatusIds_STATUS_CODE_UNSPECIFIED),
	}

	outcome, err := ydbschema.NewBuilds(operations, "/local").CancelRunning(c.Context(), "items", quickWait)

	c.Assert(err, qt.IsNil)
	c.Assert(outcome, qt.Equals, ydbschema.BuildStopped)
	c.Assert(operations.canceled, qt.DeepEquals, []string{"late"})
}

// appearingOperations lists no build on the first call and one on the next.
type appearingOperations struct {
	fakeOperations
	late *Ydb_Operations.Operation
}

func (a *appearingOperations) ListOperations(
	ctx context.Context, in *Ydb_Operations.ListOperationsRequest, opts ...grpc.CallOption,
) (*Ydb_Operations.ListOperationsResponse, error) {
	if a.listCalls > 0 {
		a.pages = [][]*Ydb_Operations.Operation{{a.late}}
	}
	return a.fakeOperations.ListOperations(ctx, in, opts...)
}

// Each row leaves a build it cannot vouch for alone: nothing is canceled
// unless exactly one build runs on the table, and a canceled build that does
// not end is reported as such.
func TestBuilds_CancelRunning_FailurePath(t *testing.T) {
	tests := []struct {
		name         string
		running      []string
		states       []Ydb.StatusIds_StatusCode
		want         ydbschema.BuildOutcome
		wantCanceled []string
	}{
		{name: "no build on the table", want: ydbschema.BuildNotFound},
		{name: "two builds on the table", running: []string{"a", "b"}, want: ydbschema.BuildUnsettled},
		{name: "a canceled build that does not end", running: []string{"a"},
			states: []Ydb.StatusIds_StatusCode{Ydb.StatusIds_STATUS_CODE_UNSPECIFIED},
			want:   ydbschema.BuildUnsettled, wantCanceled: []string{"a"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			operations := &fakeOperations{
				pages:  [][]*Ydb_Operations.Operation{{build(c, "elsewhere", "/local/other", false, 0)}},
				states: make(map[string][]*Ydb_Operations.Operation),
			}
			for _, id := range test.running {
				operations.pages[0] = append(operations.pages[0], build(c, id, "/local/items", false, 0))
				operations.states[id] = []*Ydb_Operations.Operation{build(c, id, "/local/items", false, 0)}
			}

			outcome, err := ydbschema.NewBuilds(operations, "/local").CancelRunning(c.Context(), "items", quickWait)

			c.Assert(err, qt.IsNil)
			c.Assert(outcome, qt.Equals, test.want)
			c.Assert(operations.canceled, qt.DeepEquals, test.wantCanceled)
		})
	}
}

// An operation service that cannot be asked is an error, and nothing is
// canceled.
func TestBuilds_CancelRunning_ListFails(t *testing.T) {
	c := qt.New(t)
	operations := &fakeOperations{listErr: errors.New("transport: connection refused")}

	outcome, err := ydbschema.NewBuilds(operations, "/local").CancelRunning(c.Context(), "items", quickWait)

	c.Assert(err, qt.ErrorMatches, `list YDB builds: transport: connection refused`)
	c.Assert(outcome, qt.Equals, ydbschema.BuildUnsettled)
	c.Assert(operations.canceled, qt.HasLen, 0)
}
