package ydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"google.golang.org/protobuf/proto"
)

// Source is what the reader asks a YDB database: the entries of a directory,
// and the description of a row table. Both take an absolute path.
type Source interface {
	ListDirectory(ctx context.Context, path string) ([]*Ydb_Scheme.Entry, error)
	DescribeTable(ctx context.Context, path string) (*Ydb_Table.DescribeTableResult, error)
}

// grpcSource answers through the SDK driver's gRPC connection with raw scheme
// and table service calls, and decodes the answers itself.
//
// The raw call is what keeps an index kind the SDK does not model from being
// read as another kind. ydb-go-sdk maps every index type it does not know to a
// plain global index, so a vector index described through it reads back as
// one; the raw description leaves the type empty and the data in fields the
// pinned protocol buffers do not know, and the reader refuses it.
type grpcSource struct {
	scheme  Ydb_Scheme_V1.SchemeServiceClient
	table   Ydb_Table_V1.TableServiceClient
	session string
}

// newGRPCSource opens a table session for one read. The caller ends it with
// the returned function.
func newGRPCSource(ctx context.Context, driver *ydbsdk.Driver) (*grpcSource, func(), error) {
	connection := ydbsdk.GRPCConn(driver)
	source := &grpcSource{
		scheme: Ydb_Scheme_V1.NewSchemeServiceClient(connection),
		table:  Ydb_Table_V1.NewTableServiceClient(connection),
	}
	response, err := source.table.CreateSession(ctx, &Ydb_Table.CreateSessionRequest{})
	if err != nil {
		return nil, nil, fmt.Errorf("create a YDB table session: %w", err)
	}
	var created Ydb_Table.CreateSessionResult
	if err := operationResult(response.GetOperation(), &created); err != nil {
		return nil, nil, fmt.Errorf("create a YDB table session: %w", err)
	}
	source.session = created.GetSessionId()
	end := func() {
		// A session left behind expires on the server, so a failure to
		// delete it costs nothing a caller could act on.
		_, _ = source.table.DeleteSession(context.WithoutCancel(ctx),
			&Ydb_Table.DeleteSessionRequest{SessionId: source.session})
	}
	return source, end, nil
}

// ListDirectory lists the entries directly under path.
func (s *grpcSource) ListDirectory(ctx context.Context, path string) ([]*Ydb_Scheme.Entry, error) {
	response, err := s.scheme.ListDirectory(ctx, &Ydb_Scheme.ListDirectoryRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("list YDB directory %s: %w", path, err)
	}
	var listed Ydb_Scheme.ListDirectoryResult
	if err := operationResult(response.GetOperation(), &listed); err != nil {
		return nil, fmt.Errorf("list YDB directory %s: %w", path, err)
	}
	return listed.GetChildren(), nil
}

// DescribeTable describes the row table at path.
func (s *grpcSource) DescribeTable(ctx context.Context, path string) (*Ydb_Table.DescribeTableResult, error) {
	response, err := s.table.DescribeTable(ctx, &Ydb_Table.DescribeTableRequest{SessionId: s.session, Path: path})
	if err != nil {
		return nil, fmt.Errorf("describe YDB table %s: %w", path, err)
	}
	var described Ydb_Table.DescribeTableResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB table %s: %w", path, err)
	}
	return &described, nil
}

// operationResult unpacks a completed operation's result into result, or
// reports the status and the issues of one that did not succeed. Success is
// judged by the status alone: YDB answers some successful operations with
// issue text that begins with `Error:`.
func operationResult(operation *Ydb_Operations.Operation, result proto.Message) error {
	if err := operationStatus(operation); err != nil {
		return err
	}
	if err := operation.GetResult().UnmarshalTo(result); err != nil {
		return fmt.Errorf("decode the server's answer: %w", err)
	}
	return nil
}

// issueText flattens a tree of server issues into one line.
func issueText(issues []*Ydb_Issue.IssueMessage) string {
	var messages []string
	var walk func([]*Ydb_Issue.IssueMessage)
	walk = func(level []*Ydb_Issue.IssueMessage) {
		for _, issue := range level {
			if message := strings.TrimSpace(issue.GetMessage()); message != "" {
				messages = append(messages, message)
			}
			walk(issue.GetIssues())
		}
	}
	walk(issues)
	if len(messages) == 0 {
		return "no issue text"
	}
	return strings.Join(messages, ": ")
}

// operationStatus reports the status and the issues of an operation that did
// not complete successfully.
func operationStatus(operation *Ydb_Operations.Operation) error {
	if operation == nil {
		return errors.New("the server answered with no operation")
	}
	if !operation.GetReady() {
		return errors.New("the server answered with an operation that has not finished")
	}
	if status := operation.GetStatus(); status != Ydb.StatusIds_SUCCESS {
		return fmt.Errorf("%s: %s", status, issueText(operation.GetIssues()))
	}
	return nil
}
