package ydb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Scheme_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_Replication_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/Ydb_View_V1"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Replication"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Source is what the reader asks a YDB database: a directory's own entry and
// the entries under it, the description of a row table, of a view or of a
// coordination node, the description of a topic, which is how a standalone
// topic and a changefeed's retention and consumers are read, the description
// of an async replication or a transfer, and the database's users, groups and
// memberships. A path is absolute.
//
// A directory's own entry and a table's description each carry the object's
// owner and its permission entries, which is where the reader reads them from:
// the scheme service answers for exactly the objects the read walks.
type Source interface {
	ListDirectory(ctx context.Context, path string) (self *Ydb_Scheme.Entry, children []*Ydb_Scheme.Entry, err error)
	DescribeTable(ctx context.Context, path string) (*Ydb_Table.DescribeTableResult, error)
	DescribeView(ctx context.Context, path string) (*Ydb_View.DescribeViewResult, error)
	DescribeTopic(ctx context.Context, path string) (*Ydb_Topic.DescribeTopicResult, error)
	Principals(ctx context.Context) (Principals, error)
	DescribeReplication(ctx context.Context, path string) (*Ydb_Replication.DescribeReplicationResult, error)
	DescribeTransfer(ctx context.Context, path string) (*Ydb_Replication.DescribeTransferResult, error)
	DescribeCoordinationNode(ctx context.Context, path string) (*Ydb_Coordination.DescribeNodeResult, error)
}

// grpcSource answers through the SDK driver's gRPC connection with raw scheme
// and table service calls, and decodes the answers itself.
//
// The raw call is what keeps an index kind the SDK does not model from being
// read as another kind. ydb-go-sdk maps every index type it does not know to a
// plain global index, so a vector index described through it reads back as
// one; the raw description leaves the type empty and the data in fields the
// pinned protocol buffers do not know, where the reader decodes a vector index
// and refuses every other kind by name.
type grpcSource struct {
	scheme Ydb_Scheme_V1.SchemeServiceClient
	table  Ydb_Table_V1.TableServiceClient
	view   Ydb_View_V1.ViewServiceClient
	topic  Ydb_Topic_V1.TopicServiceClient
	// replication is the replication service, which describes an async
	// replication and a transfer. local-ydb leaves it out of the services it
	// starts (its configuration lists them, and `replication` is not among
	// them), while a cluster whose configuration lists none starts it with
	// the rest; YDB_GRPC_SERVICES=replication adds it to local-ydb.
	replication  Ydb_Replication_V1.ReplicationServiceClient
	coordination grpcCoordination
	session      string
	database     string
}

// newGRPCSource opens a table session for one read. The caller ends it with
// the returned function.
func newGRPCSource(ctx context.Context, driver *ydbsdk.Driver) (*grpcSource, func(), error) {
	connection := ydbsdk.GRPCConn(driver)
	source := &grpcSource{
		scheme:       Ydb_Scheme_V1.NewSchemeServiceClient(connection),
		table:        Ydb_Table_V1.NewTableServiceClient(connection),
		view:         Ydb_View_V1.NewViewServiceClient(connection),
		topic:        Ydb_Topic_V1.NewTopicServiceClient(connection),
		replication:  Ydb_Replication_V1.NewReplicationServiceClient(connection),
		coordination: grpcCoordination{client: Ydb_Coordination_V1.NewCoordinationServiceClient(connection)},
		database:     driver.Name(),
	}
	response, err := source.table.CreateSession(ctx, &Ydb_Table.CreateSessionRequest{})
	if err != nil {
		return nil, nil, fmt.Errorf("create a YDB table session: %w", WithoutStackFrames(err))
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

// ListDirectory returns the directory at path, with its owner and its
// permission entries, and the entries directly under it.
func (s *grpcSource) ListDirectory(ctx context.Context, path string) (*Ydb_Scheme.Entry, []*Ydb_Scheme.Entry, error) {
	response, err := s.scheme.ListDirectory(ctx, &Ydb_Scheme.ListDirectoryRequest{Path: path})
	if err != nil {
		return nil, nil, fmt.Errorf("list YDB directory %s: %w", path, WithoutStackFrames(err))
	}
	var listed Ydb_Scheme.ListDirectoryResult
	if err := operationResult(response.GetOperation(), &listed); err != nil {
		return nil, nil, fmt.Errorf("list YDB directory %s: %w", path, err)
	}
	return listed.GetSelf(), listed.GetChildren(), nil
}

// Principals reads the database's users, groups and memberships from
// .sys/auth_*, one snapshot read-only query each on the read's own session.
//
// The raw table service runs them, not the SDK's query client: the client
// retries an ABORTED answer, and ABORTED is how YDB refuses .sys to a user who
// may list the database and not read it, so the read would retry until its
// context ended.
func (s *grpcSource) Principals(ctx context.Context) (Principals, error) {
	return readPrincipals(ctx, s.database, s.query)
}

// query runs one read-only statement and returns its first result set.
func (s *grpcSource) query(ctx context.Context, statement string) (*Ydb.ResultSet, error) {
	response, err := s.table.ExecuteDataQuery(ctx, &Ydb_Table.ExecuteDataQueryRequest{
		SessionId: s.session,
		TxControl: &Ydb_Table.TransactionControl{
			TxSelector: &Ydb_Table.TransactionControl_BeginTx{BeginTx: &Ydb_Table.TransactionSettings{
				TxMode: &Ydb_Table.TransactionSettings_SnapshotReadOnly{SnapshotReadOnly: &Ydb_Table.SnapshotModeSettings{}},
			}},
			CommitTx: true,
		},
		Query: &Ydb_Table.Query{Query: &Ydb_Table.Query_YqlText{YqlText: statement}},
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", statement, WithoutStackFrames(err))
	}
	operation := response.GetOperation()
	if operation.GetReady() && isPrincipalsRefusal(operation.GetStatus(), issueText(operation.GetIssues())) {
		return nil, fmt.Errorf("%w: %s: %s", ErrPrincipalsRefused, operation.GetStatus(), issueText(operation.GetIssues()))
	}
	var result Ydb_Table.ExecuteQueryResult
	if err := operationResult(operation, &result); err != nil {
		return nil, fmt.Errorf("%s: %w", statement, err)
	}
	if len(result.GetResultSets()) == 0 {
		return &Ydb.ResultSet{}, nil
	}
	return result.GetResultSets()[0], nil
}

// DescribeTable describes the row table at path.
func (s *grpcSource) DescribeTable(ctx context.Context, path string) (*Ydb_Table.DescribeTableResult, error) {
	// IncludeSetVal makes a sequence that was restarted say so: its
	// description then carries the value the restart set, which YDB replays
	// on every later ALTER SEQUENCE.
	response, err := s.table.DescribeTable(ctx, &Ydb_Table.DescribeTableRequest{
		SessionId: s.session, Path: path, IncludeSetVal: true,
	})
	if err != nil {
		return nil, fmt.Errorf("describe YDB table %s: %w", path, WithoutStackFrames(err))
	}
	var described Ydb_Table.DescribeTableResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB table %s: %w", path, err)
	}
	return &described, nil
}

// DescribeView describes the view at path.
//
// The view service's protocol buffers sit in ydb-go-genproto's draft tree,
// and the service answers on every line Ptah measured: 25.1.4.7 and 26.2.1.14
// both return the view's query, and a path that is not a view is a
// SCHEME_ERROR (`Expected a view, but got: EPathTypeTable`). SHOW CREATE VIEW
// is not the source: 25.1.4.7 answers it with a parse error (`Missing TABLE`),
// and 26.2.1.14, which has it, writes the query in a layout of its own rather
// than the text it stores.
func (s *grpcSource) DescribeView(ctx context.Context, path string) (*Ydb_View.DescribeViewResult, error) {
	response, err := s.view.DescribeView(ctx, &Ydb_View.DescribeViewRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("describe YDB view %s: %w", path, WithoutStackFrames(err))
	}
	var described Ydb_View.DescribeViewResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB view %s: %w", path, err)
	}
	return &described, nil
}

// DescribeTopic describes the topic at path: its settings and its consumers,
// without statistics. A changefeed's topic is described at
// `<table>/<changefeed>`: measured on 25.1.4.7 and 26.2.1.14, the path names
// the changefeed's stream, and DescribeTopic on it answers with its
// retention, partitions and consumers.
func (s *grpcSource) DescribeTopic(ctx context.Context, path string) (*Ydb_Topic.DescribeTopicResult, error) {
	response, err := s.topic.DescribeTopic(ctx, &Ydb_Topic.DescribeTopicRequest{Path: path})
	if err != nil {
		return nil, fmt.Errorf("describe YDB topic %s: %w", path, WithoutStackFrames(err))
	}
	var described Ydb_Topic.DescribeTopicResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB topic %s: %w", path, err)
	}
	return &described, nil
}

// DescribeReplication describes the async replication at path: its
// connection, its credential by the secret it names, its consistency, its
// items and its state.
func (s *grpcSource) DescribeReplication(
	ctx context.Context,
	path string,
) (*Ydb_Replication.DescribeReplicationResult, error) {
	response, err := s.replication.DescribeReplication(ctx, &Ydb_Replication.DescribeReplicationRequest{Path: path})
	if err != nil {
		return nil, replicationServiceError("describe YDB async replication "+path, err)
	}
	var described Ydb_Replication.DescribeReplicationResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB async replication %s: %w", path, err)
	}
	return &described, nil
}

// DescribeTransfer describes the transfer at path: its connection, its
// source, its destination, its lambda as stored, its consumer, its batch
// settings and its state.
func (s *grpcSource) DescribeTransfer(ctx context.Context, path string) (*Ydb_Replication.DescribeTransferResult, error) {
	response, err := s.replication.DescribeTransfer(ctx, &Ydb_Replication.DescribeTransferRequest{Path: path})
	if err != nil {
		return nil, replicationServiceError("describe YDB transfer "+path, err)
	}
	var described Ydb_Replication.DescribeTransferResult
	if err := operationResult(response.GetOperation(), &described); err != nil {
		return nil, fmt.Errorf("describe YDB transfer %s: %w", path, err)
	}
	return &described, nil
}

// ErrReplicationServiceUnavailable is the answer of a cluster that does not
// serve YDB's replication API, which describes async replications and
// transfers. Measured on local-ydb 25.1.4.7 and 26.2.1.14 with their default
// configuration: the CLI's `scheme describe` of a replication answers `GRpc
// error: (12)`, UNIMPLEMENTED, and describes it once the container starts
// with YDB_GRPC_SERVICES=replication.
var ErrReplicationServiceUnavailable = errors.New("the YDB cluster does not serve the replication API " +
	"(grpc_config services lack replication)")

// replicationServiceError wraps an error of a replication service call,
// naming [ErrReplicationServiceUnavailable] where the server answered
// UNIMPLEMENTED.
func replicationServiceError(subject string, err error) error {
	if grpcstatus.Code(err) == grpccodes.Unimplemented {
		return fmt.Errorf("%s: %w: %w", subject, ErrReplicationServiceUnavailable, WithoutStackFrames(err))
	}
	return fmt.Errorf("%s: %w", subject, WithoutStackFrames(err))
}

// DescribeCoordinationNode describes the coordination node at path.
func (s *grpcSource) DescribeCoordinationNode(ctx context.Context, path string) (*Ydb_Coordination.DescribeNodeResult, error) {
	return s.coordination.DescribeNode(ctx, path)
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
		return &statusError{status: status, issues: issueText(operation.GetIssues())}
	}
	return nil
}

// statusError is an operation the server finished with a status other than
// success.
type statusError struct {
	status Ydb.StatusIds_StatusCode
	issues string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("%s: %s", e.status, e.issues)
}

// isSchemeError reports a SCHEME_ERROR, which is how YDB answers a path that
// does not exist -- or one the account may not see, which it does not tell
// apart: measured on 26.2.1.14, `Cannot find table ... because it does not
// exist or you do not have access permissions`.
func isSchemeError(err error) bool {
	status, ok := errors.AsType[*statusError](err)
	return ok && status.status == Ydb.StatusIds_SCHEME_ERROR
}

// isUnauthorized reports an operation the server refused for want of a right.
func isUnauthorized(err error) bool {
	status, ok := errors.AsType[*statusError](err)
	return ok && status.status == Ydb.StatusIds_UNAUTHORIZED
}
