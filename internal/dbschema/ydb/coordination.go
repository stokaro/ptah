package ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Coordination_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"google.golang.org/protobuf/reflect/protoreflect"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/internal/ydbcoordination"
)

// ErrNoCoordinationNode is the error [Coordination.DescribeNode] wraps when no
// coordination node is at the path: the service answers SCHEME_ERROR both
// when nothing is there and when an object of another kind is, measured on
// 25.1.4.7 and 26.2.1.14.
var ErrNoCoordinationNode = errors.New("no coordination node at this path")

// Coordination is what Ptah asks of YDB's coordination service: the
// configuration of a node, and the creation, change and removal of one. Each
// takes an absolute path, and DescribeNode answers an error wrapping
// [ErrNoCoordinationNode] for a path that holds no node. Ptah's implementation
// answers through raw service calls, so a consistency mode the SDK does not
// know is not read as another one.
type Coordination interface {
	DescribeNode(ctx context.Context, absolute string) (*Ydb_Coordination.DescribeNodeResult, error)
	CreateNode(ctx context.Context, absolute string, config *Ydb_Coordination.Config) error
	AlterNode(ctx context.Context, absolute string, config *Ydb_Coordination.Config) error
	DropNode(ctx context.Context, absolute string) error
}

// grpcCoordination answers through raw coordination service calls.
type grpcCoordination struct {
	client Ydb_Coordination_V1.CoordinationServiceClient
}

func (c grpcCoordination) DescribeNode(ctx context.Context, absolute string) (*Ydb_Coordination.DescribeNodeResult, error) {
	response, err := c.client.DescribeNode(ctx, &Ydb_Coordination.DescribeNodeRequest{Path: absolute})
	if err != nil {
		return nil, fmt.Errorf("describe YDB coordination node %s: %w", absolute, WithoutStackFrames(err))
	}
	var described Ydb_Coordination.DescribeNodeResult
	err = operationResult(response.GetOperation(), &described)
	switch {
	case isSchemeError(err):
		return nil, fmt.Errorf("describe YDB coordination node %s: %w: %w", absolute, ErrNoCoordinationNode, err)
	case err != nil:
		return nil, fmt.Errorf("describe YDB coordination node %s: %w", absolute, err)
	}
	return &described, nil
}

func (c grpcCoordination) CreateNode(ctx context.Context, absolute string, config *Ydb_Coordination.Config) error {
	response, err := c.client.CreateNode(ctx, &Ydb_Coordination.CreateNodeRequest{Path: absolute, Config: config})
	if err != nil {
		return WithoutStackFrames(err)
	}
	return operationStatus(response.GetOperation())
}

func (c grpcCoordination) AlterNode(ctx context.Context, absolute string, config *Ydb_Coordination.Config) error {
	response, err := c.client.AlterNode(ctx, &Ydb_Coordination.AlterNodeRequest{Path: absolute, Config: config})
	if err != nil {
		return WithoutStackFrames(err)
	}
	return operationStatus(response.GetOperation())
}

func (c grpcCoordination) DropNode(ctx context.Context, absolute string) error {
	response, err := c.client.DropNode(ctx, &Ydb_Coordination.DropNodeRequest{Path: absolute})
	if err != nil {
		return WithoutStackFrames(err)
	}
	return operationStatus(response.GetOperation())
}

// consistencyModes and counterModes name the enum values Ptah reads. A value
// outside them is refused by its number rather than read as the nearest one:
// measured on 25.1.4.7 and 26.2.1.14, YDB stores and reports a mode of 7.
var (
	consistencyModes = map[Ydb_Coordination.ConsistencyMode]string{
		Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_UNSET:   "",
		Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_STRICT:  ydbcoordination.ConsistencyStrict,
		Ydb_Coordination.ConsistencyMode_CONSISTENCY_MODE_RELAXED: ydbcoordination.ConsistencyRelaxed,
	}
	counterModes = map[Ydb_Coordination.RateLimiterCountersMode]string{
		Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_UNSET:      "",
		Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_AGGREGATED: ydbcoordination.CountersAggregated,
		Ydb_Coordination.RateLimiterCountersMode_RATE_LIMITER_COUNTERS_MODE_DETAILED:   ydbcoordination.CountersDetailed,
	}
)

// decodeCoordinationNode reads a node's description into the catalog. The
// configuration is kept as YDB stores it, with every setting nobody sent
// unset; the comparison fills in the defaults. A setting or a mode the pinned
// protocol buffers do not model is refused, because read as absent it would
// be planned away on every run.
func decodeCoordinationNode(schema, name string, described *Ydb_Coordination.DescribeNodeResult) (catalog.CoordinationNode, error) {
	config := described.GetConfig()
	for _, message := range []protoreflect.ProtoMessage{described, config} {
		if unknown := unknownFields(message); len(unknown) > 0 {
			return catalog.CoordinationNode{}, fmt.Errorf("its description carries field %s, which this build of "+
				"Ptah does not read", joinNumbers(unknown))
		}
	}
	read, readKnown := consistencyModes[config.GetReadConsistencyMode()]
	attach, attachKnown := consistencyModes[config.GetAttachConsistencyMode()]
	counters, countersKnown := counterModes[config.GetRateLimiterCountersMode()]
	switch {
	case !readKnown:
		return catalog.CoordinationNode{}, fmt.Errorf("its read_consistency_mode is %d, which Ptah does not know",
			config.GetReadConsistencyMode())
	case !attachKnown:
		return catalog.CoordinationNode{}, fmt.Errorf("its attach_consistency_mode is %d, which Ptah does not know",
			config.GetAttachConsistencyMode())
	case !countersKnown:
		return catalog.CoordinationNode{}, fmt.Errorf("its rate_limiter_counters_mode is %d, which Ptah does not know",
			config.GetRateLimiterCountersMode())
	}
	return catalog.CoordinationNode{
		Schema: schema,
		Name:   name,
		Spec: ast.CoordinationNodeSpec{
			SelfCheckPeriodMillis:    config.GetSelfCheckPeriodMillis(),
			SessionGracePeriodMillis: config.GetSessionGracePeriodMillis(),
			ReadConsistencyMode:      read,
			AttachConsistencyMode:    attach,
			RateLimiterCountersMode:  counters,
		},
	}, nil
}

// encodeCoordinationConfig writes the settings spec sets as the configuration
// the coordination service takes; a setting spec leaves unset is sent unset,
// which the service reads as "keep" on a change and as "the default" on a
// creation.
func encodeCoordinationConfig(spec ast.CoordinationNodeSpec) *Ydb_Coordination.Config {
	config := &Ydb_Coordination.Config{
		SelfCheckPeriodMillis:    spec.SelfCheckPeriodMillis,
		SessionGracePeriodMillis: spec.SessionGracePeriodMillis,
	}
	for mode, name := range consistencyModes {
		if name == "" {
			continue
		}
		if spec.ReadConsistencyMode == name {
			config.ReadConsistencyMode = mode
		}
		if spec.AttachConsistencyMode == name {
			config.AttachConsistencyMode = mode
		}
	}
	for mode, name := range counterModes {
		if name != "" && spec.RateLimiterCountersMode == name {
			config.RateLimiterCountersMode = mode
		}
	}
	return config
}

// errNoCoordinationService is returned for a coordination node statement on a
// connection that reaches no coordination service.
var errNoCoordinationService = errors.New("this YDB connection reaches no coordination service")

// RunCoordinationStatement runs query, one of Ptah's coordination node
// statements (see [ydbcoordination.Recognize]), through service on the
// database whose absolute path is database. root is the absolute path the
// connection treats as its database: database itself, or a dev realm's
// directory under it.
//
// It refuses a node outside root, Ptah's own lock node at the root of the
// database, and a path segment that starts with a dot before it calls the
// service. A CREATE
// first asks whether the node exists, because the service answers a creation
// of an existing node with success and leaves its configuration as it was
// (measured on 25.1.4.7 and 26.2.1.14: `path exist, request accepts it`),
// which would read as the declared configuration having been applied. An
// ALTER merges the settings it changes with the node's own, and refuses a
// result the node would not run with as written.
func RunCoordinationStatement(
	ctx context.Context,
	service Coordination,
	database, root string,
	query ydbcoordination.Query,
) error {
	database = "/" + strings.Trim(database, "/")
	root = "/" + strings.Trim(root, "/")
	absolute, err := query.Absolute(database)
	if err != nil {
		return err
	}
	relative, inside := strings.CutPrefix(absolute, strings.TrimSuffix(root, "/")+"/")
	if !inside {
		return fmt.Errorf("%w: coordination node %s is not in %s", ydbcoordination.ErrStatement, absolute, root)
	}
	if root != database {
		// Ptah's lock node is at the root of the database, not of a realm.
		relative = strings.TrimPrefix(absolute, database+"/")
	}
	directory := path.Dir(relative)
	if directory == "." {
		directory = ""
	}
	if err := ydbcoordination.RefuseName(directory, path.Base(relative)); err != nil {
		return fmt.Errorf("%w: %w", ydbcoordination.ErrStatement, err)
	}
	switch query.Verb {
	case ydbcoordination.Create:
		return createCoordinationNode(ctx, service, absolute, query.Spec)
	case ydbcoordination.Alter:
		return alterCoordinationNode(ctx, service, absolute, query.Spec)
	default:
		if err := service.DropNode(ctx, absolute); err != nil {
			return fmt.Errorf("drop YDB coordination node %s: %w", absolute, err)
		}
		return nil
	}
}

// createCoordinationNode creates the node at absolute, which must not exist.
func createCoordinationNode(ctx context.Context, service Coordination, absolute string, spec ast.CoordinationNodeSpec) error {
	_, err := service.DescribeNode(ctx, absolute)
	switch {
	case err == nil:
		return fmt.Errorf("create YDB coordination node %s: the node already exists", absolute)
	case !errors.Is(err, ErrNoCoordinationNode):
		return err
	}
	if err := service.CreateNode(ctx, absolute, encodeCoordinationConfig(spec)); err != nil {
		return fmt.Errorf("create YDB coordination node %s: %w", absolute, err)
	}
	return nil
}

// alterCoordinationNode changes the settings changes names on the node at
// absolute.
func alterCoordinationNode(ctx context.Context, service Coordination, absolute string, changes ast.CoordinationNodeSpec) error {
	described, err := service.DescribeNode(ctx, absolute)
	if err != nil {
		return fmt.Errorf("change YDB coordination node %s: %w", absolute, err)
	}
	current, err := decodeCoordinationNode(path.Dir(absolute), path.Base(absolute), described)
	if err != nil {
		return fmt.Errorf("change YDB coordination node %s: %w", absolute, err)
	}
	if err := ydbcoordination.Validate(ydbcoordination.Merge(current.Spec, changes)); err != nil {
		return fmt.Errorf("%w: change YDB coordination node %s: %w", ydbcoordination.ErrStatement, absolute, err)
	}
	if err := service.AlterNode(ctx, absolute, encodeCoordinationConfig(changes)); err != nil {
		return fmt.Errorf("change YDB coordination node %s: %w", absolute, err)
	}
	return nil
}
