package ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Replication"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/tableref"
)

// replicaAttribute is the attribute YDB gives a table an async replication
// writes, measured on 25.1.4.7 and 26.2.1.14: `__async_replica = true` while
// the replication runs, kept when the replication is dropped without being
// failed over, and removed by the failover (`SET (STATE = 'DONE')`), after
// which the table is an ordinary one.
const replicaAttribute = "__async_replica"

// isReplica reports a table an async replication writes or wrote without
// failing over, which YDB keeps read-only.
func isReplica(attributes map[string]string) bool {
	return attributes[replicaAttribute] == "true"
}

// replicaTable records a table an async replication writes. It is recorded
// rather than described: YDB refuses every write and every ALTER on it (`path
// is an async replica table`), and a plan that read it as an ordinary table
// would drop it as undeclared, or alter it.
func replicaTable(schema, name string) coverage.Object {
	return coverage.Object{
		Kind:       ydbschema.CoverageReplicaTable,
		Name:       tableref.Canonical(schema, name),
		Reason:     coverage.Unsupported,
		Provenance: coverage.Observed,
	}
}

// replication adds one described async replication, or records it where the
// cluster does not serve the replication API.
func (r *Reader) replication(ctx context.Context, source Source, schema, name string, db *catalog.Database, unread *unreadObjects) error {
	objectPath := r.absolute(schema, name)
	described, err := source.DescribeReplication(ctx, objectPath)
	if errors.Is(err, ErrReplicationServiceUnavailable) {
		unread.add(ydbreplication.ReplicationKind, ydbreplication.ReplicationRef(schema, name), ydbreplication.ServiceUnavailableReason)
		return nil
	}
	if err != nil {
		return err
	}
	spec, state, err := r.decodeReplication(described)
	if err != nil {
		return fmt.Errorf("YDB async replication %s: %w", objectPath, err)
	}
	db.FeatureObjects, err = db.FeatureObjects.With(ydbreplication.ObservedReplicationObject(schema, name, spec, state))
	return err
}

// transfer adds one described transfer, or records it where the cluster does
// not serve the replication API.
func (r *Reader) transfer(ctx context.Context, source Source, schema, name string, db *catalog.Database, unread *unreadObjects) error {
	objectPath := r.absolute(schema, name)
	described, err := source.DescribeTransfer(ctx, objectPath)
	if errors.Is(err, ErrReplicationServiceUnavailable) {
		unread.add(ydbreplication.TransferKind, ydbreplication.TransferRef(schema, name), ydbreplication.ServiceUnavailableReason)
		return nil
	}
	if err != nil {
		return err
	}
	spec, state, err := r.decodeTransfer(described)
	if err != nil {
		return fmt.Errorf("YDB transfer %s: %w", objectPath, err)
	}
	db.FeatureObjects, err = db.FeatureObjects.With(ydbreplication.ObservedTransferObject(schema, name, spec, state))
	return err
}

// decodeReplication reads a replication's description as the connection,
// items and consistency it holds, and its state.
//
// A field the pinned protocol buffers do not know is refused by number, and so
// is a credential kind other than a token secret or a user's password secret:
// read as no credential, it would be left out of a declaration made from the
// read. Each item's source is read relative to the source database and its
// target relative to the root the reader reads, where each lies under it.
func (r *Reader) decodeReplication(
	described *Ydb_Replication.DescribeReplicationResult,
) (ydbreplication.ReplicationSpec, string, error) {
	if err := refuseUnknownReplicationFields(map[string]protoreflect.ProtoMessage{
		"its description":     described,
		"its connection":      described.GetConnectionParams(),
		"its global settings": described.GetGlobalConsistency(),
	}); err != nil {
		return ydbreplication.ReplicationSpec{}, "", err
	}
	connection, err := r.decodeConnection(described.GetConnectionParams())
	if err != nil {
		return ydbreplication.ReplicationSpec{}, "", err
	}
	spec := ydbreplication.ReplicationSpec{Connection: connection}
	database := described.GetConnectionParams().GetDatabase()
	for _, item := range described.GetItems() {
		spec.Items = append(spec.Items, ydbreplication.Item{
			Source: ydbreplication.SourceKey(item.GetSourcePath(), database),
			Target: r.relative(item.GetDestinationPath()),
		})
	}
	if global := described.GetGlobalConsistency(); global != nil {
		spec.ConsistencyLevel = ydbreplication.ConsistencyGlobal
		millis, whole := durationMillis(global.GetCommitInterval())
		if !whole {
			return ydbreplication.ReplicationSpec{}, "", errors.New("its commit interval holds a fraction of a millisecond, " +
				"which no declaration Ptah reads can name")
		}
		if millis != ydbreplication.DefaultCommitIntervalMillis {
			spec.CommitInterval = ydbreplication.FormatMillis(millis)
		}
	}
	return spec, replicationState(described), nil
}

// decodeTransfer reads a transfer's description as its connection, source,
// target, lambda, consumer and batch settings, and its state.
//
// The lambda is read as written: YDB stores it as `$__ydb_transfer_lambda =
// <lambda>;`, measured on 25.1.4.7 with the flag on and 26.2.1.14 alike, and
// a lambda in that form reads back as the text between. One stored in any
// other form -- written through a named lambda, or under a PRAGMA, each of
// which YDB keeps in the text before it -- is read whole, so it compares
// unequal to any inline declaration and a plan sets the declared one.
func (r *Reader) decodeTransfer(described *Ydb_Replication.DescribeTransferResult) (ydbreplication.TransferSpec, string, error) {
	if err := refuseUnknownReplicationFields(map[string]protoreflect.ProtoMessage{
		"its description":    described,
		"its connection":     described.GetConnectionParams(),
		"its batch settings": described.GetBatchSettings(),
	}); err != nil {
		return ydbreplication.TransferSpec{}, "", err
	}
	connection, err := r.decodeConnection(described.GetConnectionParams())
	if err != nil {
		return ydbreplication.TransferSpec{}, "", err
	}
	spec := ydbreplication.TransferSpec{
		Connection: connection,
		Target:     r.relative(described.GetDestinationPath()),
		Lambda:     storedLambda(described.GetTransformationLambda()),
		Consumer:   described.GetConsumerName(),
	}
	if connection.ConnectionString != "" {
		spec.Source = ydbreplication.SourceKey(described.GetSourcePath(), described.GetConnectionParams().GetDatabase())
	} else {
		spec.Source = r.localSource(described.GetSourcePath())
	}
	batch := described.GetBatchSettings()
	if size := batch.GetSizeBytes(); size != 0 && size != ydbreplication.DefaultBatchSizeBytes {
		spec.BatchSizeBytes = size
	}
	if flush := batch.GetFlushInterval(); flush != nil {
		millis, whole := durationMillis(flush)
		if !whole || millis%1000 != 0 {
			return ydbreplication.TransferSpec{}, "", errors.New("its flush interval holds a fraction of a second, which no " +
				"declaration Ptah reads can name")
		}
		if seconds := millis / 1000; seconds != ydbreplication.DefaultFlushIntervalSeconds {
			spec.FlushInterval = ydbreplication.FormatMillis(millis)
		}
	}
	return spec, transferState(described), nil
}

// decodeConnection reads a connection: its string rebuilt from the endpoint,
// the database and TLS, as YDB reports them, and its credential by the secret
// it names. A connection with no endpoint is a transfer's of a topic in its
// own database, which names none (measured: `grpc:///?database=`).
//
// A secret named by its path reads back as the secret's absolute path, and a
// secret named by name as written, so a leading slash tells the two apart: a
// path under the root the reader reads is read relative to it, as a
// declaration writes it.
func (r *Reader) decodeConnection(params *Ydb_Replication.ConnectionParams) (ydbreplication.Connection, error) {
	var connection ydbreplication.Connection
	if params.GetEndpoint() != "" {
		connection.ConnectionString = ydbreplication.Endpoint{
			Secure:   params.GetEnableSsl(),
			Address:  params.GetEndpoint(),
			Database: params.GetDatabase(),
		}.String()
	}
	for _, credential := range []protoreflect.ProtoMessage{params.GetStaticCredentials(), params.GetOauth()} {
		if err := refuseUnknownReplicationFields(map[string]protoreflect.ProtoMessage{"its credential": credential}); err != nil {
			return ydbreplication.Connection{}, err
		}
	}
	switch credentials := params.GetCredentials().(type) {
	case nil:
	case *Ydb_Replication.ConnectionParams_Oauth:
		secret := credentials.Oauth.GetTokenSecretName()
		if strings.HasPrefix(secret, "/") {
			connection.TokenSecretPath = r.relative(secret)
		} else {
			connection.TokenSecretName = secret
		}
	case *Ydb_Replication.ConnectionParams_StaticCredentials_:
		connection.User = credentials.StaticCredentials.GetUser()
		secret := credentials.StaticCredentials.GetPasswordSecretName()
		switch {
		case strings.HasPrefix(secret, "/"):
			connection.PasswordSecretPath = r.relative(secret)
		case secret != "":
			connection.PasswordSecretName = secret
		}
	default:
		return ydbreplication.Connection{}, fmt.Errorf("its connection signs in with %T, which Ptah does not read",
			credentials)
	}
	return connection, nil
}

// transferLambda is the form YDB stores an inline lambda in.
var transferLambda = regexp.MustCompile(`(?s)\A\$__ydb_transfer_lambda = (.*);\n\z`)

// storedLambda reads the lambda out of the text YDB stores, or returns the
// text whole when it holds more than one inline lambda.
func storedLambda(stored string) string {
	if match := transferLambda.FindStringSubmatch(stored); match != nil && !strings.Contains(match[1],
		"$__ydb_transfer_lambda") {
		return match[1]
	}
	return stored
}

// localSource reads the source of a transfer of a topic in its own database
// relative to the root the reader reads.
//
// 26.1.1.22 and 26.2.1.14 keep a relative source as written, and 25.2.1.24 to
// 25.4.1.15 (and 25.1.4.7 with the flag on) keep it behind a slash: `tp`
// reads back as `/tp`. A source under the root is read relative to it; any
// other with a leading slash is the older lines' spelling of a relative one.
func (r *Reader) localSource(source string) string {
	if relative := r.relative(source); relative != source {
		return relative
	}
	return strings.Trim(source, "/")
}

// relative is an absolute path relative to the root the reader reads where it
// lies under it, and the path unchanged otherwise.
func (r *Reader) relative(absolute string) string {
	root := strings.TrimRight(r.database, "/")
	if rest, under := strings.CutPrefix(absolute, root+"/"); under {
		return path.Clean(rest)
	}
	return absolute
}

// replicationState is the state a replication reports.
func replicationState(described *Ydb_Replication.DescribeReplicationResult) string {
	switch {
	case described.GetDone() != nil:
		return ydbreplication.StateDone
	case described.GetPaused() != nil:
		return ydbreplication.StatePaused
	case described.GetError() != nil:
		return ydbreplication.StateError
	case described.GetRunning() != nil:
		return ydbreplication.StateRunning
	default:
		return ""
	}
}

// transferState is the state a transfer reports.
func transferState(described *Ydb_Replication.DescribeTransferResult) string {
	switch {
	case described.GetDone() != nil:
		return ydbreplication.StateDone
	case described.GetPaused() != nil:
		return ydbreplication.StatePaused
	case described.GetError() != nil:
		return ydbreplication.StateError
	case described.GetRunning() != nil:
		return ydbreplication.StateRunning
	default:
		return ""
	}
}

// durationMillis reads a duration as whole milliseconds, and reports false for
// one holding a fraction of a millisecond.
func durationMillis(duration *durationpb.Duration) (uint64, bool) {
	if duration == nil {
		return 0, true
	}
	value := duration.AsDuration()
	if value < 0 || value%time.Millisecond != 0 {
		return 0, false
	}
	return uint64(value / time.Millisecond), true
}

// refuseUnknownReplicationFields refuses a description whose messages carry
// a field the pinned protocol buffers do not know, naming the message and the
// field numbers. A nil message carries none.
func refuseUnknownReplicationFields(messages map[string]protoreflect.ProtoMessage) error {
	for subject, message := range messages {
		if message == nil || !message.ProtoReflect().IsValid() {
			continue
		}
		if numbers := unknownFields(message); len(numbers) > 0 {
			return fmt.Errorf("%s holds fields %s, which Ptah does not read", subject, joinNumbers(numbers))
		}
	}
	return nil
}
