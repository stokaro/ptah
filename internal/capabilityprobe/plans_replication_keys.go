package capabilityprobe

import (
	"context"
	"fmt"
	"path"
	"time"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/dbschema"
)

// withReplicationKeys adds the questions the YDB renderer, reader and planner
// decide an async replication and a transfer with: whether the target has
// each, and whether one names a secret by its path.
//
// On YDB each is asked in YDB's spelling and then read back through Ptah's
// reader, since no SQL describes either: the replication service does. A
// replication reads this same database, through the address its nodes
// advertise, and a transfer reads a changefeed's topic, which the namespace
// teardown drops with its table. Both are YDB's, so every other engine is
// asked the same statement and its refusal is the measurement.
func withReplicationKeys(p plan, dialect string) plan {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		p.experiments = append(p.experiments, ydbReplicationExperiments()...)
		return p
	}
	if _, ok := typeKeySpellingFor(dialect); !ok {
		return p
	}
	const connection = "CONNECTION_STRING = 'grpc://localhost:2136/?database=/local'"
	p.experiments = append(p.experiments,
		proven(capability.AsyncReplication, schemaChange{
			change: []string{"CREATE ASYNC REPLICATION rpk FOR rsrc AS rrep WITH (" + connection + ")"},
		}),
		proven(capability.Transfers, schemaChange{
			change: []string{"CREATE TRANSFER tpk FROM tsrc TO tdst USING ($msg) -> { return []; }"},
		}),
		proven(capability.ReplicationSecretPaths, schemaChange{
			change: []string{"CREATE ASYNC REPLICATION rsp FOR rsrc AS rsprep WITH (" + connection +
				", TOKEN_SECRET_PATH = 'probe_secret')"},
		}),
	)
	return p
}

// ydbReplicationExperiments creates a replication of a namespace table, a
// replication naming a token secret by its path, and a transfer from a
// changefeed's topic into a table, and reads each back.
func ydbReplicationExperiments() []experiment {
	t := ydbSpelling
	return []experiment{
		ydbReplicationExperiment(capability.AsyncReplication, nil, []string{t.table("rsrc", "id Uint64 NOT NULL", "id")},
			func(namespace, connection string) string {
				return fmt.Sprintf("CREATE ASYNC REPLICATION rpk FOR `%s` AS rrep WITH (CONNECTION_STRING = '%s', "+
					"CONSISTENCY_LEVEL = 'GLOBAL', COMMIT_INTERVAL = Interval('PT30S'))", path.Join(namespace, "rsrc"),
					connection)
			},
			ydbDescribedReplication("rpk",
				"the replication of rsrc into rrep at the global consistency level, committing every 30 seconds",
				func(namespace string, replication catalog.AsyncReplication) bool {
					spec := replication.Spec
					return len(spec.Items) == 1 && spec.Items[0].Source == path.Join(namespace, "rsrc") &&
						spec.Items[0].Target == path.Join(namespace, "rrep") && spec.ConsistencyLevel == "global" &&
						spec.CommitInterval == "PT30S"
				})),
		ydbReplicationExperiment(capability.ReplicationSecretPaths, []capability.Capability{capability.AsyncReplication},
			[]string{t.table("rssrc", "id Uint64 NOT NULL", "id")},
			func(namespace, connection string) string {
				return fmt.Sprintf("CREATE ASYNC REPLICATION rsp FOR `%s` AS rsprep WITH (CONNECTION_STRING = '%s', "+
					"TOKEN_SECRET_PATH = '%s')", path.Join(namespace, "rssrc"), connection,
					path.Join(namespace, "probe_secret"))
			},
			ydbDescribedReplication("rsp", "the replication naming its token secret by the path "+
				"<namespace>/probe_secret",
				func(namespace string, replication catalog.AsyncReplication) bool {
					return replication.Spec.Connection.TokenSecretPath == path.Join(namespace, "probe_secret")
				})),
		{
			decides: []capability.Capability{capability.Transfers},
			setup: []string{
				t.table("tsrc", "id Uint64 NOT NULL", "id"),
				"ALTER TABLE tsrc ADD CHANGEFEED feed WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON')",
				t.table("tdst", "partition Uint32 NOT NULL, offset Uint64 NOT NULL, message Utf8", "partition, offset"),
			},
			decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
				lambda := "($msg) -> { return [<| partition: $msg._partition, offset: $msg._offset, " +
					"message: CAST($msg._data AS Utf8) |>]; }"
				created := s.exec(ctx, fmt.Sprintf("CREATE TRANSFER tpk FROM `%s` TO `%s` USING %s "+
					"WITH (BATCH_SIZE_BYTES = 1048576)", path.Join(s.namespace, "tsrc/feed"),
					path.Join(s.namespace, "tdst"), lambda))
				attempts := []Attempt{created}
				if !created.Accepted {
					return verdicts{capability.Transfers: decided(false)}, attempts
				}
				read, transfer, found := readTransfer(ctx, s, "tpk")
				attempts = append(attempts, read)
				spec := transfer.Spec
				held := found && spec.Source == path.Join(s.namespace, "tsrc/feed") &&
					spec.Target == path.Join(s.namespace, "tdst") && spec.Lambda == lambda && spec.BatchSizeBytes == 1048576
				if !held {
					return verdicts{capability.Transfers: annotated(false, fmt.Sprintf("the CREATE TRANSFER was "+
						"accepted, and then expected the transfer from tsrc/feed into tdst through its lambda in "+
						"batches of 1 MiB, and it read %+v", transfer))}, attempts
				}
				return verdicts{capability.Transfers: decided(true)}, attempts
			},
		},
	}
}

// ydbReplicationExperiment decides key by running setup, creating a
// replication of this database with the statement create writes for the
// session's namespace and the address the cluster's nodes advertise, and
// running after. The statement is built at run time, because a replication's
// source path is resolved in the source database's root rather than under the
// session's pragma, and the address is the server's own.
func ydbReplicationExperiment(
	key capability.Capability,
	requires []capability.Capability,
	setup []string,
	create func(namespace, connection string) string,
	after check,
) experiment {
	return experiment{
		decides:  []capability.Capability{key},
		requires: requires,
		setup:    setup,
		decide: func(ctx context.Context, s *session) (verdicts, []Attempt) {
			connection, err := selfConnectionString(ctx, s)
			if err != nil {
				return verdicts{key: cannotDecide("the address the cluster's nodes reach this database through "+
					"was not read: %v", err)}, nil
			}
			created := s.exec(ctx, create(s.namespace, connection))
			attempts := []Attempt{created}
			if !created.Accepted {
				return verdicts{key: decided(false)}, attempts
			}
			attempt, held, did := after.run(ctx, s)
			attempts = append(attempts, attempt)
			if !held {
				return verdicts{key: annotated(false, "the CREATE ASYNC REPLICATION was accepted, and then expected "+
					after.expectation()+", and it "+did)}, attempts
			}
			return verdicts{key: decided(true)}, attempts
		},
	}
}

// selfConnector is what the probe needs of the connection's YDB schema writer
// to point a replication at the database it runs in.
type selfConnector interface {
	SelfConnectionString(ctx context.Context) (string, error)
}

// selfConnectionString is the connection string the cluster's nodes reach the
// session's database through.
func selfConnectionString(ctx context.Context, s *session) (string, error) {
	connector, ok := s.conn.SchemaWriter().(selfConnector)
	if !ok {
		return "", fmt.Errorf("the connection's schema writer %T reads no cluster address", s.conn.SchemaWriter())
	}
	return connector.SelfConnectionString(ctx)
}

// replicationSettle is how long a read of a replication waits for the server to
// resolve its items: CREATE ASYNC REPLICATION returns before the replication
// has found its tables, and a description read at once lists none.
const replicationSettle = 30 * time.Second

// ydbDescribedReplication reads the namespace through Ptah's YDB reader until
// its replication name lists an item, or the settle time ends, and holds when
// the replication reads back as want says.
func ydbDescribedReplication(
	name, expectation string,
	want func(namespace string, replication catalog.AsyncReplication) bool,
) check {
	return check{
		describes: expectation,
		inspect: func(ctx context.Context, s *session) (Attempt, bool, string) {
			attempt := Attempt{Statement: fmt.Sprintf("read async replication %s through Ptah's YDB reader",
				path.Join(s.database, s.namespace, name))}
			deadline := time.Now().Add(replicationSettle)
			for {
				db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
				if err != nil {
					attempt.ServerErr = err.Error()
					return attempt, false, "was refused"
				}
				attempt.Accepted = true
				replication, found := replicationNamed(db.AsyncReplications, name)
				switch {
				case found && want(s.namespace, replication):
					return attempt, true, fmt.Sprintf("read %+v", replication)
				case time.Now().After(deadline):
					if !found {
						return attempt, false, "found no such replication"
					}
					return attempt, false, fmt.Sprintf("read %+v", replication)
				}
				select {
				case <-ctx.Done():
					return attempt, false, "was canceled: " + ctx.Err().Error()
				case <-time.After(time.Second):
				}
			}
		},
	}
}

// replicationNamed finds a replication by name.
func replicationNamed(replications []catalog.AsyncReplication, name string) (catalog.AsyncReplication, bool) {
	for _, replication := range replications {
		if replication.Name == name {
			return replication, true
		}
	}
	return catalog.AsyncReplication{}, false
}

// readTransfer reads the namespace through Ptah's YDB reader and returns the
// transfer name.
func readTransfer(ctx context.Context, s *session, name string) (Attempt, catalog.Transfer, bool) {
	attempt := Attempt{Statement: fmt.Sprintf("read transfer %s through Ptah's YDB reader",
		path.Join(s.database, s.namespace, name))}
	db, err := dbschema.ReadSchemaWithSchemasContext(ctx, s.conn, []string{s.namespace})
	if err != nil {
		attempt.ServerErr = err.Error()
		return attempt, catalog.Transfer{}, false
	}
	attempt.Accepted = true
	for _, transfer := range db.Transfers {
		if transfer.Name == name {
			return attempt, transfer, true
		}
	}
	return attempt, catalog.Transfer{}, false
}
