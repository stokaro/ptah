package capabilityprobe

import (
	"ptah.run/core/platform/capability"
)

// withReplicationKeys adds the questions whether the target has an async
// replication and a transfer, and whether one names a secret by its path.
// Both are YDB's, which the YDB provider measures on its own lines, so every
// engine here is asked the statement and its refusal is the measurement.
func withReplicationKeys(p plan, dialect string) plan {
	if _, ok := typeKeySpellingFor(dialect); !ok {
		return p
	}
	const connection = "CONNECTION_STRING = 'grpc://localhost:2136/?database=/local'"
	p.experiments = append(p.experiments,
		proven(capability.AsyncReplication, schemaChange{
			change: []string{"CREATE ASYNC REPLICATION repl_key FOR repl_src AS repl_replica WITH (" + connection + ")"},
		}),
		proven(capability.Transfers, schemaChange{
			change: []string{"CREATE TRANSFER xfer_key FROM xfer_src TO xfer_dst USING ($msg) -> { return []; }"},
		}),
		proven(capability.ReplicationSecretPaths, schemaChange{
			change: []string{"CREATE ASYNC REPLICATION repl_secret_key FOR repl_secret_src AS repl_secret_replica " +
				"WITH (" + connection + ", TOKEN_SECRET_PATH = 'repl_secret')"},
		}),
	)
	return p
}
