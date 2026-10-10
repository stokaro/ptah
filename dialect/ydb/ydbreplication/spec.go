package ydbreplication

// Connection is how a YDB async replication or transfer reaches the database
// it reads: the connection string and the credentials, each credential named
// by the secret that holds it and never by its value.
//
// A field left at its zero value declares nothing. At most one credential
// form is set: a token secret, by name or by path, or a user with a password
// secret, by name or by path. A name refers to an object secret (`CREATE
// OBJECT ... (TYPE SECRET)`), a path to a scheme secret (`CREATE SECRET`),
// relative to the database the replication runs in.
type Connection struct {
	// ConnectionString is `CONNECTION_STRING`, the source database as
	// `grpc://host:port/?database=/path` or `grpcs://...`. A transfer that
	// reads a topic of its own database leaves it empty.
	ConnectionString string `json:"connection_string,omitempty"`
	// TokenSecretName is `TOKEN_SECRET_NAME`.
	TokenSecretName string `json:"token_secret_name,omitempty"`
	// TokenSecretPath is `TOKEN_SECRET_PATH`.
	TokenSecretPath string `json:"token_secret_path,omitempty"`
	// User is `USER`, the account a password secret signs in as.
	User string `json:"user,omitempty"`
	// PasswordSecretName is `PASSWORD_SECRET_NAME`.
	PasswordSecretName string `json:"password_secret_name,omitempty"`
	// PasswordSecretPath is `PASSWORD_SECRET_PATH`.
	PasswordSecretPath string `json:"password_secret_path,omitempty"`
}

// Item is one `FOR <source> AS <target>` pair of an async replication: a
// table, or a directory of tables, of the source database and the path YDB
// creates its replica at in this one.
type Item struct {
	// Source is the path in the source database, relative to its root or
	// absolute.
	Source string `json:"source"`
	// Target is the path in this database, relative to its root.
	Target string `json:"target"`
}

// ReplicationSpec is a YDB async replication: what `CREATE ASYNC REPLICATION
// <name> FOR <source> AS <target>, ... WITH (...)` takes.
//
// YDB creates each target itself as a read-only replica table and keeps it
// current. The items, the consistency level and the commit interval never
// change in place; the connection and the credentials change while the
// replication is paused.
type ReplicationSpec struct {
	// Connection is how the replication reaches the source database.
	Connection Connection `json:"connection"`
	// Items are the replicated tables, in the order they are declared.
	Items []Item `json:"items,omitempty"`
	// ConsistencyLevel is `CONSISTENCY_LEVEL`, `row` or `global`; empty is
	// YDB's `row`.
	ConsistencyLevel string `json:"consistency_level,omitempty"`
	// CommitInterval is `COMMIT_INTERVAL`, an ISO 8601 duration a global
	// replication commits after; empty is YDB's ten seconds.
	CommitInterval string `json:"commit_interval,omitempty"`
}

// Clone returns an independent copy.
func (s ReplicationSpec) Clone() ReplicationSpec {
	out := s
	if s.Items != nil {
		out.Items = append([]Item(nil), s.Items...)
	}
	return out
}

// TransferSpec is a YDB transfer: what `CREATE TRANSFER <name> FROM <topic> TO
// <table> USING <lambda> WITH (...)` takes. It reads the topic's messages,
// turns each into rows through the lambda and writes them into the table.
// It holds no reference type, so a copy is independent.
type TransferSpec struct {
	// Connection is how the transfer reaches a topic of another database,
	// and zero for a topic of its own.
	Connection Connection `json:"connection,omitzero"`
	// Source is the topic's path: relative to this database's root, or for
	// a topic of another database relative to that one's root or absolute.
	Source string `json:"source"`
	// Target is the table's path in this database, relative to its root.
	Target string `json:"target"`
	// Lambda is the YQL lambda, written inline as `($msg) -> { ... }`.
	Lambda string `json:"lambda"`
	// Consumer is `CONSUMER`, the topic consumer the transfer reads through.
	// Empty lets YDB create one, which dropping the transfer removes.
	Consumer string `json:"consumer,omitempty"`
	// BatchSizeBytes is `BATCH_SIZE_BYTES`; zero is YDB's 8 MiB.
	BatchSizeBytes uint64 `json:"batch_size_bytes,omitempty"`
	// FlushInterval is `FLUSH_INTERVAL`, an ISO 8601 duration of whole
	// seconds; empty is YDB's minute.
	FlushInterval string `json:"flush_interval,omitempty"`
}

// The states a replication or a transfer reports, as its observation carries
// them.
const (
	// StateRunning is a replication or transfer that copies, YDB's StandBy.
	StateRunning = "running"
	// StatePaused is one paused with `SET (STATE = 'PAUSED')`, the only state
	// in which YDB changes its connection and credentials.
	StatePaused = "paused"
	// StateDone is a replication failed over with `SET (STATE = 'DONE')`. It
	// copies nothing more, and its replica tables are ordinary writable
	// tables.
	StateDone = "done"
	// StateError is one that stopped on an error, such as a secret it cannot
	// read.
	StateError = "error"
)
