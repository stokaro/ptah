package ast

// ReplicationConnectionSpec is how a YDB async replication or transfer
// reaches the database it reads: the connection string and the credentials,
// each credential named by the secret that holds it and never by its value.
//
// A field left at its zero value declares nothing. At most one credential
// form is set: a token secret, by name or by path, or a user with a password
// secret, by name or by path. A name refers to an object secret
// (`CREATE OBJECT ... (TYPE SECRET)`), a path to a scheme secret (`CREATE
// SECRET`), relative to the database the replication runs in.
type ReplicationConnectionSpec struct {
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

// AsyncReplicationItem is one `FOR <source> AS <target>` pair of an async
// replication: a table, or a directory of tables, of the source database and
// the path YDB creates its replica at in this one.
type AsyncReplicationItem struct {
	// Source is the path in the source database, relative to its root or
	// absolute.
	Source string `json:"source"`
	// Target is the path in this database, relative to its root.
	Target string `json:"target"`
}

// AsyncReplicationSpec is a YDB async replication: what `CREATE ASYNC
// REPLICATION <name> FOR <source> AS <target>, ... WITH (...)` takes.
//
// YDB creates each target itself as a read-only replica table and keeps it
// current. The items, the consistency level and the commit interval never
// change in place; the connection and the credentials change while the
// replication is paused. [ptah.run/internal/ydbreplication] owns the rules.
type AsyncReplicationSpec struct {
	// Connection is how the replication reaches the source database.
	Connection ReplicationConnectionSpec `json:"connection"`
	// Items are the replicated tables, in the order they are declared.
	Items []AsyncReplicationItem `json:"items,omitempty"`
	// ConsistencyLevel is `CONSISTENCY_LEVEL`, `row` or `global`; empty is
	// YDB's `row`.
	ConsistencyLevel string `json:"consistency_level,omitempty"`
	// CommitInterval is `COMMIT_INTERVAL`, an ISO 8601 duration a global
	// replication commits after; empty is YDB's ten seconds.
	CommitInterval string `json:"commit_interval,omitempty"`
}

// Clone returns an independent copy.
func (s AsyncReplicationSpec) Clone() AsyncReplicationSpec {
	out := s
	if s.Items != nil {
		out.Items = append([]AsyncReplicationItem(nil), s.Items...)
	}
	return out
}

// TransferSpec is a YDB transfer: what `CREATE TRANSFER <name> FROM <topic> TO
// <table> USING <lambda> WITH (...)` takes. It reads the topic's messages,
// turns each into rows through the lambda and writes them into the table.
type TransferSpec struct {
	// Connection is how the transfer reaches a topic of another database,
	// and zero for a topic of its own.
	Connection ReplicationConnectionSpec `json:"connection,omitzero"`
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

// CreateAsyncReplicationNode creates a YDB async replication. The YDB renderer
// writes it; every other renderer refuses it.
type CreateAsyncReplicationNode struct {
	// Name is the replication's canonical reference: the directory and the
	// name, as [ptah.run/internal/tableref.Canonical] joins a table's.
	Name string
	// Spec is the replication.
	Spec AsyncReplicationSpec
}

// NewCreateAsyncReplication creates a CREATE ASYNC REPLICATION node.
func NewCreateAsyncReplication(name string, spec AsyncReplicationSpec) *CreateAsyncReplicationNode {
	return &CreateAsyncReplicationNode{Name: name, Spec: spec.Clone()}
}

// Accept implements the Node interface for CreateAsyncReplicationNode.
func (n *CreateAsyncReplicationNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterAsyncReplicationNode changes the connection or the credentials of a
// paused YDB async replication through `ALTER ASYNC REPLICATION ... SET`. It
// carries the replication before the change, because the statement names
// only the settings that differ. The YDB renderer writes it; every other
// renderer refuses it.
type AlterAsyncReplicationNode struct {
	// Name is the replication's canonical reference.
	Name string
	// Spec is the replication as it is to be.
	Spec AsyncReplicationSpec
	// Previous is the replication as the database holds it.
	Previous AsyncReplicationSpec
}

// NewAlterAsyncReplication creates an ALTER ASYNC REPLICATION node.
func NewAlterAsyncReplication(name string, spec, previous AsyncReplicationSpec) *AlterAsyncReplicationNode {
	return &AlterAsyncReplicationNode{Name: name, Spec: spec.Clone(), Previous: previous.Clone()}
}

// Accept implements the Node interface for AlterAsyncReplicationNode.
func (n *AlterAsyncReplicationNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropAsyncReplicationNode drops a YDB async replication. With Cascade the
// replica tables it created go with it; without it they stay, writable only
// when the replication was failed over first. The YDB renderer writes it;
// every other renderer refuses it.
type DropAsyncReplicationNode struct {
	// Name is the replication's canonical reference.
	Name string
	// Cascade drops the replica tables too.
	Cascade bool
}

// NewDropAsyncReplication creates a DROP ASYNC REPLICATION node.
func NewDropAsyncReplication(name string, cascade bool) *DropAsyncReplicationNode {
	return &DropAsyncReplicationNode{Name: name, Cascade: cascade}
}

// Accept implements the Node interface for DropAsyncReplicationNode.
func (n *DropAsyncReplicationNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// CreateTransferNode creates a YDB transfer. The YDB renderer writes it;
// every other renderer refuses it.
type CreateTransferNode struct {
	// Name is the transfer's canonical reference.
	Name string
	// Spec is the transfer.
	Spec TransferSpec
}

// NewCreateTransfer creates a CREATE TRANSFER node.
func NewCreateTransfer(name string, spec TransferSpec) *CreateTransferNode {
	return &CreateTransferNode{Name: name, Spec: spec}
}

// Accept implements the Node interface for CreateTransferNode.
func (n *CreateTransferNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// AlterTransferNode changes a YDB transfer in place through `ALTER TRANSFER
// ... SET`: its lambda, its batch settings, and while it is paused its
// connection and credentials. It carries the transfer before the change,
// because the statement names only what differs. The YDB renderer writes it;
// every other renderer refuses it.
type AlterTransferNode struct {
	// Name is the transfer's canonical reference.
	Name string
	// Spec is the transfer as it is to be.
	Spec TransferSpec
	// Previous is the transfer as the database holds it.
	Previous TransferSpec
}

// NewAlterTransfer creates an ALTER TRANSFER node.
func NewAlterTransfer(name string, spec, previous TransferSpec) *AlterTransferNode {
	return &AlterTransferNode{Name: name, Spec: spec, Previous: previous}
}

// Accept implements the Node interface for AlterTransferNode.
func (n *AlterTransferNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }

// DropTransferNode drops a YDB transfer, and the topic consumer YDB created
// for it. The destination table and the topic stay. The YDB renderer writes
// it; every other renderer refuses it.
type DropTransferNode struct {
	// Name is the transfer's canonical reference.
	Name string
}

// NewDropTransfer creates a DROP TRANSFER node.
func NewDropTransfer(name string) *DropTransferNode {
	return &DropTransferNode{Name: name}
}

// Accept implements the Node interface for DropTransferNode.
func (n *DropTransferNode) Accept(visitor Visitor) error { return visitor.VisitNode(n) }
