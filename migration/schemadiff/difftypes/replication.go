package difftypes

import (
	"encoding/json"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbreplication"
)

// AsyncReplicationChanges is a set of YDB async replications one change
// applies to, carrying each one's connection and items and not only its name:
// a created replication is written from them, and so is the replication a
// rollback creates again.
type AsyncReplicationChanges []schemamodel.AsyncReplication

// MarshalJSON writes the replication names alone, as the other object lists
// of a diff write theirs.
func (r AsyncReplicationChanges) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.Names())
}

// Names is the canonical references of the replications this change applies
// to.
func (r AsyncReplicationChanges) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r))
	for _, replication := range r {
		names = append(names, replication.QualifiedName())
	}
	return names
}

// TransferChanges is a set of YDB transfers one change applies to, carrying
// each one's source, target and lambda.
type TransferChanges []schemamodel.Transfer

// MarshalJSON writes the transfer names alone.
func (t TransferChanges) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	return json.Marshal(t.Names())
}

// Names is the canonical references of the transfers this change applies to.
func (t TransferChanges) Names() []string {
	if t == nil {
		return nil
	}
	names := make([]string, 0, len(t))
	for _, transfer := range t {
		names = append(names, transfer.QualifiedName())
	}
	return names
}

// AsyncReplicationDiff describes a YDB async replication whose declaration
// differs from what the database holds.
//
// The flags and the list name what differs; the two specs are the operands
// and State is the replication's state, all three off the wire, since a plan
// reads them to decide whether YDB can make the change at all.
type AsyncReplicationDiff struct {
	// Name is the replication's canonical reference.
	Name string `json:"name"`
	// ConnectionChanged reports a connection string that differs.
	ConnectionChanged bool `json:"connection_changed,omitempty"`
	// CredentialsChanged reports a credential that differs.
	CredentialsChanged bool `json:"credentials_changed,omitempty"`
	// CreateOnlyChanged names the settings that differ and that YDB changes in
	// no replication in place.
	CreateOnlyChanged []string `json:"create_only_changed,omitempty"`
	// Desired is the replication as the target schema declares it.
	Desired ast.AsyncReplicationSpec `json:"-"`
	// Current is the replication as the database holds it.
	Current ast.AsyncReplicationSpec `json:"-"`
	// State is the state the database reports for it.
	State string `json:"-"`
}

// NewAsyncReplicationDiff reads how the replication name differs between
// desired and current, and reports whether it differs at all.
func NewAsyncReplicationDiff(name string, desired, current ast.AsyncReplicationSpec, state string) (AsyncReplicationDiff, bool) {
	changes := ydbreplication.CompareReplication(desired, current)
	return AsyncReplicationDiff{
		Name:               name,
		ConnectionChanged:  changes.ConnectionString,
		CredentialsChanged: changes.Credentials,
		CreateOnlyChanged:  changes.CreateOnly,
		Desired:            desired.Clone(),
		Current:            current.Clone(),
		State:              state,
	}, changes.Any()
}

// TransferDiff describes a YDB transfer whose declaration differs from what
// the database holds.
type TransferDiff struct {
	// Name is the transfer's canonical reference.
	Name string `json:"name"`
	// LambdaChanged reports a lambda that differs.
	LambdaChanged bool `json:"lambda_changed,omitempty"`
	// BatchChanged reports a batch size or a flush interval that differs.
	BatchChanged bool `json:"batch_changed,omitempty"`
	// ConnectionChanged reports a connection string that differs.
	ConnectionChanged bool `json:"connection_changed,omitempty"`
	// CredentialsChanged reports a credential that differs.
	CredentialsChanged bool `json:"credentials_changed,omitempty"`
	// CreateOnlyChanged names the settings that differ and that YDB changes in
	// no transfer in place.
	CreateOnlyChanged []string `json:"create_only_changed,omitempty"`
	// Desired is the transfer as the target schema declares it.
	Desired ast.TransferSpec `json:"-"`
	// Current is the transfer as the database holds it.
	Current ast.TransferSpec `json:"-"`
	// State is the state the database reports for it.
	State string `json:"-"`
}

// NewTransferDiff reads how the transfer name differs between desired and
// current, and reports whether it differs at all.
func NewTransferDiff(name string, desired, current ast.TransferSpec, state string) (TransferDiff, bool) {
	changes := ydbreplication.CompareTransfer(desired, current)
	return TransferDiff{
		Name:               name,
		LambdaChanged:      changes.Lambda,
		BatchChanged:       changes.Batch,
		ConnectionChanged:  changes.ConnectionString,
		CredentialsChanged: changes.Credentials,
		CreateOnlyChanged:  changes.CreateOnly,
		Desired:            desired,
		Current:            current,
		State:              state,
	}, changes.Any()
}

// ReplicationContext is every YDB async replication and transfer on each side
// of a comparison, changed or not. A plan reads it to keep what the objects
// depend on: the replica tables a replication owns, the table and the topic a
// transfer reads and writes, and the state that decides what YDB can change.
type ReplicationContext struct {
	// DesiredObjects retains named dependencies, including unchanged streams.
	DesiredObjects schemaext.Objects
	// CurrentCoverage retains explicit records of unreadable dependencies.
	CurrentCoverage schemaext.Coverage
	// CurrentReplications are the replications the database holds.
	CurrentReplications []catalog.AsyncReplication
	// CurrentTransfers are the transfers the database holds.
	CurrentTransfers []catalog.Transfer
	// DeclaredReplications are the replications the target schema declares.
	DeclaredReplications []schemamodel.AsyncReplication
	// DeclaredTransfers are the transfers the target schema declares.
	DeclaredTransfers []schemamodel.Transfer
	// CurrentTopics are the paths of the topics the database holds, relative
	// to the database root, as a transfer of a topic in its own database
	// names one.
	CurrentTopics []string
	// DeclaredTopics are the paths of the topics the target schema declares.
	DeclaredTopics []string
}

// CurrentReplication finds a replication the database holds by name.
func (c ReplicationContext) CurrentReplication(name string) (catalog.AsyncReplication, bool) {
	for _, replication := range c.CurrentReplications {
		if replication.QualifiedName() == name {
			return replication, true
		}
	}
	return catalog.AsyncReplication{}, false
}

// Declares reports whether the target schema declares the replication name.
func (c ReplicationContext) Declares(name string) bool {
	for _, replication := range c.DeclaredReplications {
		if replication.QualifiedName() == name {
			return true
		}
	}
	return false
}
