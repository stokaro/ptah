package ydbreplication

import (
	"ptah.run/catalog"
	"ptah.run/core/ast"
)

// The specifications of a replication and a transfer, under the names this
// package keeps for them.
//
// Each is an alias of the core/ast type for now, because the common
// replication path still builds them there. Step 2 of the replication slice
// of #4140, the flip that moves every source onto this owner, moves each
// definition here and deletes the core/ast type; nothing else changes for a
// caller that names them through this package.
type (
	// Connection is how a replication or a transfer reaches the database it
	// reads: the connection string and the credential, each credential named
	// by the secret that holds it and never by its value. A field left at
	// its zero value declares nothing, and at most one credential form is
	// set.
	Connection = ast.ReplicationConnectionSpec
	// Item is one `FOR <source> AS <target>` pair of a replication.
	Item = ast.AsyncReplicationItem
	// ReplicationSpec is an async replication: its connection, items and
	// consistency settings.
	ReplicationSpec = ast.AsyncReplicationSpec
	// TransferSpec is a transfer: its connection, the topic it reads, the
	// table it writes, the lambda and the batch settings.
	TransferSpec = ast.TransferSpec
)

// The states a replication or a transfer reports. They are the catalog's
// values until step 2 of the replication slice of #4140 moves them here and
// deletes the catalog's.
const (
	// StateRunning is a replication or transfer that copies, YDB's StandBy.
	StateRunning = catalog.ReplicationRunning
	// StatePaused is one paused with `SET (STATE = 'PAUSED')`, the only state
	// in which YDB changes its connection and credentials.
	StatePaused = catalog.ReplicationPaused
	// StateDone is a replication failed over with `SET (STATE = 'DONE')`. It
	// copies nothing more, and its replica tables are ordinary writable
	// tables.
	StateDone = catalog.ReplicationDone
	// StateError is one that stopped on an error, such as a secret it cannot
	// read.
	StateError = catalog.ReplicationError
)
