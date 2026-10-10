package ydbast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationKind identifies one statement on a YDB async replication.
const AsyncReplicationKind schemaext.Kind = "ptah.run/ydb/async-replication-operation"

// TransferKind identifies one statement on a YDB transfer.
const TransferKind schemaext.Kind = "ptah.run/ydb/transfer-operation"

// AsyncReplication creates, changes or drops one async replication. A missing
// before operand requests CREATE ASYNC REPLICATION, a missing after one DROP
// ASYNC REPLICATION, with CASCADE unless the replication was failed over, and
// both ALTER ASYNC REPLICATION, written from the difference between them.
// Directory and leaf names stay separate, so a dot is part of a name.
type AsyncReplication struct {
	Schema string                   `json:"schema"`
	Name   string                   `json:"name"`
	Change ydbdiff.AsyncReplication `json:"change"`
}

// Transfer creates, changes or drops one transfer, as [AsyncReplication]
// does a replication.
type Transfer struct {
	Schema string           `json:"schema"`
	Name   string           `json:"name"`
	Change ydbdiff.Transfer `json:"change"`
}

// Kind returns the stable operation identity.
func (*AsyncReplication) Kind() schemaext.Kind { return AsyncReplicationKind }

// Kind returns the stable operation identity.
func (*Transfer) Kind() schemaext.Kind { return TransferKind }

// CloneExtension returns an independent operation and both captured operands.
func (v *AsyncReplication) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*AsyncReplication)(nil)
	}
	change, _ := v.Change.CloneChange().(*ydbdiff.AsyncReplication)
	return &AsyncReplication{Schema: v.Schema, Name: v.Name, Change: *change}
}

// CloneExtension returns an independent operation and both captured operands.
func (v *Transfer) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*Transfer)(nil)
	}
	change, _ := v.Change.CloneChange().(*ydbdiff.Transfer)
	return &Transfer{Schema: v.Schema, Name: v.Name, Change: *change}
}

// Subject returns the schema-scoped identity of the replication.
func (v *AsyncReplication) Subject() objectidentity.ID {
	return ydbreplication.ReplicationRef(v.Schema, v.Name)
}

// Subject returns the schema-scoped identity of the transfer.
func (v *Transfer) Subject() objectidentity.ID {
	return ydbreplication.TransferRef(v.Schema, v.Name)
}

// Reference is the replication's canonical reference, the name the
// statements and the refusals of [ydbreplication] take.
func (v *AsyncReplication) Reference() string {
	return ydbreplication.Reference(v.Schema, v.Name)
}

// Reference is the transfer's canonical reference.
func (v *Transfer) Reference() string {
	return ydbreplication.Reference(v.Schema, v.Name)
}

// Validate refuses an invalid path and invalid or empty operands.
func (v *AsyncReplication) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: async replication operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbreplication.ValidateIdentity(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// Validate refuses an invalid path and invalid or empty operands.
func (v *Transfer) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: transfer operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbreplication.ValidateIdentity(v.Subject()); err != nil {
		return err
	}
	return v.Change.Validate()
}

// Effect records what the statement does to the replica tables.
func (v *AsyncReplication) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// Effect records what the statement does to the transfer's position in its
// topic and the rows it writes.
func (v *Transfer) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}

// AsyncReplicationCodec records the explicit replication operation wire: the
// path and both operands of the change.
func AsyncReplicationCodec() schemaext.Codec {
	return pathChangeCodec("async replication", &AsyncReplication{}, ydbdiff.AsyncReplicationCodec(),
		func(value *AsyncReplication) (string, string, *ydbdiff.AsyncReplication) {
			return value.Schema, value.Name, &value.Change
		},
		func(schema, name string, change ydbdiff.AsyncReplication) *AsyncReplication {
			return &AsyncReplication{Schema: schema, Name: name, Change: change}
		})
}

// TransferCodec records the explicit transfer operation wire: the path and
// both operands of the change.
func TransferCodec() schemaext.Codec {
	return pathChangeCodec("transfer", &Transfer{}, ydbdiff.TransferCodec(),
		func(value *Transfer) (string, string, *ydbdiff.Transfer) {
			return value.Schema, value.Name, &value.Change
		},
		func(schema, name string, change ydbdiff.Transfer) *Transfer {
			return &Transfer{Schema: schema, Name: name, Change: change}
		})
}
