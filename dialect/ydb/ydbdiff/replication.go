package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
)

// AsyncReplicationKind identifies a change to one YDB async replication.
const AsyncReplicationKind schemaext.Kind = "ptah.run/ydb/async-replication-change"

// TransferKind identifies a change to one YDB transfer.
const TransferKind schemaext.Kind = "ptah.run/ydb/transfer-change"

// The reasons a replication or a transfer change reports, in the words the
// safety report gives for the statements that make it. Measured on 25.1.4.7
// and 26.2.1.14: a replication dropped with CASCADE takes its replica tables
// with it, and one dropped without CASCADE leaves them read-only for good
// unless it was failed over first.
const (
	// DropReplicationReason is why dropping a replication with CASCADE is
	// destructive.
	DropReplicationReason = "DROP ASYNC REPLICATION ... CASCADE drops the replica tables with the replication"
	// KeepReplicaTablesReason is why dropping one without CASCADE needs
	// review.
	KeepReplicaTablesReason = "DROP ASYNC REPLICATION without CASCADE ends the replication and keeps its tables, " +
		"read-only for good unless it was failed over first"
	// AlterReplicationReason is why changing a replication in place needs
	// review.
	AlterReplicationReason = "ALTER ASYNC REPLICATION points the replication at another source or credential"
	// DropTransferReason is why dropping a transfer is destructive.
	DropTransferReason = "DROP TRANSFER stops the transfer and drops the topic consumer YDB created for it, with " +
		"its position in the topic"
	// AlterTransferReason is why changing a transfer's lambda or batch
	// settings needs review.
	AlterTransferReason = "ALTER TRANSFER changes the rows the transfer writes from each message"
	// ReconnectTransferReason is why changing a transfer's connection needs
	// review.
	ReconnectTransferReason = "ALTER TRANSFER points the transfer at another source or credential"
)

// AsyncReplication captures both operands of a change to one async
// replication. A nil Before means the database holds no replication at the
// path; a nil After means the declaration drops it. A nil side is established
// absence, never an unread replication. Before carries the state the
// replication reported, which decides what YDB can change and whether a drop
// takes the replica tables along.
type AsyncReplication struct {
	Before *ydbreplication.ObservedReplication `json:"before"`
	After  *ydbreplication.DesiredReplication  `json:"after"`
}

// Transfer captures both operands of a change to one transfer, as
// [AsyncReplication] does.
type Transfer struct {
	Before *ydbreplication.ObservedTransfer `json:"before"`
	After  *ydbreplication.DesiredTransfer  `json:"after"`
}

// NewAsyncReplication is the change from before to after, either of which
// may be nil for the replication's absence.
func NewAsyncReplication(before *ydbreplication.ObservedReplication, after *ydbreplication.DesiredReplication) *AsyncReplication {
	return &AsyncReplication{Before: before, After: after}
}

// NewTransfer is the change from before to after, either of which may be nil
// for the transfer's absence.
func NewTransfer(before *ydbreplication.ObservedTransfer, after *ydbreplication.DesiredTransfer) *Transfer {
	return &Transfer{Before: before, After: after}
}

// Kind returns the stable change identity.
func (*AsyncReplication) Kind() schemaext.Kind { return AsyncReplicationKind }

// Kind returns the stable change identity.
func (*Transfer) Kind() schemaext.Kind { return TransferKind }

// CloneChange returns independent before and after snapshots.
func (v *AsyncReplication) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*AsyncReplication)(nil)
	}
	cloned := &AsyncReplication{}
	if v.Before != nil {
		cloned.Before, _ = v.Before.Clone().(*ydbreplication.ObservedReplication)
	}
	if v.After != nil {
		cloned.After, _ = v.After.Clone().(*ydbreplication.DesiredReplication)
	}
	return cloned
}

// CloneChange returns independent before and after snapshots.
func (v *Transfer) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Transfer)(nil)
	}
	cloned := &Transfer{}
	if v.Before != nil {
		cloned.Before, _ = v.Before.Clone().(*ydbreplication.ObservedTransfer)
	}
	if v.After != nil {
		cloned.After, _ = v.After.Clone().(*ydbreplication.DesiredTransfer)
	}
	return cloned
}

// Validate refuses a change without operands, an operand its codec refuses,
// and two operands that describe the same replication.
func (v *AsyncReplication) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: an async replication change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := v.Before.Validate(); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && ydbreplication.ReplicationsEqual(v.After.Spec, v.Before.Spec) {
		return fmt.Errorf("%w: async replication operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// Validate refuses a change without operands, an operand its codec refuses,
// and two operands that describe the same transfer.
func (v *Transfer) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a transfer change requires a before or after operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := v.Before.Validate(); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := v.After.Validate(); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && ydbreplication.TransfersEqual(v.After.Spec, v.Before.Spec) {
		return fmt.Errorf("%w: transfer operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// Operands returns the spec of each operand, nil for an absent one, and the
// state the before operand reported: what [ydbreplication.RefuseReplication]
// holds the change to. The specs are the change's own, not copies.
func (v *AsyncReplication) Operands() (before, after *ydbreplication.ReplicationSpec, state string) {
	if v.Before != nil {
		before, state = &v.Before.Spec, v.Before.State
	}
	if v.After != nil {
		after = &v.After.Spec
	}
	return before, after, state
}

// Operands returns the spec of each operand, nil for an absent one, and the
// state the before operand reported: what [ydbreplication.RefuseTransfer]
// holds the change to. The specs are the change's own, not copies.
func (v *Transfer) Operands() (before, after *ydbreplication.TransferSpec, state string) {
	if v.Before != nil {
		before, state = &v.Before.Spec, v.Before.State
	}
	if v.After != nil {
		after = &v.After.Spec
	}
	return before, after, state
}

// Cascade reports whether dropping the replication drops its replica tables
// too: every replication but one failed over, whose tables are ordinary and
// stay.
func (v *AsyncReplication) Cascade() bool {
	return v != nil && v.Before != nil && v.Before.State != ydbreplication.StateDone
}

// Effect records what the change does to the replica tables and to the data
// path. Creating a replication adds it and its replica tables; dropping one
// with CASCADE drops the replica tables, and dropping a failed-over one
// without CASCADE keeps them; a change in place points it at another source or
// credential.
func (v *AsyncReplication) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE ASYNC REPLICATION adds a replication and its replica tables"}
	case v.After == nil && v.Cascade():
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: DropReplicationReason}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: KeepReplicaTablesReason}
	default:
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: AlterReplicationReason}
	}
}

// Effect records what the change does to the transfer's position in its topic
// and to the rows it writes. Dropping a transfer drops the consumer YDB created
// for it; a change of its lambda or batch settings changes the rows it writes;
// a change of its connection points it at another source.
func (v *Transfer) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE TRANSFER adds a transfer"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: DropTransferReason}
	}
	changes := ydbreplication.CompareTransfer(v.After.Spec, v.Before.Spec)
	if changes.Lambda || changes.Batch {
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: AlterTransferReason}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ReconnectTransferReason}
}

// AsyncReplicationCodec describes the complete before/after wire for one
// replication change.
func AsyncReplicationCodec() schemaext.Codec {
	return replicationChangeCodec(&AsyncReplication{}, ydbreplication.ReplicationCodecs(),
		func(data json.RawMessage, models []schemaext.Codec) (*AsyncReplication, error) {
			before, after, err := decodeStandaloneOperands[*ydbreplication.ObservedReplication, *ydbreplication.DesiredReplication](
				data, "async replication", models)
			return &AsyncReplication{Before: before, After: after}, err
		})
}

// TransferCodec describes the complete before/after wire for one transfer
// change.
func TransferCodec() schemaext.Codec {
	return replicationChangeCodec(&Transfer{}, ydbreplication.TransferCodecs(),
		func(data json.RawMessage, models []schemaext.Codec) (*Transfer, error) {
			before, after, err := decodeStandaloneOperands[*ydbreplication.ObservedTransfer, *ydbreplication.DesiredTransfer](
				data, "transfer", models)
			return &Transfer{Before: before, After: after}, err
		})
}

// replicationChange is a change type of this file.
type replicationChange interface {
	schemaext.ChangeValue
	Validate() error
}

// replicationChangeCodec builds the codec of one change type, whose operands
// the models encode.
func replicationChangeCodec[T replicationChange](prototype T, models []schemaext.Codec,
	decode func(json.RawMessage, []schemaext.Codec) (T, error),
) schemaext.Codec {
	definitions := make(map[schemaext.Representation]json.RawMessage)
	for _, codec := range models {
		definitions[codec.Representation] = codec.Definition
	}
	return schemaext.ModelCodec[T]{Prototype: prototype, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["before","after"],"additionalProperties":false,`+
			`"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`,
			definitions[schemaext.Observed], definitions[schemaext.Desired])),
		Shape: func(data json.RawMessage) error {
			_, err := decode(data, models)
			return err
		},
		Validate: func(value T) error { return value.Validate() },
		Clone: func(value T) T {
			cloned, _ := value.CloneChange().(T)
			return cloned
		},
	}.Codec()
}
