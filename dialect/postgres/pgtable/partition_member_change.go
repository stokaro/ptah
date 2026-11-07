package pgtable

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// PartitionMemberChangeKind identifies a change of a table's place in a
// PostgreSQL partition tree.
const PartitionMemberChangeKind schemaext.Kind = "ptah.run/postgres/table-partition-member-change"

// PartitionMemberChange moves a table in a partition tree. Before is the parent
// and bound the table holds, and nil for an ordinary table; After is the parent
// and bound the declaration gives it, and nil for an ordinary table. At least one
// is present, and the two differ in parent or bound.
//
// The plan detaches the table from the parent it holds and attaches it to the
// declared one. PostgreSQL has no statement that changes a bound in place, so a
// bound change detaches and attaches under the same parent.
type PartitionMemberChange struct {
	Before *ObservedPartitionMember `json:"before"`
	After  *DesiredPartitionMember  `json:"after"`
}

// Kind returns the owned change identity.
func (*PartitionMemberChange) Kind() schemaext.Kind { return PartitionMemberChangeKind }

// Effect describes what the move costs a running workload.
func (*PartitionMemberChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "DETACH and ATTACH PARTITION lock the parent table, ATTACH reads every row of the partition, and of a default partition, to check the bound"}
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (c *PartitionMemberChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *PartitionMemberChange) Copy() *PartitionMemberChange {
	if c == nil {
		return nil
	}
	result := &PartitionMemberChange{}
	if c.Before != nil {
		result.Before = new(*c.Before)
	}
	if c.After != nil {
		after := *c.After
		if c.After.Normalized != nil {
			after.Normalized = new(*c.After.Normalized)
		}
		result.After = &after
	}
	return result
}

// ValidatePartitionMemberChange refuses nil, invalid operands and a pair that
// names the same parent and the same bound. Parents are compared as written; a
// caller that folds names compares before it builds a change. Every error wraps
// schemaext.ErrInvalidValue.
func ValidatePartitionMemberChange(c *PartitionMemberChange) error {
	if c == nil || c.Before == nil && c.After == nil {
		return fmt.Errorf("%w: partition member change requires an operand", schemaext.ErrInvalidValue)
	}
	if c.Before != nil {
		if err := ValidateObservedPartitionMember(c.Before); err != nil {
			return err
		}
	}
	if c.After != nil {
		if err := ValidateDesiredPartitionMember(c.After); err != nil {
			return err
		}
	}
	if c.Before != nil && c.After != nil && PartitionMember(*c.Before) == c.After.PartitionMember {
		return fmt.Errorf("%w: partition member change requires a different parent or bound", schemaext.ErrInvalidValue)
	}
	return nil
}

// Reverse returns the inverse change without mutating c: the table goes back to
// the parent and bound it held. Invalid operands are refused under
// ValidatePartitionMemberChange's contract.
func (c *PartitionMemberChange) Reverse() (*PartitionMemberChange, error) {
	if err := ValidatePartitionMemberChange(c); err != nil {
		return nil, err
	}
	result := &PartitionMemberChange{}
	if c.Before != nil {
		result.After = &DesiredPartitionMember{PartitionMember: PartitionMember(*c.Before)}
	}
	if c.After != nil {
		result.Before = new(ObservedPartitionMember(c.After.PartitionMember))
	}
	return result, nil
}

// PartitionMemberChangeCodec returns an independent version-one change codec.
// Both keys are required; an absent operand is null.
func PartitionMemberChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*PartitionMemberChange]{
		Prototype: &PartitionMemberChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(partitionMemberChangeDefinition),
		Shape:      partitionMemberChangeShape, Validate: ValidatePartitionMemberChange, Clone: (*PartitionMemberChange).Copy,
	}.Codec()
}

func partitionMemberChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "table partition member change",
		Allowed: []string{"before", "after"}, Required: []string{"before", "after"}, Nullable: []string{"before", "after"}})
	if err != nil {
		return err
	}
	for _, codec := range PartitionMemberCodecs() {
		key := "after"
		if codec.Representation == schemaext.Observed {
			key = "before"
		}
		if string(fields[key]) == "null" {
			continue
		}
		if _, err := codec.Decode(fields[key]); err != nil {
			return err
		}
	}
	return nil
}

var partitionMemberChangeDefinition = `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{` +
	`"before":{"anyOf":[{"type":"null"},` + partitionMemberDefinition + `]},` +
	`"after":{"anyOf":[{"type":"null"},` + desiredPartitionMemberDefinition + `]}}}`

// PartitionMoveOperationKind identifies one statement of a partition move.
const PartitionMoveOperationKind schemaext.Kind = "ptah.run/postgres/alter-table-partition-move"

// Partition move steps: a move detaches first and attaches second.
const (
	// PartitionDetach is `ALTER TABLE parent DETACH PARTITION table`, the
	// parent named by the change's Before.
	PartitionDetach = "detach"
	// PartitionAttach is `ALTER TABLE parent ATTACH PARTITION table bound`,
	// the parent and bound named by the change's After.
	PartitionAttach = "attach"
)

// MovePartition is one statement of a partition move, the detachment or the
// attachment, applied to the table the operation's parent names. Step selects
// which; the change carries both operands, so a renderer and a report read the
// whole move from either statement.
type MovePartition struct {
	Change PartitionMemberChange `json:"change"`
	Step   string                `json:"step"`
}

// Kind returns the operation identity.
func (*MovePartition) Kind() schemaext.Kind { return PartitionMoveOperationKind }

// CloneExtension returns an independent operation.
func (op *MovePartition) CloneExtension() ast.ExtensionPayload { return cloneMovePartition(op) }

func cloneMovePartition(op *MovePartition) *MovePartition {
	if op == nil {
		return (*MovePartition)(nil)
	}
	return &MovePartition{Change: *op.Change.Copy(), Step: op.Step}
}

// Effect is the change's effect.
func (op *MovePartition) Effect() schemaext.Effect { return op.Change.Effect() }

// ValidateMovePartition refuses a nil operation, an invalid change, an unknown
// step, and a step whose operand the change does not carry.
func ValidateMovePartition(op *MovePartition) error {
	if op == nil {
		return fmt.Errorf("%w: nil partition move operation", schemaext.ErrInvalidValue)
	}
	if err := ValidatePartitionMemberChange(&op.Change); err != nil {
		return err
	}
	switch {
	case op.Step == PartitionDetach && op.Change.Before != nil, op.Step == PartitionAttach && op.Change.After != nil:
		return nil
	case op.Step == PartitionDetach || op.Step == PartitionAttach:
		return fmt.Errorf("%w: partition move step %q has no operand to act on", schemaext.ErrInvalidValue, op.Step)
	default:
		return fmt.Errorf("%w: unknown partition move step %q", schemaext.ErrInvalidValue, op.Step)
	}
}

// MovePartitionCodec returns an independent version-one operation codec.
func MovePartitionCodec() schemaext.Codec {
	return schemaext.ModelCodec[*MovePartition]{Prototype: &MovePartition{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["change","step"],"additionalProperties":false,"properties":{"change":` +
			partitionMemberChangeDefinition + `,"step":{"enum":["detach","attach"]}}}`),
		Shape: movePartitionShape, Validate: ValidateMovePartition, Clone: cloneMovePartition}.Codec()
}

func movePartitionShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "partition move operation", Allowed: []string{"change", "step"}, Required: []string{"change", "step"}})
	if err != nil {
		return err
	}
	_, err = PartitionMemberChangeCodec().Decode(fields["change"])
	return err
}
