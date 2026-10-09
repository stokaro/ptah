package ydbast

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
)

// CoordinationNodeKind identifies a standalone configuration transition.
const CoordinationNodeKind schemaext.Kind = "ptah.run/ydb/coordination-node-operation"

// CoordinationNode creates, changes, or drops one standalone node. A missing
// before operand requests CREATE; a missing after requests DROP. Migration
// operands use independently established absence, while source creation expresses
// authored intent. Directory and leaf names remain separate, including dots.
type CoordinationNode struct {
	Schema string                   `json:"schema"`
	Name   string                   `json:"name"`
	Change ydbdiff.CoordinationNode `json:"change"`
}

// Kind returns the stable operation identity.
func (*CoordinationNode) Kind() schemaext.Kind { return CoordinationNodeKind }

// CloneExtension returns an independent operation and both captured operands.
func (v *CoordinationNode) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*CoordinationNode)(nil)
	}
	cloned := &CoordinationNode{Schema: v.Schema, Name: v.Name}
	if v.Change.Before != nil {
		cloned.Change.Before = new(*v.Change.Before)
	}
	if v.Change.After != nil {
		cloned.Change.After = new(*v.Change.After)
	}
	return cloned
}

// Subject returns the schema-scoped identity without a table parent.
func (v *CoordinationNode) Subject() objectidentity.ID {
	return ydbcoordination.Ref(v.Schema, v.Name)
}

// Validate refuses invalid paths, incomplete operands, and empty transitions.
func (v *CoordinationNode) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: coordination operation is nil", schemaext.ErrInvalidValue)
	}
	if err := ydbcoordination.ValidateRef(v.Subject()); err != nil {
		return err
	}
	if err := v.Change.Validate(); err != nil {
		return err
	}
	if v.Change.Before != nil && v.Change.After != nil && ydbcoordination.Changes(v.Change.After.Spec, v.Change.Before.Spec).IsZero() {
		return fmt.Errorf("%w: coordination operands contain no change", schemaext.ErrInvalidValue)
	}
	return nil
}

// Effect records runtime loss separately from restoration of configuration.
func (v *CoordinationNode) Effect() schemaext.Effect {
	if v == nil {
		return schemaext.Effect{}
	}
	return v.Change.Effect()
}
