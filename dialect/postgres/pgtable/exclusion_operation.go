package pgtable

import (
	"encoding/json"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// ExclusionOperationKind identifies an operation on an exclusion constraint.
const ExclusionOperationKind schemaext.Kind = "ptah.run/postgres/exclusion-operation"

// AlterExclusion changes one constraint on the containing ALTER TABLE. Name
// is the constraint's SQL identifier; Change retains both operands. Replacing
// a definition requires a transaction around the whole rendered operation.
type AlterExclusion struct {
	Name   string          `json:"name,omitempty"`
	Change ExclusionChange `json:"change"`
}

// Kind returns the operation identity.
func (*AlterExclusion) Kind() schemaext.Kind { return ExclusionOperationKind }

// CloneExtension returns independent operands. A nil receiver stays typed nil.
func (op *AlterExclusion) CloneExtension() ast.ExtensionPayload { return cloneExclusionOperation(op) }

func cloneExclusionOperation(op *AlterExclusion) *AlterExclusion {
	if op == nil {
		return nil
	}
	return &AlterExclusion{Name: op.Name, Change: *op.Change.Copy()}
}

// Effect describes the constraint change. A nil receiver has no assessment.
func (op *AlterExclusion) Effect() schemaext.Effect {
	if op == nil {
		return schemaext.Effect{}
	}
	return op.Change.Effect()
}

// SchemaChange names the logical constraint change for migration reports.
// A nil operation returns no action.
func (op *AlterExclusion) SchemaChange() ast.ExtensionChange {
	if op == nil {
		return ast.ExtensionChange{}
	}
	action := ast.ExtensionModify
	if op.Change.Before == nil {
		action = ast.ExtensionAdd
	} else if op.Change.After == nil {
		action = ast.ExtensionDrop
	}
	return ast.ExtensionChange{Action: action, Name: op.Name}
}

// ValidateAlterExclusion validates a captured change. A source addition may
// leave its name to the server; drops, replacements and comments require one.
// Errors wrap schemaext.ErrInvalidValue.
func ValidateAlterExclusion(op *AlterExclusion) error {
	if op == nil || strings.ContainsRune(op.Name, '\x00') {
		return exclusionModelError(schemaext.Operation, "EXCLUDE operation requires a valid declaration")
	}
	if err := ValidateExclusionChange(&op.Change); err != nil {
		return err
	}
	if strings.TrimSpace(op.Name) == "" && (op.Name != "" || op.Change.Before != nil || op.Change.CommentOnly || op.Change.After == nil || op.Change.After.Comment != "") {
		return exclusionModelError(schemaext.Operation, "EXCLUDE operation requires a constraint name")
	}
	return nil
}

// ExclusionOperationCodec returns a strict version-one operation codec. Nested
// changes use their registered representation and canonical dependency order.
func ExclusionOperationCodec() schemaext.Codec {
	return schemaext.ModelCodec[*AlterExclusion]{Prototype: &AlterExclusion{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["change"],"additionalProperties":false,"properties":{"name":{"type":"string","minLength":1},"change":` + string(ExclusionChangeCodec().Definition) + `}}`),
		Shape:      exclusionOperationShape, Validate: ValidateAlterExclusion, Clone: cloneExclusionOperation, Canonical: canonicalExclusionOperation,
	}.Codec()
}

func exclusionOperationShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "EXCLUDE operation", Allowed: []string{"name", "change"}, Required: []string{"change"}, NonEmpty: []string{"name"}})
	if err != nil {
		return err
	}
	_, err = ExclusionChangeCodec().Decode(fields["change"])
	return err
}

func canonicalExclusionOperation(op *AlterExclusion) *AlterExclusion {
	return &AlterExclusion{Name: op.Name, Change: *canonicalExclusionChange(&op.Change)}
}
