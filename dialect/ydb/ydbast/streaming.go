package ydbast

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/internal/tableref"
)

// StreamingQueryKind identifies an operation on a continuous YDB query.
const StreamingQueryKind schemaext.Kind = "ptah.run/ydb/streaming-query-operation"

// StreamingOperation selects creation, alteration, or removal.
type StreamingOperation string

const (
	// StreamingCreate creates a query, with explicit existence and replacement guards.
	StreamingCreate StreamingOperation = "create"
	// StreamingAlter changes execution settings or the query body.
	StreamingAlter StreamingOperation = "alter"
	// StreamingDrop removes the query and its checkpoints.
	StreamingDrop StreamingOperation = "drop"
)

// StreamingCreation records creation guards. IfNotExists preserves an existing
// query even when OrReplace is true; unguarded replacement can reset its state.
type StreamingCreation struct {
	OrReplace   bool `json:"or_replace"`
	IfNotExists bool `json:"if_not_exists"`
}

// StreamingQuery carries a standalone operation and its captured operands.
// Schema is a database-relative directory and Name is a leaf name. Alterations
// require Previous; AllowStateReset is explicit permission, never observed state.
// A zero value is invalid. Creation guards belong only to StreamingCreate.
type StreamingQuery struct {
	Operation       StreamingOperation `json:"operation"`
	Schema          string             `json:"schema"`
	Name            string             `json:"name"`
	Creation        StreamingCreation  `json:"creation"`
	Spec            ydbstreaming.Spec  `json:"spec"`
	Previous        ydbstreaming.Spec  `json:"previous"`
	AllowStateReset bool               `json:"allow_state_reset"`
}

// Kind returns the stable operation identity.
func (*StreamingQuery) Kind() schemaext.Kind { return StreamingQueryKind }

// CloneExtension returns independent copies of both operands and optional Run
// settings. A nil receiver returns a typed nil for the envelope to reject.
func (v *StreamingQuery) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*StreamingQuery)(nil)
	}
	cloned := *v
	cloned.Spec, cloned.Previous = v.Spec.Clone(), v.Previous.Clone()
	return &cloned
}

// QualifiedName formats the directory and leaf without losing literal dots.
func (v *StreamingQuery) QualifiedName() string { return tableref.CanonicalExact(v.Schema, v.Name) }

// Validate refuses missing operands, irrelevant permissions, invalid names, and
// body changes without explicit permission to reset aggregation state.
func (v *StreamingQuery) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: streaming operation is nil", schemaext.ErrInvalidValue)
	}
	for _, text := range []string{v.Schema, v.Name, v.Spec.Text, v.Spec.ResourcePool, v.Previous.Text, v.Previous.ResourcePool} {
		if !utf8.ValidString(text) {
			return fmt.Errorf("%w: streaming operation contains invalid UTF-8", schemaext.ErrInvalidValue)
		}
	}
	if strings.TrimSpace(v.Name) == "" || strings.ContainsAny(v.Name, "/\x00") || strings.ContainsRune(v.Schema, '\x00') {
		return fmt.Errorf("%w: streaming operation requires separate directory and leaf names", schemaext.ErrInvalidValue)
	}
	if v.Operation != StreamingCreate && v.Creation != (StreamingCreation{}) {
		return fmt.Errorf("%w: streaming creation guards require create", schemaext.ErrInvalidValue)
	}
	if v.Operation != StreamingAlter && (v.AllowStateReset || v.Previous != (ydbstreaming.Spec{})) {
		return fmt.Errorf("%w: streaming previous state and reset permission require alter", schemaext.ErrInvalidValue)
	}
	switch v.Operation {
	case StreamingCreate:
		return validateStreamingSpec(v.Spec)
	case StreamingAlter:
		if err := validateStreamingSpec(v.Spec); err != nil {
			return err
		}
		if err := validateStreamingSpec(v.Previous); err != nil {
			return err
		}
		if err := ydbstreaming.ValidateAlter(v.QualifiedName(), v.Spec, v.Previous, ydbstreaming.AlterOptions{AllowStateReset: v.AllowStateReset}); err != nil {
			return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
		}
		return nil
	case StreamingDrop:
		if v.Spec != (ydbstreaming.Spec{}) {
			return fmt.Errorf("%w: streaming drop cannot carry desired settings", schemaext.ErrInvalidValue)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown streaming operation %q", schemaext.ErrInvalidValue, v.Operation)
	}
}

func validateStreamingSpec(spec ydbstreaming.Spec) error {
	if err := ydbstreaming.Validate(spec); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// RunsBeforeChanges reports whether the statement belongs before the plan's
// other changes rather than after them: a drop, and a stop that changes
// nothing else, so the query is not running while the tables and topics it
// reads change. Every other statement runs after them, against the schema
// they leave.
func (v *StreamingQuery) RunsBeforeChanges() bool {
	if v == nil {
		return false
	}
	return v.Operation == StreamingDrop || (v.Operation == StreamingAlter &&
		ydbstreaming.Running(v.Previous) && !ydbstreaming.Running(v.Spec) &&
		ydbstreaming.SameBody(v.Previous.Text, v.Spec.Text) && ydbstreaming.Pool(v.Previous) == ydbstreaming.Pool(v.Spec))
}

// Effect preserves checkpoint-loss classification through generic envelopes.
// Invalid operations have unknown effects; reconstructing their declarations
// cannot restore discarded checkpoint state.
func (v *StreamingQuery) Effect() schemaext.Effect {
	if v.Validate() != nil {
		return schemaext.Effect{}
	}
	replaces := (ydbstreaming.CreateOptions{OrReplace: v.Creation.OrReplace, IfNotExists: v.Creation.IfNotExists}).ReplacesExisting()
	if v.Operation == StreamingDrop || replaces || (v.Operation == StreamingAlter && !ydbstreaming.SameBody(v.Spec.Text, v.Previous.Text)) {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbstreaming.CheckpointLoss}
	}
	if v.Operation == StreamingAlter {
		return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbstreaming.ExecutionChange}
	}
	return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a streaming query without replacing existing checkpoint state"}
}
