package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// CoordinationNodeKind identifies a change to one standalone YDB node.
const CoordinationNodeKind schemaext.Kind = "ptah.run/ydb/coordination-node-change"

// CoordinationNode captures complete operands. A nil side means established
// absence, never an unreadable node. Settings retain their raw stored form.
// Runtime semaphores and rate limiter resources are not schema configuration.
type CoordinationNode struct {
	Before *ydbcoordination.Observed `json:"before"`
	After  *ydbcoordination.Desired  `json:"after"`
}

// Kind returns the stable change identity.
func (*CoordinationNode) Kind() schemaext.Kind { return CoordinationNodeKind }

// CloneChange returns independent before and after configuration snapshots.
func (v *CoordinationNode) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*CoordinationNode)(nil)
	}
	cloned := &CoordinationNode{}
	if v.Before != nil {
		cloned.Before = &ydbcoordination.Observed{Spec: v.Before.Spec}
	}
	if v.After != nil {
		cloned.After = new(*v.After)
	}
	return cloned
}

// Validate refuses missing operands and settings YDB would silently clamp.
// Semantic equality is a planning decision, not a wire-format decision.
func (v *CoordinationNode) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: coordination change requires a before or after definition", schemaext.ErrInvalidValue)
	}
	for _, spec := range []*ydbcoordination.Spec{coordinationBefore(v), coordinationAfter(v)} {
		if spec != nil {
			if err := ydbcoordination.Validate(*spec); err != nil {
				return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
			}
		}
	}
	return nil
}

func coordinationBefore(v *CoordinationNode) *ydbcoordination.Spec {
	if v.Before != nil {
		return &v.Before.Spec
	}
	return nil
}

func coordinationAfter(v *CoordinationNode) *ydbcoordination.Spec {
	if v.After != nil {
		return &v.After.Spec
	}
	return nil
}

// CoordinationCodec describes the complete before/after wire for one node change.
func CoordinationCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := coordinationValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	models := ydbcoordination.Codecs()
	return schemaext.Codec{Prototype: &CoordinationNode{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := coordinationValue(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneChange(), nil
		},
		Decode: decodeCoordinationChange,
	}
}

func decodeCoordinationChange(data json.RawMessage) (schemaext.Payload, error) {
	before, after, err := decodeStandaloneOperands[*ydbcoordination.Observed, *ydbcoordination.Desired](data, "coordination", ydbcoordination.Codecs())
	if err != nil {
		return nil, err
	}
	value := &CoordinationNode{Before: before, After: after}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func coordinationValue(payload schemaext.Payload) (*CoordinationNode, error) {
	value, ok := payload.(*CoordinationNode)
	if !ok {
		return nil, fmt.Errorf("%w: expected a coordination change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

// Effect records consequences established by the complete operands. The same
// metadata feeds diff reports and the operation produced by the owner planner.
// Restoring configuration cannot restore runtime semaphores or rate limiters.
func (v *CoordinationNode) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	if v.Before == nil {
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a coordination node without removing existing state"}
	}
	if v.After == nil {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP COORDINATION NODE removes the node with its semaphores and rate limiter resources, even while a session holds a lock on it"}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "coordination settings change session timing, consistency, or rate limiter counters"}
}
