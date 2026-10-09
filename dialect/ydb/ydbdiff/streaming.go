package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingQueryKind identifies a change to one standalone YDB query.
const StreamingQueryKind schemaext.Kind = "ptah.run/ydb/streaming-query-change"

// StreamingQuery captures complete operands. A nil side means established
// absence, never an unreadable query. Settings retain their raw stored form.
// Runtime offsets and checkpoints are not schema configuration.
type StreamingQuery struct {
	Before *ydbstreaming.Observed `json:"before"`
	After  *ydbstreaming.Desired  `json:"after"`
}

// Kind returns the stable change identity.
func (*StreamingQuery) Kind() schemaext.Kind { return StreamingQueryKind }

// CloneChange returns independent before and after configuration snapshots.
func (v *StreamingQuery) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*StreamingQuery)(nil)
	}
	cloned := &StreamingQuery{}
	if v.Before != nil {
		cloned.Before = &ydbstreaming.Observed{Spec: v.Before.Spec.Clone()}
	}
	if v.After != nil {
		cloned.After = new(*v.After)
		cloned.After.Spec = v.After.Spec.Clone()
	}
	return cloned
}

// Validate refuses missing operands and invalid query bodies.
// Semantic equality is a planning decision, not a wire-format decision.
func (v *StreamingQuery) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: streaming change requires a before or after definition", schemaext.ErrInvalidValue)
	}
	for _, spec := range []*ydbstreaming.Spec{streamingBefore(v), streamingAfter(v)} {
		if spec != nil {
			if err := ydbstreaming.Validate(*spec); err != nil {
				return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
			}
		}
	}
	return nil
}

func streamingBefore(v *StreamingQuery) *ydbstreaming.Spec {
	if v.Before != nil {
		return &v.Before.Spec
	}
	return nil
}

func streamingAfter(v *StreamingQuery) *ydbstreaming.Spec {
	if v.After != nil {
		return &v.After.Spec
	}
	return nil
}

// StreamingCodec describes the complete before/after wire for one query change.
func StreamingCodec() schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := streamingValue(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	definition := `{"type":"object","required":["before","after"],"additionalProperties":false,"properties":{"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]}}}`
	models := ydbstreaming.Codecs()
	return schemaext.Codec{Prototype: &StreamingQuery{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(definition, models[1].Definition, models[0].Definition)),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := streamingValue(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneChange(), nil
		},
		Decode: decodeStreamingChange,
	}
}

func decodeStreamingChange(data json.RawMessage) (schemaext.Payload, error) {
	before, after, err := decodeStandaloneOperands[*ydbstreaming.Observed, *ydbstreaming.Desired](data, "streaming", ydbstreaming.Codecs())
	if err != nil {
		return nil, err
	}
	value := &StreamingQuery{Before: before, After: after}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

func streamingValue(payload schemaext.Payload) (*StreamingQuery, error) {
	value, ok := payload.(*StreamingQuery)
	if !ok {
		return nil, fmt.Errorf("%w: expected a streaming change, got %T", schemaext.ErrInvalidValue, payload)
	}
	if err := value.Validate(); err != nil {
		return nil, err
	}
	return value, nil
}

// Effect records checkpoint and execution consequences of the captured change.
func (v *StreamingQuery) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	if v.Before == nil {
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a streaming query"}
	}
	if v.After == nil {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbstreaming.CheckpointLoss}
	}
	if !ydbstreaming.SameBody(v.Before.Spec.Text, v.After.Spec.Text) {
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbstreaming.CheckpointLoss}
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbstreaming.ExecutionChange}
}
