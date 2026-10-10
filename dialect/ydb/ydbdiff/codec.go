package ydbdiff

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// Codecs describes the current self-contained YDB change representations.
// Before and After retain distinct observed and desired model meanings.
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{{Prototype: &Changefeed{}, Representation: schemaext.Change, Version: 1,
		Definition: ydbschema.WireDefinition(),
		Clone: func(value schemaext.Payload) (schemaext.Payload, error) {
			change, err := changefeedValue(value)
			if err != nil {
				return nil, err
			}
			return change.CloneChange(), nil
		},
		Encode: func(value schemaext.Payload) (json.RawMessage, error) {
			change, err := changefeedValue(value)
			if err != nil {
				return nil, err
			}
			return json.Marshal(change)
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			change, err := schemaext.DecodeJSON[*Changefeed](data)
			if err != nil {
				return nil, err
			}
			return changefeedValue(change)
		},
		Canonical: func(value schemaext.Payload) (json.RawMessage, error) {
			change, err := changefeedValue(value)
			if err != nil {
				return nil, err
			}
			cloned := change.clone()
			if cloned.Before != nil {
				ydbschema.CanonicalChangefeed(&cloned.Before.Spec)
			}
			if cloned.After != nil {
				ydbschema.CanonicalChangefeed(&cloned.After.Spec)
			}
			return json.Marshal(cloned)
		},
	}, CoordinationCodec(), StreamingCodec(), ResourcePoolCodec(), ResourcePoolClassifierCodec(), SecretCodec(), TopicCodec(), ExternalDataSourceCodec(), ExternalTableCodec(), AsyncReplicationCodec(), TransferCodec()}
}

func changefeedValue(value schemaext.Payload) (*Changefeed, error) {
	change, ok := value.(*Changefeed)
	if !ok || change == nil || (change.Before == nil && change.After == nil) {
		return nil, fmt.Errorf("%w: a changefeed change requires a before or after definition", schemaext.ErrInvalidValue)
	}
	if change.Before != nil {
		if err := change.Before.Replication.Validate(); err != nil {
			return nil, err
		}
		if err := ydbschema.ValidateChangefeed(change.Before.Spec); err != nil {
			return nil, err
		}
	}
	if change.After != nil {
		if err := change.After.RetainedReplication.Validate(); err != nil {
			return nil, err
		}
		if err := ydbschema.ValidateChangefeed(change.After.Spec); err != nil {
			return nil, err
		}
	}
	if change.Before != nil && change.After != nil && change.Before.Spec.Name != change.After.Spec.Name {
		return nil, fmt.Errorf("%w: a changefeed change cannot rename its subject", schemaext.ErrInvalidValue)
	}
	return change, nil
}
