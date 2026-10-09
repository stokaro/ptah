package chast

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// ModifyRefreshKind identifies an in-place change to a refreshable
// materialized view's schedule.
const ModifyRefreshKind schemaext.Kind = "ptah.run/clickhouse/modify-refresh"

// ModifyRefresh sets the refresh schedule of a materialized view that already
// has one, keeping the rows the view holds. Use it inside
// ast.ExtensionAlterOperation with an ast.AlterTableNode naming the view, which
// is how ClickHouse spells it: `ALTER TABLE <view> MODIFY REFRESH ...`. A
// plain view cannot gain a schedule this way, a refreshable one cannot lose
// its schedule, and APPEND cannot be added or removed; each replaces the view.
type ModifyRefresh struct {
	Schedule chschema.Schedule `json:"schedule"`
}

// Kind returns the stable operation identity.
func (*ModifyRefresh) Kind() schemaext.Kind { return ModifyRefreshKind }

// CloneExtension returns an independent schedule; a nil receiver remains typed
// nil.
func (v *ModifyRefresh) CloneExtension() ast.ExtensionPayload {
	if v == nil {
		return (*ModifyRefresh)(nil)
	}
	return &ModifyRefresh{Schedule: v.Schedule.Clone()}
}

// Effect reports that the view keeps its rows and is refreshed on the new
// schedule.
func (*ModifyRefresh) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "MODIFY REFRESH changes when the view is refreshed; the rows it holds are kept"}
}

// Validate requires a valid schedule.
func (v *ModifyRefresh) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: ClickHouse MODIFY REFRESH requires a schedule", schemaext.ErrInvalidValue)
	}
	return chschema.ValidateDesiredRefresh(&chschema.DesiredRefresh{Schedule: v.Schedule})
}

func refreshCodec() schemaext.Codec {
	schedule := chschema.RefreshCodecs()[0]
	value := func(payload schemaext.Payload) (*ModifyRefresh, error) {
		v, ok := payload.(*ModifyRefresh)
		if !ok {
			return nil, fmt.Errorf("%w: expected ClickHouse MODIFY REFRESH, got %T", schemaext.ErrInvalidValue, payload)
		}
		return v, v.Validate()
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		v, err := value(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
	return schemaext.Codec{
		Prototype: &ModifyRefresh{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"schedule":%s}`, schedule.Definition)),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			v, err := value(payload)
			if err != nil {
				return nil, err
			}
			return v.CloneExtension(), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
			if err != nil {
				return nil, err
			}
			if len(fields) != 1 || fields["schedule"] == nil {
				return nil, fmt.Errorf("%w: ClickHouse MODIFY REFRESH requires exactly schedule", schemaext.ErrInvalidValue)
			}
			decoded, err := schedule.Decode(fields["schedule"])
			if err != nil {
				return nil, err
			}
			v, err := value(&ModifyRefresh{Schedule: decoded.(*chschema.DesiredRefresh).Schedule})
			if err != nil {
				return nil, err
			}
			return v, nil
		},
	}
}
