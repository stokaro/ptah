package ydbast

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ResourcePoolCodec describes the version-one pool operation and both operands.
// Omitted optional settings remain distinct from explicit zero values.
func ResourcePoolCodec() schemaext.Codec {
	spec := `{"type":"object","additionalProperties":false,"properties":{"concurrent_query_limit":{"type":"integer","minimum":0,"maximum":2147483647},
"queue_size":{"type":"integer","minimum":0,"maximum":2147483647},"database_load_cpu_threshold":{"type":"number","minimum":0,"maximum":100},
"query_memory_limit_percent_per_node":{"type":"number","minimum":0,"maximum":100},"query_cpu_limit_percent_per_node":{"type":"number","minimum":0,
"maximum":100},"total_cpu_limit_percent_per_node":{"type":"number","minimum":0,"maximum":100},"resource_weight":{"type":"number","minimum":0,
"maximum":100}}}`
	allowed := slices.DeleteFunc(ydbworkload.PoolAttributes(), func(name string) bool { return name == ydbworkload.AttributeName })
	return workloadCodec(&ResourcePool{}, spec, nil, allowed)
}

// ResourcePoolClassifierCodec describes explicit routing operands. Rank is
// required even when zero; omitted ranks must not acquire a server-chosen value.
func ResourcePoolClassifierCodec() schemaext.Codec {
	spec := `{"type":"object","required":["resource_pool","rank"],"additionalProperties":false,"properties":{"resource_pool":{"type":"string","minLength":1},
"member_name":{"type":"string"},"rank":{"type":"integer","minimum":0,"maximum":9223372036854775807}}}`
	return workloadCodec(&ResourcePoolClassifier{}, spec, []string{"resource_pool", "rank"}, []string{"resource_pool", "member_name", "rank"})
}

type workloadPayload interface {
	ast.ExtensionPayload
	Validate() error
}

func workloadCodec[T workloadPayload](prototype T, spec string, required, allowed []string) schemaext.Codec {
	validated := func(payload schemaext.Payload) (T, error) {
		value, ok := payload.(T)
		if !ok {
			return value, fmt.Errorf("%w: unexpected workload operation %T", schemaext.ErrInvalidValue, payload)
		}
		if err := value.Validate(); err != nil {
			return value, &schemaext.InvalidModelError{Kind: prototype.Kind(), Representation: schemaext.Operation, Message: err.Error()}
		}
		return value, nil
	}
	encode := func(payload schemaext.Payload) (json.RawMessage, error) {
		value, err := validated(payload)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	return schemaext.Codec{Prototype: prototype, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["operation","name"],"additionalProperties":false,"properties":{"operation":{"enum":["create","alter","drop"]},
"name":{"type":"string","minLength":1},"spec":%s,"previous":%s},"oneOf":[{"properties":{"operation":{"const":"create"}},"required":["spec"],
"not":{"required":["previous"]}},{"properties":{"operation":{"const":"alter"}},"required":["spec","previous"]},
{"properties":{"operation":{"const":"drop"}},"not":{"anyOf":[{"required":["spec"]},{"required":["previous"]}]}}]}`, spec, spec)),
		Encode: encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.CloneExtension(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			fields, err := workloadFields(data, []string{"operation", "name"}, []string{"operation", "name", "spec", "previous"})
			if err != nil {
				return nil, err
			}
			for _, key := range []string{"spec", "previous"} {
				if raw, found := fields[key]; found {
					if _, err := workloadFields(raw, required, allowed); err != nil {
						return nil, err
					}
				}
			}
			value, err := schemaext.DecodeJSON[T](data)
			if err != nil {
				return nil, err
			}
			checked, err := validated(value)
			if err != nil {
				return nil, err
			}
			return checked, nil
		},
	}
}

func workloadFields(data json.RawMessage, required, allowed []string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("%w: workload operation requires an object", schemaext.ErrInvalidValue)
	}
	for _, key := range required {
		if len(fields[key]) == 0 {
			return nil, fmt.Errorf("%w: workload operation requires %s", schemaext.ErrInvalidValue, key)
		}
	}
	for key, value := range fields {
		if !slices.Contains(allowed, key) {
			return nil, fmt.Errorf("%w: unknown workload operation field %s", schemaext.ErrInvalidValue, key)
		}
		if string(value) == "null" {
			return nil, fmt.Errorf("%w: workload operation field %s cannot be null", schemaext.ErrInvalidValue, key)
		}
	}
	return fields, nil
}
