package ydbworkload

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// PoolCodecs describes version-one desired and observed pool configuration.
// Both representations preserve every optional setting, including explicit zero.
func PoolCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredPool{}, schemaext.Desired, poolDefinition, nil, poolFields),
		modelCodec(&ObservedPool{}, schemaext.Observed, poolDefinition, nil, poolFields),
	}
}

// ClassifierCodecs describes version-one routing configuration. Rank is required
// even at zero. A codec never assigns the server's context-dependent default rank.
func ClassifierCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		modelCodec(&DesiredClassifier{}, schemaext.Desired, classifierDefinition, []string{"resource_pool", "rank"}, classifierFields),
		modelCodec(&ObservedClassifier{}, schemaext.Observed, classifierDefinition, []string{"resource_pool", "rank"}, classifierFields),
	}
}

// Codecs returns model descriptors for both workload object families.
func Codecs() []schemaext.Codec {
	return append(PoolCodecs(), ClassifierCodecs()...)
}

// PoolSpecSchema returns an independent JSON Schema for the version-one settings
// object. Contextual name restrictions and target capabilities remain separate.
func PoolSpecSchema() json.RawMessage { return json.RawMessage(poolDefinition) }

// DecodePoolSpec decodes the exact version-one settings grammar. Unknown,
// duplicate, case-variant, null, and invalid values are refused. Errors wrap
// schemaext.ErrInvalidValue and return a zero spec; unset and zero stay distinct.
func DecodePoolSpec(data json.RawMessage) (PoolSpec, error) {
	if _, err := wireFields(data, nil, poolFields); err != nil {
		return PoolSpec{}, err
	}
	spec, err := schemaext.DecodeJSON[PoolSpec](data)
	if err != nil {
		return PoolSpec{}, err
	}
	if err := ValidatePool(spec); err != nil {
		return PoolSpec{}, err
	}
	return spec, nil
}

const poolDefinition = `{"type":"object","additionalProperties":false,"properties":{
"concurrent_query_limit":{"type":"integer","minimum":0,"maximum":2147483647},
"queue_size":{"type":"integer","minimum":0,"maximum":2147483647},
"database_load_cpu_threshold":{"type":"number","minimum":0,"maximum":100},
"query_memory_limit_percent_per_node":{"type":"number","minimum":0,"maximum":100},
"query_cpu_limit_percent_per_node":{"type":"number","minimum":0,"maximum":100},
"total_cpu_limit_percent_per_node":{"type":"number","minimum":0,"maximum":100},
"resource_weight":{"type":"number","minimum":0,"maximum":100}}}`

const classifierDefinition = `{"type":"object","required":["resource_pool","rank"],"additionalProperties":false,"properties":{
"resource_pool":{"type":"string","minLength":1},"member_name":{"type":"string"},
"rank":{"type":"integer","minimum":0,"maximum":9223372036854775807}}}`

var poolFields = []string{"concurrent_query_limit", "queue_size", "database_load_cpu_threshold", "query_memory_limit_percent_per_node",
	"query_cpu_limit_percent_per_node", "total_cpu_limit_percent_per_node", "resource_weight"}

var classifierFields = []string{"resource_pool", "member_name", "rank"}

type model interface {
	schemaext.Value
	validate() error
}

func modelCodec[T model](prototype T, representation schemaext.Representation, specDefinition string, required, allowed []string) schemaext.Codec {
	validated := func(payload schemaext.Payload) (T, error) {
		value, ok := payload.(T)
		if !ok {
			return value, fmt.Errorf("%w: unexpected workload model %T", schemaext.ErrInvalidValue, payload)
		}
		if err := value.validate(); err != nil {
			return value, &schemaext.InvalidModelError{Kind: prototype.Kind(), Representation: representation, Message: err.Error()}
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
	properties := `"spec":` + specDefinition
	fields := []string{"spec"}
	if representation == schemaext.Desired {
		properties += `,"struct_name":{"type":"string"}`
		fields = append(fields, "struct_name")
	}
	return schemaext.Codec{Prototype: prototype, Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object","required":["spec"],"additionalProperties":false,"properties":{` + properties + `}}`),
		Encode:     encode, Canonical: encode,
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			value, err := validated(payload)
			if err != nil {
				return nil, err
			}
			return value.Clone(), nil
		},
		Decode: func(data json.RawMessage) (schemaext.Payload, error) {
			wire, err := wireFields(data, []string{"spec"}, fields)
			if err != nil {
				return nil, err
			}
			if _, err := wireFields(wire["spec"], required, allowed); err != nil {
				return nil, err
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

func wireFields(data json.RawMessage, required, allowed []string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("%w: workload models require an object", schemaext.ErrInvalidValue)
	}
	for _, key := range required {
		if len(fields[key]) == 0 {
			return nil, fmt.Errorf("%w: workload models require %s", schemaext.ErrInvalidValue, key)
		}
	}
	for key, value := range fields {
		if !slices.Contains(allowed, key) || string(value) == "null" {
			return nil, fmt.Errorf("%w: unknown or null workload field %s", schemaext.ErrInvalidValue, key)
		}
	}
	return fields, nil
}
