package schemaext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

// ObjectShape names the keys a strict JSON object may and must hold.
type ObjectShape struct {
	// Name is the object's name in a refusal, such as "policy".
	Name string
	// Allowed is every key the object may hold, spelled exactly.
	Allowed []string
	// Required are the keys it must hold. Each must also be in Allowed.
	Required []string
	// Nullable are the keys whose value may be JSON null. Any other key holding
	// null is refused: null is neither an omitted value nor an absence.
	Nullable []string
}

// DecodeObject decodes data as one JSON object of shape and returns each key
// it holds with its raw value.
//
// It refuses what [DecodeJSON] cannot see in a struct: a key spelled in another
// letter case, which encoding/json would match to a field, a null where the
// shape allows none, and a missing required key. It also refuses a value that
// is not an object, null included, an unknown key, and a duplicate key. The
// refusal wraps [ErrInvalidValue] and names the first offending key in sorted
// order, so one input is refused with one message.
func DecodeObject(data json.RawMessage, shape ObjectShape) (map[string]json.RawMessage, error) {
	canonical, err := CanonicalJSON(data)
	if err != nil {
		return nil, err
	}
	if !bytes.HasPrefix(canonical, []byte("{")) {
		return nil, fmt.Errorf("%w: expected a %s object", ErrInvalidValue, shape.Name)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &fields); err != nil {
		return nil, fmt.Errorf("%w: decode %s object: %v", ErrInvalidValue, shape.Name, err)
	}
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if !slices.Contains(shape.Allowed, key) {
			return nil, fmt.Errorf("%w: unknown %s property %q", ErrInvalidValue, shape.Name, key)
		}
		if bytes.Equal(fields[key], []byte("null")) && !slices.Contains(shape.Nullable, key) {
			return nil, fmt.Errorf("%w: %s property %q cannot be null", ErrInvalidValue, shape.Name, key)
		}
	}
	for _, key := range shape.Required {
		if _, found := fields[key]; !found {
			return nil, fmt.Errorf("%w: missing %s property %q", ErrInvalidValue, shape.Name, key)
		}
	}
	return fields, nil
}
