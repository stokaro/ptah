package schemaext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
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
	// NonEmpty are the keys an encoder leaves out while they hold their empty
	// value, as encoding/json's omitempty does. A present one holding "",
	// false, a zero number, [] or {} is refused: the encoder never writes that
	// spelling, so accepting it would give one value two encodings. A key
	// whose Go field is a pointer does not belong here, since omitempty omits
	// only its nil and false or zero is a value.
	NonEmpty []string
}

// DecodeObject decodes data as one JSON object of shape and returns each key
// it holds with its raw value.
//
// It refuses what [DecodeJSON] cannot see in a struct: a key spelled in another
// letter case, which encoding/json would match to a field, a null where the
// shape allows none, an empty value under a [ObjectShape.NonEmpty] key, and a
// missing required key. It also refuses a value that is not an object, null
// included, an unknown key, and a duplicate key. The refusal wraps
// [ErrInvalidValue] and names the first offending key in sorted order, so one
// input is refused with one message.
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
		if slices.Contains(shape.NonEmpty, key) && emptyJSON(fields[key]) {
			return nil, fmt.Errorf("%w: %s property %q cannot be empty; omit it instead", ErrInvalidValue, shape.Name, key)
		}
	}
	for _, key := range shape.Required {
		if _, found := fields[key]; !found {
			return nil, fmt.Errorf("%w: missing %s property %q", ErrInvalidValue, shape.Name, key)
		}
	}
	return fields, nil
}

// emptyJSON reports whether a canonical JSON value is empty in encoding/json's
// omitempty sense. A number is compared exactly, so 0.0 and -0e5 are empty
// and 1e-400, which a float64 would round to zero, is not.
func emptyJSON(value json.RawMessage) bool {
	switch string(value) {
	case `""`, `false`, `[]`, `{}`:
		return true
	}
	if len(value) == 0 || (value[0] != '-' && (value[0] < '0' || value[0] > '9')) {
		return false
	}
	number, ok := new(big.Float).SetString(string(value))
	return ok && number.Sign() == 0
}
