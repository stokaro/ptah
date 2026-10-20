package schemaext

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/stokaro/ptah/provider-sdk/modeljson"
)

// CanonicalJSON applies the shared lossless wire normalization and reports
// malformed data through the schema model's ErrInvalidValue sentinel. It does
// not define semantic equality; owners supply their canonical projections.
func CanonicalJSON(data []byte) (json.RawMessage, error) {
	value, err := modeljson.Canonical(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidValue, err)
	}
	return value, nil
}

// DecodeJSON decodes a concrete wire model, refusing unknown fields and duplicate
// keys before the owner's shape validation. No interface payload is inferred.
func DecodeJSON[T any](data json.RawMessage) (T, error) {
	var value T
	canonical, err := CanonicalJSON(data)
	if err != nil {
		return value, err
	}
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("%w: decode concrete payload: %v", ErrInvalidValue, err)
	}
	return value, nil
}
