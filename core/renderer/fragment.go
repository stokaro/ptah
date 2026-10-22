package renderer

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// ErrNativeSQL identifies a native operation that cannot be represented as SQL.
var ErrNativeSQL = fmt.Errorf("%w: native operations cannot be represented as SQL", ptaherr.ErrUnsupportedFeature)

// Fragment holds either SQL text or a typed native operation, never both.
// Empty SQL accounts for a node that emits no work. A native payload remains
// owner-defined data; its methods must not execute it or call a provider.
// Persistence requires an explicit operation codec, not default JSON encoding.
type Fragment struct {
	SQL    string
	Native ast.ExtensionPayload
}

// SQLFragments wraps SQL text in node order without parsing or changing bytes.
func SQLFragments(text []string) []Fragment {
	if text == nil {
		return nil
	}
	result := make([]Fragment, len(text))
	for i, sql := range text {
		result[i] = Fragment{SQL: sql}
	}
	return result
}

// Validate refuses ambiguous bodies and invalid native payloads or clones.
func (f Fragment) Validate() error {
	_, err := f.Clone()
	return err
}

// Clone returns an independent snapshot and validates the payload boundary.
func (f Fragment) Clone() (Fragment, error) {
	if f.Native == nil {
		return f, nil
	}
	if f.SQL != "" {
		return Fragment{}, fmt.Errorf("%w: fragment has SQL and native bodies", ErrInvalidResult)
	}
	payload, err := ast.CloneExtensionPayload(f.Native)
	if err != nil {
		return Fragment{}, fmt.Errorf("%w: %w", ErrInvalidResult, err)
	}
	return Fragment{Native: payload}, nil
}

// MarshalJSON prevents encoding a native Go type without its owner's codec.
func (f Fragment) MarshalJSON() ([]byte, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	if f.Native != nil {
		return nil, schemaext.ErrExplicitCodec
	}
	return json.Marshal(struct {
		SQL string `json:"sql"`
	}{f.SQL})
}
