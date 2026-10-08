package schemaext

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

// CanonicalJSON normalizes JSON object-key order and insignificant whitespace.
// It preserves array order and number precision. Duplicate keys, invalid UTF-8,
// trailing values, and excessive nesting are errors. This is wire normalization,
// not feature equality; owners define set ordering in their canonical codecs.
func CanonicalJSON(data []byte) (json.RawMessage, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%w: JSON is not valid UTF-8", ErrInvalidValue)
	}
	if err := checkUnicodeEscapes(data); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := jsonValue(decoder, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %v", ErrInvalidValue, err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing JSON content", ErrInvalidValue)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%w: encode JSON: %v", ErrInvalidValue, err)
	}
	return canonical, nil
}

// encoding/json substitutes U+FFFD for an unpaired escaped surrogate. Reject
// that input before decoding so distinct source strings cannot collapse.
func checkUnicodeEscapes(data []byte) error {
	inString := false
	for i := 0; i < len(data); i++ {
		if data[i] == '"' {
			inString = !inString
			continue
		}
		if !inString || data[i] != '\\' || i+1 >= len(data) {
			continue
		}
		if data[i+1] != 'u' {
			i++
			continue
		}
		value, ok := escapedCodePoint(data, i)
		if !ok {
			// The JSON parser reports malformed escapes.
			continue
		}
		switch {
		case value >= 0xD800 && value <= 0xDBFF:
			low, ok := escapedCodePoint(data, i+6)
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return fmt.Errorf("%w: unpaired JSON surrogate", ErrInvalidValue)
			}
			i += 11
		case value >= 0xDC00 && value <= 0xDFFF:
			return fmt.Errorf("%w: unpaired JSON surrogate", ErrInvalidValue)
		default:
			i += 5
		}
	}
	return nil
}

func escapedCodePoint(data []byte, offset int) (uint64, bool) {
	if offset+6 > len(data) || data[offset] != '\\' || data[offset+1] != 'u' {
		return 0, false
	}
	value, err := strconv.ParseUint(string(data[offset+2:offset+6]), 16, 16)
	return value, err == nil
}

func jsonValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 256 {
		return nil, fmt.Errorf("nesting exceeds 256 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		return jsonObject(decoder, depth)
	case '[':
		var values []any
		for decoder.More() {
			value, err := jsonValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		// A JSON empty array remains [], never null.
		if values == nil {
			values = make([]any, 0)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected delimiter %q", delimiter)
	}
}

func jsonObject(decoder *json.Decoder, depth int) (map[string]any, error) {
	values := make(map[string]any)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("object key is not a string")
		}
		if _, duplicate := values[key]; duplicate {
			return nil, fmt.Errorf("duplicate object key %q", key)
		}
		value, err := jsonValue(decoder, depth+1)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	return values, nil
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
