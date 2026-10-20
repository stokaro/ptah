package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// NormalizeValues demonstrates an independent owner's offline interpretation.
// The fixture accepts a retention declaration in seconds or minutes. Desired
// omission selects 60 seconds; observed omission is incomplete and is refused.
// This model exercises the SDK only; the fixture's SQL table service does not
// claim retention support and never silently applies this value to SQLite.
func (*Provider) NormalizeValues(ctx context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	result := &protocol.NormalizedValues{Complete: true}
	for _, input := range request.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value := input.GetValue()
		if value.GetKind() != "example.org/fixture/retention" || value.GetSchemaVersion() != 1 || value.GetEncoding() != "json" {
			return nil, fmt.Errorf("unsupported retention value")
		}
		if input.GetRepresentation() != protocol.ValueInput_DESIRED && input.GetRepresentation() != protocol.ValueInput_OBSERVED {
			return nil, fmt.Errorf("unsupported retention representation")
		}
		seconds, err := retentionSeconds(value.GetPayload(), input.GetRepresentation())
		if err != nil {
			return nil, err
		}
		normalized := proto.CloneOf(value)
		normalized.Payload = []byte(fmt.Sprintf(`{"seconds":%d}`, seconds))
		result.Values = append(result.Values, &protocol.NormalizedValue{
			Id: input.GetId(), Value: normalized, NormalizedOwner: proto.CloneOf(value.GetOwner()),
			CanonicalFormat: "example.org/fixture/retention/seconds-json-v1", CanonicalProjection: bytes.Clone(normalized.Payload),
		})
	}
	return result, nil
}

func retentionSeconds(payload []byte, representation protocol.ValueInput_Representation) (uint64, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return 0, fmt.Errorf("retention must be an object")
	}
	var seen bool
	var seconds uint64
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil || seen || (key != "seconds" && key != "minutes") {
			return 0, fmt.Errorf("unknown, repeated, or conflicting retention field")
		}
		seen = true
		var raw any
		if err := decoder.Decode(&raw); err != nil {
			return 0, fmt.Errorf("invalid retention number")
		}
		number, ok := raw.(json.Number)
		if !ok {
			return 0, fmt.Errorf("retention must be an exact nonnegative integer")
		}
		seconds, err = strconv.ParseUint(string(number), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("retention is outside the supported integer range")
		}
		if key == "minutes" {
			if seconds > math.MaxUint64/60 {
				return 0, fmt.Errorf("retention conversion overflows")
			}
			seconds *= 60
		}
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('}') {
		return 0, fmt.Errorf("unterminated retention object")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return 0, fmt.Errorf("trailing retention content")
	}
	if !seen {
		if representation != protocol.ValueInput_DESIRED {
			return 0, fmt.Errorf("observed retention is incomplete")
		}
		return 60, nil
	}
	return seconds, nil
}
