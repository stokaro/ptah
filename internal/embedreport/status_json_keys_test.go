package embedreport_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedreport"
)

// TestStatus_EveryJSONKeyIsSnakeCaseHappyPath covers stokaro/ptah#2741.
//
// `status --format json` emitted one Go field name into an otherwise
// snake_case document -- `"Progress"` among twenty siblings that all carried a
// tag. A machine reader keying on `progress` found nothing, and the nested keys
// being correct made it easy to miss.
//
// The assertion walks every key at both levels rather than naming the one that
// was wrong, because the defect was a field that nobody gave a tag: a test that
// checked for `progress` alone would pass while the next untagged field
// shipped the same way.
func TestStatus_EveryJSONKeyIsSnakeCaseHappyPath(t *testing.T) {
	c := qt.New(t)

	encoded, err := json.Marshal(embedreport.Status{})
	c.Assert(err, qt.IsNil)

	var document map[string]json.RawMessage
	c.Assert(json.Unmarshal(encoded, &document), qt.IsNil)
	// A liveness floor, not a census: most of this struct carries omitempty, so
	// a zero value renders nine keys. It is here because a sweep over an empty
	// document finds nothing and reports success.
	c.Assert(len(document) > 5, qt.IsTrue, qt.Commentf("keys: %v", keysOf(document)))
	c.Assert(pascalCaseKeys(c, encoded), qt.HasLen, 0)

	// The key the issue names, asserted by its presence rather than only by the
	// absence above: a `json:"-"` would satisfy the sweep and delete the field.
	_, found := document["progress"]
	c.Assert(found, qt.IsTrue, qt.Commentf("keys: %v", keysOf(document)))
}

// pascalCaseKeys returns every key in the document, at any depth, whose first
// rune is upper case -- which is what an untagged Go field serializes as.
func pascalCaseKeys(c *qt.C, encoded []byte) []string {
	c.Helper()
	var decoded any
	c.Assert(json.Unmarshal(encoded, &decoded), qt.IsNil)
	return upperCaseKeysIn(decoded, nil)
}

func upperCaseKeysIn(value any, path []string) []string {
	var found []string
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			at := append(append([]string(nil), path...), key)
			if key != "" && key[0] >= 'A' && key[0] <= 'Z' {
				found = append(found, joinPath(at))
			}
			found = append(found, upperCaseKeysIn(nested, at)...)
		}
	case []any:
		for _, nested := range typed {
			found = append(found, upperCaseKeysIn(nested, path)...)
		}
	}
	return found
}

func joinPath(path []string) string {
	joined := ""
	for index, part := range path {
		if index > 0 {
			joined += "."
		}
		joined += part
	}
	return joined
}

func keysOf(document map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(document))
	for key := range document {
		keys = append(keys, key)
	}
	return keys
}
