package annotationschema_test

import (
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/annotationschema"
)

func TestGenerateMatchesCommittedSchema(t *testing.T) {
	c := qt.New(t)

	generated, err := annotationschema.Generate()
	c.Assert(err, qt.IsNil)
	committed, err := os.ReadFile("../../" + annotationschema.SchemaPath)
	c.Assert(err, qt.IsNil)

	c.Assert(string(generated), qt.Equals, string(committed))
}

func TestGenerateFieldSchemaRejectsUnknownAttributes(t *testing.T) {
	c := qt.New(t)

	generated, err := annotationschema.Generate()
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(json.Unmarshal(generated, &doc), qt.IsNil)
	defs := doc["$defs"].(map[string]any)
	field := defs["migrator.schema.field"].(map[string]any)
	properties := field["properties"].(map[string]any)
	attrs := properties["attributes"].(map[string]any)

	c.Assert(attrs["additionalProperties"], qt.Equals, false)
	c.Assert(attrs["patternProperties"], qt.IsNotNil)
	patterns := attrs["patternProperties"].(map[string]any)
	c.Assert(patterns[`^platform\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$`], qt.IsNotNil)
	attrProps := attrs["properties"].(map[string]any)
	c.Assert(attrProps["default_expr"], qt.IsNotNil)
	c.Assert(attrProps["defaul"], qt.IsNil)
}
