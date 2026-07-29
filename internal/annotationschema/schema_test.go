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

func TestGenerateOmitsDroppedAnnotationSyntax(t *testing.T) {
	c := qt.New(t)

	generated, err := annotationschema.Generate()
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(json.Unmarshal(generated, &doc), qt.IsNil)
	defs := doc["$defs"].(map[string]any)

	field := defs["migrator.schema.field"].(map[string]any)
	fieldContainer := field["properties"].(map[string]any)
	fieldAttributes := fieldContainer["attributes"].(map[string]any)
	fieldProperties := fieldAttributes["properties"].(map[string]any)
	c.Assert(fieldProperties["nullable"], qt.IsNil)
	c.Assert(fieldProperties["autoincrement"], qt.IsNil)
	c.Assert(fieldProperties["index"], qt.IsNil)

	embedded := defs["migrator.embedded"].(map[string]any)
	embeddedContainer := embedded["properties"].(map[string]any)
	embeddedAttributes := embeddedContainer["attributes"].(map[string]any)
	embeddedProperties := embeddedAttributes["properties"].(map[string]any)
	c.Assert(embeddedProperties["not_null"], qt.IsNil)
	c.Assert(embeddedProperties["index"], qt.IsNil)

	directivesWithoutPlatformOverrides := []string{
		"migrator.schema.index",
		"migrator.schema.schema",
		"migrator.schema.view",
		"migrator.schema.matview",
		"migrator.schema.trigger",
	}
	for _, directive := range directivesWithoutPlatformOverrides {
		c.Run(directive, func(c *qt.C) {
			definition := defs[directive].(map[string]any)
			properties := definition["properties"].(map[string]any)
			attributes := properties["attributes"].(map[string]any)
			c.Assert(attributes["patternProperties"], qt.IsNil)
		})
	}
}
