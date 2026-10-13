// Package pgroutinesource reads and writes PostgreSQL function properties in
// source formats. Frontends supply evaluated data and retain syntax ownership.
package pgroutinesource

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgroutine"
)

const functionDirective = "ptah:schema:function"

// Coverage states that a source describes the properties of every function it
// declares. A function without properties uses the server defaults.
func Coverage() (schemaext.Coverage, error) {
	return pgroutine.PropertiesCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// Annotations registers the PostgreSQL function attributes and their encoder.
func Annotations() annotation.Extension {
	return annotation.Extension{
		Owner: pgroutine.PropertiesOwner, Kinds: []schemaext.Kind{pgroutine.PropertiesKind},
		Attributes: []annotation.DirectiveAttributes{{Directive: functionDirective, Attributes: []annotation.Attribute{
			{Name: "leakproof", Value: "boolean", Description: "Marks the function LEAKPROOF, letting a filter using it be pushed past a security barrier."},
			{Name: "parallel", Value: "string", Description: "Parallel level: SAFE, RESTRICTED or UNSAFE."},
			{Name: "strict", Value: "boolean", Description: "Marks the function STRICT: it returns NULL without running when any argument is NULL."},
		}, Decode: properties}},
		Coverage: annotation.Unlimited(Coverage), Encoder: annotationEncoder{}, Encodes: []schemaext.Kind{pgroutine.PropertiesKind},
	}
}

// YAML registers the same attributes on function entries.
func YAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgroutine.PropertiesOwner, Kinds: []schemaext.Kind{pgroutine.PropertiesKind},
		EntryAttributes: []yamlext.EntryAttributes{{Entry: yamlext.EntryFunction, Attributes: []string{"leakproof", "parallel", "strict"}, Decode: properties}}, Coverage: Coverage}
}
func properties(attributes map[string]string) (schemaext.Facets, error) {
	if raw, found := attributes["parallel"]; found && strings.TrimSpace(raw) == "" {
		return schemaext.Facets{}, fmt.Errorf("%w: parallel must be SAFE, RESTRICTED or UNSAFE", schemaext.ErrInvalidValue)
	}
	p := pgroutine.Properties{Parallel: attributes["parallel"]}
	for _, item := range []struct {
		name        string
		destination *bool
	}{{"leakproof", &p.Leakproof}, {"strict", &p.Strict}} {
		if raw, found := attributes[item.name]; found {
			value, err := strconv.ParseBool(raw)
			if err != nil {
				return schemaext.Facets{}, fmt.Errorf("%w: %s must be a boolean, not %q", schemaext.ErrInvalidValue, item.name, raw)
			}
			*item.destination = value
		}
	}
	return pgroutine.WithProperties(schemaext.Facets{}, p)
}

type annotationEncoder struct{}

func (annotationEncoder) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if err := exportCoverage(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	result := annotation.EncodeResult{}
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		if slot.Slot.Directive != functionDirective {
			return annotation.EncodeResult{}, fmt.Errorf("%w: function properties on %s", schemaext.ErrInvalidValue, slot.Slot)
		}
		value, _, err := pgroutine.PropertiesOf(slot.Facets)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		var attributes []annotation.EncodedAttribute
		if value.Leakproof {
			attributes = append(attributes, annotation.EncodedAttribute{Name: "leakproof", Value: "true"})
		}
		if value.Parallel != "" {
			attributes = append(attributes, annotation.EncodedAttribute{Name: "parallel", Value: value.Parallel})
		}
		if value.Strict {
			attributes = append(attributes, annotation.EncodedAttribute{Name: "strict", Value: "true"})
		}
		result.Attributes = append(result.Attributes, annotation.SlotAttributes{Slot: slot.Slot, Attributes: attributes})
	}
	return result, ctx.Err()
}
