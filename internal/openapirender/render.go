// Package openapirender renders a Ptah schema as an OpenAPI 3.0 document whose
// components.schemas holds one Schema Object per table. The output is a minimal
// but valid OpenAPI document (empty paths) so it passes an OpenAPI linter and can
// be $ref'd or merged into a hand-authored spec.
package openapirender

import (
	"bytes"
	"fmt"

	yaml "go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

const (
	openAPIVersion = "3.0.3"
	defaultTitle   = "Ptah Exported Schema"
	defaultVersion = "1.0.0"
)

// Options controls the OpenAPI export.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// Title and Version populate the required info block. Both fall back to
	// sensible defaults when empty.
	Title   string
	Version string
}

// Result is the rendered OpenAPI YAML plus any lossy-export diagnostics.
type Result struct {
	Data        []byte
	Diagnostics []schemaexport.Diagnostic
}

// Render renders db as a deterministic OpenAPI 3.0 YAML document.
func Render(db *goschema.Database, opts Options) (Result, error) {
	if db == nil {
		return Result{}, fmt.Errorf("schema database is nil")
	}

	tables := schemaexport.SelectTables(db, schemaexport.Options{
		IncludeTables: opts.IncludeTables,
		ExcludeTables: opts.ExcludeTables,
	})
	// Refused before anything is written, for the same reason a field
	// collision is: a table that shadows another drops it from the document,
	// and the document cannot record that it lost one.
	if err := schemaexport.ValidateTableAPINames(tables); err != nil {
		return Result{}, err
	}
	enums := schemaexport.EnumIndex(db)

	var diagnostics []schemaexport.Diagnostic
	schemas := newOrderedMap()
	for _, table := range tables {
		fields := schemaexport.FieldsFor(db, table)
		// Refused before anything is written: an alias that shadows another
		// column would drop it from the document, and the reader of the
		// document has nothing left to notice the loss with.
		if err := schemaexport.ValidateFieldAPINames(table, fields); err != nil {
			return Result{}, err
		}
		pk := toSet(schemaexport.EffectivePrimaryKey(table, fields))
		obj := &schemaObject{Type: "object"}
		obj.Description = table.Comment
		properties := newOrderedMap()
		var required []string
		for _, field := range fields {
			property, diag := columnSchema(table, field, enums, pk)
			if diag != nil {
				diagnostics = append(diagnostics, *diag)
			}
			apiName := schemaexport.FieldAPIName(field)
			properties.set(apiName, property)
			// A primary-key column is NOT NULL by SQL rule, regardless of how the
			// nullability was declared on the source annotation. The membership
			// test stays on the COLUMN name: the primary key is a property of
			// the table, not of what the column is published as.
			if !field.Nullable || pk[field.Name] {
				required = append(required, apiName)
			}
		}
		obj.Required = required
		if properties.len() > 0 {
			obj.Properties = properties
		}
		schemas.set(schemaexport.TableAPIName(table), obj)
	}

	doc := document{
		OpenAPI: openAPIVersion,
		Info: info{
			Title:   firstNonEmpty(opts.Title, defaultTitle),
			Version: firstNonEmpty(opts.Version, defaultVersion),
		},
		Servers:    []server{{URL: "/"}},
		Paths:      map[string]any{},
		Components: components{Schemas: schemas},
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return Result{}, fmt.Errorf("encode OpenAPI document: %w", err)
	}
	if err := enc.Close(); err != nil {
		return Result{}, fmt.Errorf("finalize OpenAPI document: %w", err)
	}
	return Result{Data: buf.Bytes(), Diagnostics: diagnostics}, nil
}

// columnSchema builds the Schema Object for one column, resolving enums and
// mapping the SQL type. It returns a diagnostic when a type could not be
// resolved and was defaulted to string.
func columnSchema(table goschema.Table, field goschema.Field, enums map[string][]string, pk map[string]bool) (*schemaObject, *schemaexport.Diagnostic) {
	// A primary-key column is NOT NULL by SQL rule, regardless of how the
	// nullability was declared on the source annotation.
	nullable := field.Nullable && !pk[field.Name]

	// An array column maps to an array schema whose items are the element type.
	if element, isArray := schemaexport.ElementType(field.Type); isArray {
		elementField := field
		elementField.Type = element
		elementField.Nullable = false // the items schema carries its own (non-null) shape
		items, diag := columnSchema(table, elementField, enums, pk)
		return &schemaObject{
			Type:        "array",
			Items:       items,
			Description: field.Comment,
			Nullable:    nullable,
		}, diag
	}

	obj := &schemaObject{Description: field.Comment, Nullable: nullable}

	if values, ok := schemaexport.ResolveEnumValues(field, enums); ok {
		obj.Type = "string"
		obj.Enum = toAnySlice(values)
		// Under OpenAPI 3.0 `nullable: true` alone does not permit null against an
		// enum constraint, so null must be an explicit member.
		if nullable {
			obj.Enum = append(obj.Enum, nil)
		}
		return obj, nil
	}

	mapped := mapOpenAPIType(field.Type)
	obj.Type = mapped.Type
	obj.Format = mapped.Format
	obj.MaxLength = mapped.MaxLength
	obj.Minimum = mapped.Minimum
	if !mapped.Known {
		// The path is a coordinate INSIDE the document, so it spells the names
		// the document uses. A reader who resolves it against the output finds
		// the property; the source names would point at nothing there.
		return obj, &schemaexport.Diagnostic{
			Severity: schemaexport.SeverityWarning,
			Path: "components.schemas." + schemaexport.TableAPIName(table) +
				".properties." + schemaexport.FieldAPIName(field),
			Message: fmt.Sprintf("unknown column type %q mapped to string", field.Type),
		}
	}
	return obj, nil
}

func toAnySlice(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

func firstNonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func toSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}
