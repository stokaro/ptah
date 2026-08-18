// Package openapirender renders a Ptah schema as an OpenAPI 3.0 document whose
// components.schemas holds one Schema Object per table. The output is a minimal
// but valid OpenAPI document (empty paths) so it passes an OpenAPI linter and can
// be $ref'd or merged into a hand-authored spec.
package openapirender

import (
	"bytes"
	"fmt"
	"strings"

	yaml "go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

const (
	openAPIVersion = "3.0.3"
	defaultTitle   = "Ptah Exported Schema"
	defaultVersion = "1.0.0"
)

// withheldOnly keeps the diagnostics whose column reached neither shape.
func withheldOnly(diagnostics []schemaexport.Diagnostic, emitted []goschema.Field) []schemaexport.Diagnostic {
	present := make(map[string]bool, len(emitted))
	for _, field := range emitted {
		present[field.Name] = true
	}
	kept := make([]schemaexport.Diagnostic, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		column := diagnostic.Path[strings.LastIndex(diagnostic.Path, ".")+1:]
		if present[column] {
			continue
		}
		kept = append(kept, diagnostic)
	}
	return kept
}

// mergeWriteOnly folds the write shape into the read shape, and reports which
// columns belong to only one of them.
//
// OpenAPI publishes one schema per table, so a column a caller may send but
// never receives has to appear in that schema marked writeOnly rather than be
// absent from it. The returned order keeps the read columns in their schema
// order and appends the write-only ones, so a document does not reshuffle when
// an exposure changes.
func mergeWriteOnly(read, write []goschema.Field) (all []goschema.Field, writeOnly, readOnly map[string]bool) {
	readNames := make(map[string]bool, len(read))
	for _, field := range read {
		readNames[field.Name] = true
	}
	writeNames := make(map[string]bool, len(write))
	for _, field := range write {
		writeNames[field.Name] = true
	}
	all = append([]goschema.Field(nil), read...)
	writeOnly = make(map[string]bool)
	readOnly = make(map[string]bool)
	for _, field := range write {
		if !readNames[field.Name] {
			all = append(all, field)
			writeOnly[field.Name] = true
		}
	}
	for _, field := range read {
		if !writeNames[field.Name] {
			readOnly[field.Name] = true
		}
	}
	return all, writeOnly, readOnly
}

// Options controls the OpenAPI export.
type Options struct {
	IncludeTables []string
	ExcludeTables []string
	// Title and Version populate the required info block. Both fall back to
	// sensible defaults when empty.
	Title   string
	Version string
	// FieldPolicy decides what an undeclared column means. The zero value is
	// the historical behavior: every column of an exported table is exported.
	FieldPolicy schemaexport.FieldPolicy
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
	if err := schemaexport.ValidateTableAPINames(tables, schemaexport.TargetOpenAPI); err != nil {
		return Result{}, err
	}
	enums := schemaexport.EnumIndex(db)

	var diagnostics []schemaexport.Diagnostic
	policy := opts.FieldPolicy
	if policy == "" {
		policy = schemaexport.FieldPolicyAll
	}
	schemas := newOrderedMap()
	for _, table := range tables {
		// One schema per table carries both contract directions, so the read
		// shape decides membership and the write-only columns are added to it
		// below. OpenAPI 3.0 says a writeOnly property is still declared and
		// merely not returned, which is why this is a shape declaration rather
		// than a second document.
		fields, exposureDiagnostics, err := schemaexport.ExposedFields(
			db, table, schemaexport.ShapeRead, policy)
		if err != nil {
			return Result{}, err
		}
		writable, _, err := schemaexport.ExposedFields(db, table, schemaexport.ShapeWrite, policy)
		if err != nil {
			return Result{}, err
		}
		fields, writeOnly, readOnlyNames := mergeWriteOnly(fields, writable)
		// The shared model reports per shape, and this target publishes one
		// schema carrying both. A column the read shape withheld but the write
		// shape kept IS in the document, marked writeOnly, so reporting it as
		// omitted would describe something the reader can see is there.
		diagnostics = append(diagnostics, withheldOnly(exposureDiagnostics, fields)...)
		// Refused before anything is written: an alias that shadows another
		// column would drop it from the document, and the reader of the
		// document has nothing left to notice the loss with. It reads the
		// EXPORTED set, so two columns colliding on api_name where a policy
		// publishes only one of them is not a collision.
		if err := schemaexport.ValidateFieldAPINames(table, fields, schemaexport.TargetOpenAPI); err != nil {
			return Result{}, err
		}
		pk := toSet(schemaexport.EffectivePrimaryKey(table, fields))
		obj := &schemaObject{Type: "object"}
		obj.Description = table.Comment
		properties := newOrderedMap()
		var required []string
		for _, field := range fields {
			// The API type is substituted once here, so the mapping, the array
			// detection and the enum lookup below all read one answer.
			projected := schemaexport.ProjectedField(field)
			if err := schemaexport.RefuseUnknownAPIType(
				table, field, schemaexport.TargetOpenAPI, enums,
				func(t string) bool { return mapOpenAPIType(t).Known },
			); err != nil {
				return Result{}, err
			}
			property, diag := columnSchema(table, projected, enums, pk)
			if diag != nil {
				diagnostics = append(diagnostics, *diag)
			}
			// readOnly and writeOnly are OpenAPI's own words for the two
			// directions, so the shape is declared on the property rather than
			// by emitting a second schema. They are set on the property and
			// never on an items sub-schema, where the specification says they
			// mean nothing.
			property.ReadOnly = readOnlyNames[field.Name]
			property.WriteOnly = writeOnly[field.Name]
			apiName := schemaexport.FieldAPIName(field, schemaexport.TargetOpenAPI)
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
		schemas.set(schemaexport.TableAPIName(table, schemaexport.TargetOpenAPI), obj)
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
			Path: "components.schemas." + schemaexport.TableAPIName(table, schemaexport.TargetOpenAPI) +
				".properties." + schemaexport.FieldAPIName(field, schemaexport.TargetOpenAPI),
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
