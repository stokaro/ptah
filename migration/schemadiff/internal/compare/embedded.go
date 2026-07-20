package compare

import (
	"github.com/stokaro/ptah/core/goschema"
)

// processEmbeddedFieldsForStruct processes embedded fields for a specific struct and generates corresponding schema fields.
//
// This function implements the core logic for transforming embedded fields into database schema fields
// according to their specified embedding mode. It processes only embedded fields that belong to the
// specified structName.
//
// This is a local implementation to replace the obsolete transform package.
func processEmbeddedFieldsForStruct(embeddedFields []goschema.EmbeddedField, allFields []goschema.Field, structName string) []goschema.Field {
	var generatedFields []goschema.Field

	// Process each embedded field definition
	for _, embedded := range embeddedFields {
		// Filter: only process embedded fields for the target struct
		if embedded.StructName != structName {
			continue
		}

		switch embedded.Mode {
		case "inline":
			// INLINE MODE: Expand embedded struct fields as individual table columns (with recursive support)
			generatedFields = processEmbeddedInlineModeRecursiveForSchemaDiff(generatedFields, embedded, allFields, embeddedFields, structName)
		case "json":
			// JSON MODE: Create a single JSON/JSONB column for the embedded struct
			jsonField := goschema.Field{
				StructName: structName,
				FieldName:  embedded.EmbeddedTypeName,
				Name:       embedded.Name,
				Type:       embedded.Type,
				Nullable:   embedded.Nullable,
				Comment:    embedded.Comment,
			}
			generatedFields = append(generatedFields, jsonField)
		case "relation":
			// RELATION MODE: Create a foreign key field
			// Create platform-specific overrides for MySQL/MariaDB behavior
			overrides := make(map[string]map[string]string)
			overrides["mysql"] = map[string]string{"type": "INT"}
			overrides["mariadb"] = map[string]string{"type": "INT"}

			relationField := goschema.Field{
				StructName: structName,
				FieldName:  embedded.EmbeddedTypeName + "ID",
				Name:       embedded.Field,
				Type:       "INTEGER", // Default to INTEGER for foreign keys
				Foreign:    embedded.Ref,
				OnDelete:   embedded.OnDelete, // Mirror of fromschema.processEmbeddedRelationMode — keeps the diff path in agreement with the generate path on FK actions (#117).
				OnUpdate:   embedded.OnUpdate,
				Comment:    embedded.Comment,
				Overrides:  overrides, // Platform-specific type overrides
			}
			generatedFields = append(generatedFields, relationField)
		case "skip":
			// SKIP MODE: Do nothing - completely ignore this embedded field
			continue
		default:
			// DEFAULT MODE: Fall back to inline behavior for unrecognized modes (with recursive support)
			generatedFields = processEmbeddedInlineModeRecursiveForSchemaDiff(generatedFields, embedded, allFields, embeddedFields, structName)
		}
	}

	return generatedFields
}

// processEmbeddedInlineModeRecursiveForSchemaDiff recursively processes embedded fields in inline mode for schema diff.
// This handles nested embedded structs by recursively expanding embedded fields within embedded types.
func processEmbeddedInlineModeRecursiveForSchemaDiff(generatedFields []goschema.Field, embedded goschema.EmbeddedField, allFields []goschema.Field, allEmbeddedFields []goschema.EmbeddedField, structName string) []goschema.Field {
	// Step 1: Add direct fields from the embedded type
	for _, field := range allFields {
		if field.StructName != embedded.EmbeddedTypeName {
			continue
		}
		// Clone the field and reassign to target struct
		newField := field
		newField.StructName = structName

		// Apply prefix to column name if specified
		if embedded.Prefix != "" {
			newField.Name = embedded.Prefix + field.Name
		}

		generatedFields = append(generatedFields, newField)
	}

	// Step 2: Recursively process embedded fields within the embedded type
	for _, nestedEmbedded := range allEmbeddedFields {
		if nestedEmbedded.StructName != embedded.EmbeddedTypeName {
			continue
		}

		// Only process inline mode embedded fields recursively
		if nestedEmbedded.Mode == "inline" {
			// Create a new embedded field with the target struct name and combined prefix
			recursiveEmbedded := nestedEmbedded
			recursiveEmbedded.StructName = structName

			// Combine prefixes: if the parent has a prefix, prepend it to the nested prefix
			if embedded.Prefix != "" {
				if recursiveEmbedded.Prefix != "" {
					recursiveEmbedded.Prefix = embedded.Prefix + recursiveEmbedded.Prefix
				} else {
					recursiveEmbedded.Prefix = embedded.Prefix
				}
			}

			// Recursively process the nested embedded field
			generatedFields = processEmbeddedInlineModeRecursiveForSchemaDiff(generatedFields, recursiveEmbedded, allFields, allEmbeddedFields, structName)
		}
	}

	return generatedFields
}
