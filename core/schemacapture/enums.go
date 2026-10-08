package schemacapture

import "ptah.run/core/schemamodel"

// EnumsFor returns independent enum declarations in first-reference order.
// Each enum name occurs at most once. Unmatched field types contribute nothing;
// no matched enum returns nil. The inputs remain unchanged.
func EnumsFor(fields []schemamodel.Field, enums []schemamodel.Enum) []schemamodel.Enum {
	var needed []schemamodel.Enum
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		enum := declaredEnum(field.Type, enums)
		if enum == nil || seen[enum.Name] {
			continue
		}
		seen[enum.Name] = true
		needed = append(needed, enum.Clone())
	}
	return needed
}

func declaredEnum(fieldType string, enums []schemamodel.Enum) *schemamodel.Enum {
	for i := range enums {
		if enums[i].Name == fieldType {
			return &enums[i]
		}
	}
	return nil
}
