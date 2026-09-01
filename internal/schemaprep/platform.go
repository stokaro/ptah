package schemaprep

import (
	"slices"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
)

// EffectiveFieldForPlatform returns the field after applying the target's
// declared platform overrides and portable type mapping.
func EffectiveFieldForPlatform(field schemamodel.Field, targetPlatform string) schemamodel.Field {
	fieldType := platformFieldType(field, targetPlatform)
	checkConstraint := field.Check
	checkName := field.CheckName
	comment := field.Comment
	charset := field.Charset
	collate := field.Collate
	defaultValue := field.Default
	defaultSet := field.DefaultSet
	defaultExpr := field.DefaultExpr

	if targetPlatform == "" || field.Overrides == nil {
		return fieldWithPlatformValues(
			field, fieldType, checkConstraint, checkName, comment, charset,
			collate, defaultValue, defaultSet, defaultExpr,
		)
	}
	overrides, ok := PlatformOverrideGroup(field.Overrides, targetPlatform)
	if !ok {
		return fieldWithPlatformValues(
			field, fieldType, checkConstraint, checkName, comment, charset,
			collate, defaultValue, defaultSet, defaultExpr,
		)
	}
	if value, found := overrides["type"]; found {
		fieldType = value
	}
	if value, found := overrides["check"]; found {
		checkConstraint = value
	}
	if value, found := overrides["check_name"]; found {
		checkName = value
	}
	if value, found := overrides["comment"]; found {
		comment = value
	}
	if value, found := overrides["charset"]; found {
		charset = value
	}
	if value, found := overrides["collate"]; found {
		collate = value
	}
	if value, found := overrides["default"]; found {
		defaultValue = value
		defaultSet = true
		defaultExpr = ""
	}
	if value, found := overrides["default_expr"]; found {
		defaultExpr = value
		defaultValue = ""
		defaultSet = false
	}
	return fieldWithPlatformValues(
		field, fieldType, checkConstraint, checkName, comment, charset,
		collate, defaultValue, defaultSet, defaultExpr,
	)
}

// PlatformOverrideGroup returns the deterministic override group for a target
// dialect, including an alias-keyed group for the same canonical engine.
func PlatformOverrideGroup(
	overrides map[string]map[string]string,
	targetPlatform string,
) (map[string]string, bool) {
	if len(overrides) == 0 {
		return nil, false
	}
	canonical := canonicalPlatform(targetPlatform)
	if group, ok := overrides[canonical]; ok {
		return group, true
	}
	candidates := make([]string, 0, len(overrides))
	for key := range overrides {
		if canonicalPlatform(key) == canonical {
			candidates = append(candidates, key)
		}
	}
	if len(candidates) == 0 {
		return nil, false
	}
	slices.Sort(candidates)
	return overrides[candidates[0]], true
}

func canonicalPlatform(targetPlatform string) string {
	if canonical := platform.NormalizeDialect(targetPlatform); canonical != "" {
		return canonical
	}
	return targetPlatform
}

func platformFieldType(field schemamodel.Field, targetPlatform string) string {
	if field.TypeRawSQL || field.TypeIsDeclaredText {
		return field.Type
	}
	switch platform.NormalizeDialect(targetPlatform) {
	case platform.MySQL, platform.MariaDB:
		switch field.Type {
		case "SERIAL":
			return "INT"
		case "BIGSERIAL":
			return "BIGINT"
		}
	case platform.SQLServer:
		switch field.Type {
		case "SERIAL":
			return "INT"
		case "BIGSERIAL":
			return "BIGINT"
		case "TEXT", "VARCHAR":
			return "NVARCHAR(MAX)"
		}
	}
	return field.Type
}

func fieldWithPlatformValues(
	field schemamodel.Field,
	fieldType, checkConstraint, checkName, comment, charset, collate,
	defaultValue string,
	defaultSet bool,
	defaultExpr string,
) schemamodel.Field {
	field.Type = fieldType
	field.Check = checkConstraint
	field.CheckName = checkName
	field.Comment = comment
	field.Charset = charset
	field.Collate = collate
	field.Default = defaultValue
	field.DefaultSet = defaultSet
	field.DefaultExpr = defaultExpr
	return field
}
