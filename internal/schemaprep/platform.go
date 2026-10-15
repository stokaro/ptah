package schemaprep

import (
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
)

// EffectiveFieldForPlatform returns the field after applying the target's
// declared platform overrides and portable type mapping.
func EffectiveFieldForPlatform(field schemamodel.Field, targetPlatform string) schemamodel.Field {
	// A serial type is an auto-increment integer on the targets that write it
	// as a plain one, so the column takes their auto-increment clause with
	// it: written as INT alone, a SERIAL key on MySQL or SQL Server took no
	// value of its own.
	if SerialWritesAutoIncrement(field, targetPlatform) {
		field.AutoInc = true
	}
	fieldType := platformFieldType(field, targetPlatform)
	checkConstraint := field.Check
	checkName := field.CheckName
	comment := field.Comment
	collate := field.Collate
	defaultValue := field.Default
	defaultSet := field.DefaultSet
	defaultExpr := field.DefaultExpr

	if targetPlatform == "" || field.Overrides == nil {
		return fieldWithPlatformValues(
			field, fieldType, checkConstraint, checkName, comment,
			collate, defaultValue, defaultSet, defaultExpr,
		)
	}
	overrides, ok := PlatformOverrideGroup(field.Overrides, targetPlatform)
	if !ok {
		return fieldWithPlatformValues(
			field, fieldType, checkConstraint, checkName, comment,
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
		field, fieldType, checkConstraint, checkName, comment,
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
	fieldType, _ := platformType(field, targetPlatform)
	return fieldType
}

// platformType is the type targetPlatform writes for field's declared type,
// and whether it writes that type, a serial one, as its own auto-increment
// integer. The dialect's descriptor states both
// (descriptor.Preparation.SerialAutoIncrement and TypeSubstitutions). A
// dialect with serial types of its own, such as YDB, keeps the declared type
// for its type map, which writes it as one of those.
func platformType(field schemamodel.Field, targetPlatform string) (fieldType string, serialAutoIncrement bool) {
	if field.TypeRawSQL || field.TypeIsDeclaredText {
		return field.Type, false
	}
	preparation := describe(targetPlatform).preparation
	if preparation.SerialAutoIncrement {
		if integer, ok := serialInteger(field.Type); ok {
			return integer, true
		}
	}
	if substitute, ok := preparation.TypeSubstitutions[field.Type]; ok {
		return substitute, false
	}
	return field.Type, false
}

// serialInteger is the integer type a serial type stands for: SERIAL is INT,
// BIGSERIAL BIGINT and SMALLSERIAL SMALLINT, as PostgreSQL builds them over a
// sequence.
func serialInteger(declared string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(declared)) {
	case "SERIAL":
		return "INT", true
	case "BIGSERIAL":
		return "BIGINT", true
	case "SMALLSERIAL":
		return "SMALLINT", true
	default:
		return "", false
	}
}

// SerialWritesAutoIncrement reports whether targetPlatform writes field's
// serial type as its own auto-increment integer: INT, BIGINT or SMALLINT with
// AUTO_INCREMENT on MySQL and MariaDB, and with IDENTITY(1,1) on SQL Server.
//
// A type written as raw SQL or kept as declared text is the server's to read,
// and a type the field overrides for the target is the one written there, so
// neither is one: `type="SERIAL" platform.mysql.type="INT AUTO_INCREMENT"`
// already says what MySQL gets.
func SerialWritesAutoIncrement(field schemamodel.Field, targetPlatform string) bool {
	if _, serial := platformType(field, targetPlatform); !serial {
		return false
	}
	overrides, _ := PlatformOverrideGroup(field.Overrides, targetPlatform)
	_, typed := overrides["type"]
	return !typed
}

func fieldWithPlatformValues(
	field schemamodel.Field,
	fieldType, checkConstraint, checkName, comment, collate,
	defaultValue string,
	defaultSet bool,
	defaultExpr string,
) schemamodel.Field {
	field.Type = fieldType
	field.Check = checkConstraint
	field.CheckName = checkName
	field.Comment = comment
	field.Collate = collate
	field.Default = defaultValue
	field.DefaultSet = defaultSet
	field.DefaultExpr = defaultExpr
	return field
}

// DeclaresIndexesInCreateTable reports whether a target writes a new table's
// indexes inside its CREATE TABLE rather than as statements after it: the
// targets without a CREATE INDEX statement, [capability.CreateIndexStatement].
//
// YDB is the target that has none. Its only standalone form is ALTER TABLE ...
// ADD INDEX, and that refuses a unique index on every measured line (`Adding a
// unique index to an existing table is disabled`, even on an empty table),
// while the same index inside CREATE TABLE is accepted. Writing every index of
// a new table there keeps one rule for unique and plain indexes alike, and
// makes the table and its indexes one statement.
//
// A nil set, which is what a dialect without a preset has, keeps indexes
// standalone: that is what every target with a CREATE INDEX statement does,
// and the renderer refuses such a dialect anyway.
func DeclaresIndexesInCreateTable(caps capability.Capabilities) bool {
	return caps.Len() != 0 && !caps.Has(capability.CreateIndexStatement)
}
