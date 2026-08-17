package compare

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/normalize"
)

// Regular expressions for constraint-based index detection
var (
	// PostgreSQL constraint-based unique index pattern: tablename_columnname_key
	postgresConstraintPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*_[a-zA-Z_][a-zA-Z0-9_]*_key$`)

	// MySQL/MariaDB constraint-based unique index patterns
	mysqlUKPattern           = regexp.MustCompile(`^uk_[a-zA-Z_][a-zA-Z0-9_]*`)
	mysqlTableColumnsPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*_[a-zA-Z_][a-zA-Z0-9_]*$`)

	// Custom index patterns (these should NOT be considered constraint-based)
	// Match indexes that start with "idx_" or "index_", or end with "_idx" or "_index"
	customIndexPattern = regexp.MustCompile(`(?i)(^(idx|index)_|_(idx|index)$)`)

	defaultAggregateAliasPattern       = regexp.MustCompile(`\b(count|sum|avg|min|max)\(([^)]*)\)\s+as\s+([a-z_][a-z0-9_]*)\b`)
	defaultColumnAliasPattern          = regexp.MustCompile(`\b([a-z_][a-z0-9_]*)\s+as\s+([a-z_][a-z0-9_]*)\b`)
	simpleComparisonParenthesesPattern = regexp.MustCompile(
		`\(([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*\s*` +
			`(?:=|<>|!=|<=|>=|<|>|like|is(?:\s+not)?)\s*` +
			`(?:[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*|[0-9]+(?:\.[0-9]+)?|'[^']*'|true|false|null))\)`,
	)
)

func nonEmptyNames(names []string) []string {
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if name := strings.TrimSpace(name); name != "" {
			filtered = append(filtered, name)
		}
	}
	return filtered
}

// rawDBColumnType is the type spelling the comparator holds the desired schema
// against.
//
// FormattedType comes first because it is the only field that survives an array
// or a domain, and because the desired side reads the same field -- see
// goSchemaFieldType in internal/convert/dbschematogo. The reader fills it from
// the server's own format_type for exactly those two shapes and leaves it empty
// for every other column.
//
// With ColumnType and UDTName first the two sides read different fields for the
// same column, and the comparator reported a change between a database and
// ITSELF. Measured on PostgreSQL 17, `ptah-compat schema diff` with --from and
// --to naming one database, seven phantom rows:
//
//	arrays.a_bit          type: _bit    -> bit(8)[]
//	arrays.a_char         type: _bpchar -> character(5)[]
//	arrays.a_cube         type: _cube   -> cube[]
//	arrays.a_enum         type: _status -> status[]
//	arrays.a_varchar      type: varchar -> character varying(100)[]
//	arrays.a_varchar_dim  type: varchar -> character varying(100)[]
//	scalars.c_tags        type: text    -> tags
//
// Every one of them proposed an ALTER COLUMN ... TYPE to the type the column
// already had. None survive this (stokaro/ptah#1138).
//
// What this string may then be USED for is not uniform, and the difference is
// the whole of #1138's comparator half. An array's spelling is a type. A
// domain's spelling is the identifier its author chose, and columnTypeChange
// keeps it away from normalize.Type for that reason.
func rawDBColumnType(dbCol types.DBColumn) string {
	rawType := strings.TrimSpace(dbCol.FormattedType)
	if rawType == "" {
		rawType = strings.TrimSpace(dbCol.ColumnType)
	}
	if rawType == "" && dbCol.UDTName != "" {
		rawType = strings.TrimSpace(dbCol.UDTName)
	}
	if rawType == "" {
		rawType = strings.TrimSpace(dbCol.DataType)
	}

	if strings.Contains(rawType, "(") {
		return rawType
	}
	switch normalize.Type(rawType) {
	case "varchar":
		if dbCol.CharacterMaxLength != nil {
			return fmt.Sprintf("%s(%d)", rawType, *dbCol.CharacterMaxLength)
		}
	case "decimal":
		if dbCol.NumericPrecision == nil {
			return rawType
		}
		if dbCol.NumericScale != nil {
			return fmt.Sprintf("%s(%d,%d)", rawType, *dbCol.NumericPrecision, *dbCol.NumericScale)
		}
		return fmt.Sprintf("%s(%d)", rawType, *dbCol.NumericPrecision)
	}
	return rawType
}

func stringSetsEqual(left, right []string) bool {
	left = uniqueStringsPreserveOrder(left)
	right = uniqueStringsPreserveOrder(right)
	sort.Strings(left)
	sort.Strings(right)
	return slices.Equal(left, right)
}

func boolPtrEqual(left, right *bool) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	return new(*value)
}

func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
