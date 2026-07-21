package compare

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/normalize"
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
	sqlCommaSpacingPattern = regexp.MustCompile(`\s*,\s*`)
	schemaQualifierPattern = regexp.MustCompile(`\b[a-z_][a-z0-9_]*\.`)
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

func rawDBColumnType(dbCol types.DBColumn) string {
	rawType := strings.TrimSpace(dbCol.ColumnType)
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
	clone := *value
	return &clone
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

// compareNamedItems is a generic helper function that compares two maps of named items
// and returns the names of items that are added (in generated but not in database)
// and removed (in database but not in generated).
//
// This helper eliminates code duplication between Functions and RLSPolicies comparison logic.
func compareNamedItems[T, U any](generated map[string]T, database map[string]U) (added, removed []string) {
	// Find added items (in generated but not in database)
	for name := range generated {
		if _, exists := database[name]; !exists {
			added = append(added, name)
		}
	}

	// Find removed items (in database but not in generated)
	for name := range database {
		if _, exists := generated[name]; !exists {
			removed = append(removed, name)
		}
	}

	return added, removed
}
