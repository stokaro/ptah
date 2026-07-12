// Package typechange compares SQL type transitions.
package typechange

import (
	"regexp"
	"strconv"
	"strings"
)

var typeArgRe = regexp.MustCompile(`^([a-zA-Z0-9_ ]+)\(([^)]*)\)$`)

// IsNarrowing reports whether changing from oldType to newType can lose data
// by reducing the representable range or length.
func IsNarrowing(oldType, newType string) bool {
	oldSpec := parseSpec(oldType)
	newSpec := parseSpec(newType)
	if oldSpec.name == "" || newSpec.name == "" || oldSpec.name == newSpec.name && oldSpec.arg == 0 && newSpec.arg == 0 {
		return false
	}
	if isTextType(oldSpec.name) && isSizedString(newSpec.name) {
		return true
	}
	if oldSpec.kind == "string" && newSpec.kind == "string" && oldSpec.arg > 0 && newSpec.arg > 0 {
		return newSpec.arg < oldSpec.arg
	}
	if oldSpec.kind == "integer" && newSpec.kind == "integer" {
		return integerRank(newSpec.name) < integerRank(oldSpec.name)
	}
	if oldSpec.kind == "decimal" && newSpec.kind == "decimal" {
		return decimalNarrows(oldSpec.args, newSpec.args)
	}
	return false
}

// Same reports whether two type names normalize to the same semantic type.
func Same(left, right string) bool {
	return normalizeName(left) == normalizeName(right)
}

type spec struct {
	name string
	kind string
	arg  int
	args []int
}

func parseSpec(raw string) spec {
	clean := normalizeName(raw)
	if clean == "" {
		return spec{}
	}
	name := clean
	var args []int
	if match := typeArgRe.FindStringSubmatch(clean); match != nil {
		name = strings.TrimSpace(match[1])
		for token := range strings.SplitSeq(match[2], ",") {
			value, err := strconv.Atoi(strings.TrimSpace(token))
			if err == nil {
				args = append(args, value)
			}
		}
	}
	parsed := spec{name: name, args: args}
	if len(args) > 0 {
		parsed.arg = args[0]
	}
	switch {
	case isSizedString(name), isTextType(name):
		parsed.kind = "string"
	case integerRank(name) > 0:
		parsed.kind = "integer"
	case name == "numeric" || name == "decimal" || name == "number":
		parsed.kind = "decimal"
	}
	return parsed
}

func normalizeName(raw string) string {
	clean := strings.ToLower(strings.TrimSpace(raw))
	clean = strings.TrimPrefix(clean, "pg_catalog.")
	clean = strings.ReplaceAll(clean, "character varying", "varchar")
	clean = strings.ReplaceAll(clean, "double precision", "double")
	clean = strings.ReplaceAll(clean, "unsigned", "")
	return strings.Join(strings.Fields(clean), " ")
}

func isSizedString(name string) bool {
	return name == "varchar" || name == "char" || name == "character"
}

func isTextType(name string) bool {
	return name == "text" || name == "tinytext" || name == "mediumtext" || name == "longtext"
}

func integerRank(name string) int {
	switch name {
	case "tinyint":
		return 1
	case "smallint", "int2":
		return 2
	case "mediumint":
		return 3
	case "int", "integer", "int4":
		return 4
	case "bigint", "int8":
		return 5
	default:
		return 0
	}
}

func decimalNarrows(oldArgs, newArgs []int) bool {
	if len(oldArgs) == 0 || len(newArgs) == 0 {
		return false
	}
	if newArgs[0] < oldArgs[0] {
		return true
	}
	if len(oldArgs) < 2 || len(newArgs) < 2 {
		return false
	}
	oldScale := oldArgs[1]
	newScale := newArgs[1]
	oldIntegerDigits := oldArgs[0] - oldScale
	newIntegerDigits := newArgs[0] - newScale
	return newScale < oldScale || newIntegerDigits < oldIntegerDigits
}
