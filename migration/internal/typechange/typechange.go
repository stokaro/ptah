// Package typechange compares SQL type transitions.
package typechange

import (
	"math"
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
	if oldSpec.kind == "string" && newSpec.kind == "string" {
		oldLen, oldOK := stringLength(oldSpec)
		newLen, newOK := stringLength(newSpec)
		if oldOK && newOK {
			return newLen < oldLen
		}
	}
	if oldSpec.kind == "integer" && newSpec.kind == "integer" {
		return integerRank(newSpec.name) < integerRank(oldSpec.name)
	}
	if oldSpec.kind == "decimal" && newSpec.kind == "decimal" {
		return decimalNarrows(oldSpec.args, newSpec.args)
	}
	return false
}

// IsWidening reports whether changing from oldType to newType keeps the same
// type category but increases the representable range, length, or precision.
//
// It is the widening counterpart to IsNarrowing. The schema-diff normalizer
// deliberately folds together members of a category (for example INTEGER and
// BIGINT both normalize to "integer", and VARCHAR loses its length) so that
// harmless dialect aliases do not surface as spurious changes. That folding also
// hides genuine widenings — INTEGER -> BIGINT or VARCHAR(50) -> VARCHAR(100) —
// which are real ALTERs that a database built directly from the desired schema
// would apply. Reusing integerRank keeps the alias folding intact: INT, INTEGER
// and INT4 share a rank, so widening compares range, not spelling.
func IsWidening(oldType, newType string) bool {
	oldSpec := parseSpec(oldType)
	newSpec := parseSpec(newType)
	if oldSpec.name == "" || newSpec.name == "" || oldSpec.name == newSpec.name && oldSpec.arg == 0 && newSpec.arg == 0 {
		return false
	}
	if oldSpec.kind == "string" && newSpec.kind == "string" {
		oldLen, oldOK := stringLength(oldSpec)
		newLen, newOK := stringLength(newSpec)
		if oldOK && newOK {
			return newLen > oldLen
		}
	}
	if oldSpec.kind == "integer" && newSpec.kind == "integer" {
		return integerRank(newSpec.name) > integerRank(oldSpec.name)
	}
	if oldSpec.kind == "decimal" && newSpec.kind == "decimal" {
		return decimalWidens(oldSpec.args, newSpec.args)
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
	oldP, oldS, oldOK := decimalPrecisionScale(oldArgs)
	newP, newS, newOK := decimalPrecisionScale(newArgs)
	if !oldOK || !newOK {
		return false
	}
	oldIntegerDigits := oldP - oldS
	newIntegerDigits := newP - newS
	return newP < oldP || newS < oldS || newIntegerDigits < oldIntegerDigits
}

func decimalWidens(oldArgs, newArgs []int) bool {
	oldP, oldS, oldOK := decimalPrecisionScale(oldArgs)
	newP, newS, newOK := decimalPrecisionScale(newArgs)
	if !oldOK || !newOK {
		return false
	}
	oldIntegerDigits := oldP - oldS
	newIntegerDigits := newP - newS
	return newP > oldP || newS > oldS || newIntegerDigits > oldIntegerDigits
}

// decimalPrecisionScale returns the precision and scale carried by a decimal
// type's parsed arguments. A precision given without a scale defaults the scale
// to 0 (NUMERIC(10) is NUMERIC(10,0)), so a later scale addition or removal is
// still a comparable change. ok is false for a bare NUMERIC with no precision at
// all, whose unbounded precision cannot be compared against a sized one.
func decimalPrecisionScale(args []int) (precision, scale int, ok bool) {
	switch len(args) {
	case 0:
		return 0, 0, false
	case 1:
		return args[0], 0, true
	default:
		return args[0], args[1], true
	}
}

// unboundedStringLength is the effective length of a bare VARCHAR (no declared
// limit); it compares greater than any real column length.
const unboundedStringLength = math.MaxInt

// stringLength returns the effective character length of a sized-string spec.
// A declared length is used as-is; a bare VARCHAR is unbounded. ok is false for
// a bare CHAR / CHARACTER, whose implicit length is dialect-defined, so such a
// type is not compared on length.
func stringLength(s spec) (length int, ok bool) {
	if s.arg > 0 {
		return s.arg, true
	}
	if s.name == "varchar" {
		return unboundedStringLength, true
	}
	return 0, false
}
