// Package typechange compares SQL type transitions.
package typechange

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"ptah.run/internal/normalize"
)

var typeArgRe = regexp.MustCompile(`^([a-zA-Z0-9_ ]+)\(([^)]*)\)$`)

// IsNarrowing reports whether changing from oldType to newType can lose data
// by reducing the representable range or length.
//
// A temporal type counts when its precision, its range or the fields it keeps go down.
// A size that only one side declares means what the engine says it means, so without an
// engine that transition is not answered here; see [Engine.Narrows].
func IsNarrowing(oldType, newType string) bool {
	return narrows(oldType, newType, nil)
}

func narrows(oldType, newType string, engine *Engine) bool {
	if oldInt, newInt, ok := fixedWidthIntegers(oldType, newType); ok {
		return fixedWidthNarrows(oldInt, newInt)
	}
	if old, ok := parseTemporal(oldType); ok {
		if updated, ok := parseTemporal(newType); ok {
			return temporalNarrows(old, updated, engine)
		}
	}
	oldSpec, newSpec, ok := parseElementSpecs(oldType, newType)
	if !ok {
		return false
	}
	if isTextType(oldSpec.name) && isSizedString(newSpec.name) {
		// A bare VARCHAR has no limit, so a text value keeps every character there.
		newLen, bounded := stringLength(newSpec, engine)
		return !bounded || newLen != unboundedStringLength
	}
	if oldSpec.kind == "string" && newSpec.kind == "string" {
		oldLen, oldOK := stringLength(oldSpec, engine)
		newLen, newOK := stringLength(newSpec, engine)
		if oldOK && newOK {
			return newLen < oldLen
		}
	}
	if oldSpec.kind == "integer" && newSpec.kind == "integer" {
		return integerRank(newSpec.name) < integerRank(oldSpec.name)
	}
	if oldSpec.kind == "decimal" && newSpec.kind == "decimal" {
		return decimalNarrows(oldSpec, newSpec, engine)
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
	return widens(oldType, newType, nil)
}

func widens(oldType, newType string, engine *Engine) bool {
	if oldInt, newInt, ok := fixedWidthIntegers(oldType, newType); ok {
		return oldInt != newInt && !fixedWidthNarrows(oldInt, newInt)
	}
	if old, ok := parseTemporal(oldType); ok {
		if updated, ok := parseTemporal(newType); ok {
			return temporalWidens(old, updated, engine)
		}
	}
	oldSpec, newSpec, ok := parseElementSpecs(oldType, newType)
	if !ok {
		return false
	}
	if oldSpec.kind == "string" && newSpec.kind == "string" {
		oldLen, oldOK := stringLength(oldSpec, engine)
		newLen, newOK := stringLength(newSpec, engine)
		if oldOK && newOK {
			return newLen > oldLen
		}
	}
	if oldSpec.kind == "integer" && newSpec.kind == "integer" {
		return integerRank(newSpec.name) > integerRank(oldSpec.name)
	}
	if oldSpec.kind == "decimal" && newSpec.kind == "decimal" {
		return decimalWidens(oldSpec, newSpec, engine)
	}
	return false
}

// parseElementSpecs reads two types to compare, or their elements if both are arrays. An
// array against its element, and two bare spellings of one name, have no answer.
func parseElementSpecs(oldType, newType string) (oldSpec, newSpec spec, ok bool) {
	oldElement, oldArray := normalize.ArrayElement(normalizeName(oldType))
	newElement, newArray := normalize.ArrayElement(normalizeName(newType))
	if oldArray != newArray {
		return spec{}, spec{}, false
	}
	oldSpec, newSpec = parseSpec(oldElement), parseSpec(newElement)
	switch {
	case oldSpec.name == "" || newSpec.name == "":
		return spec{}, spec{}, false
	case oldSpec.name == newSpec.name && oldSpec.bare() && newSpec.bare():
		return spec{}, spec{}, false
	}
	return oldSpec, newSpec, true
}

// VectorDimensionChange reports a pgvector column whose declared dimension
// changes, and the two dimensions.
//
// It is not a cast and it is not narrowing: it is a different vector space.
// Measured on pgvector 0.8.6 / PostgreSQL 17, `ALTER TABLE t ALTER COLUMN emb
// TYPE vector(1024)` on a column declared `vector(384)`:
//
//	three rows holding 384-dimension vectors -> ERROR: expected 1024 dimensions, not 384
//	no rows, or rows whose value is NULL      -> ALTER TABLE
//
// So the server performs it exactly when there is nothing to convert. Every
// existing vector has to be recomputed by the embedding model that produced it,
// which is not a migration a schema tool can write, and the widening direction
// is no different from the narrowing one -- 384 numbers do not become 1024 by
// padding.
//
// Measured on the same build: `halfvec(3) -> halfvec(8)` answers "expected 8
// dimensions, not 3" and `sparsevec(5) -> sparsevec(9)` answers "expected 9
// dimensions, not 5". `bit` is deliberately absent -- it is not pgvector's, and
// its length is a width rather than a space.
//
// Reported for the sized spellings only, and that exclusion is measured rather
// than conservative. What the server checks is the dimension of the value
// STORED, which an unsized declaration does not record: a `vector` column
// holding a 3-dimension value takes `TYPE vector(3)` and refuses
// `TYPE vector(7)` with "expected 7 dimensions, not 3". The declared types
// alone cannot tell those two apart, so a transition to or from the unsized
// form is not answered here (stokaro/ptah#2068).
func VectorDimensionChange(oldType, newType string) (from, to int, ok bool) {
	oldSpec := parseSpec(oldType)
	newSpec := parseSpec(newType)
	if !isVectorType(oldSpec.name) || !isVectorType(newSpec.name) {
		return 0, 0, false
	}
	if len(oldSpec.args) != 1 || len(newSpec.args) != 1 {
		return 0, 0, false
	}
	if oldSpec.arg == newSpec.arg {
		return 0, 0, false
	}
	return oldSpec.arg, newSpec.arg, true
}

// isVectorType reports pgvector's dimensioned types.
func isVectorType(name string) bool {
	return name == "vector" || name == "halfvec" || name == "sparsevec"
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
	// unlimited reports SQL Server's (MAX).
	unlimited bool
}

// bare reports a type that declares no size.
func (s spec) bare() bool {
	return s.arg == 0 && !s.unlimited
}

func parseSpec(raw string) spec {
	clean := normalizeName(raw)
	if clean == "" {
		return spec{}
	}
	name := clean
	var args []int
	unlimited := false
	if match := typeArgRe.FindStringSubmatch(clean); match != nil {
		name = strings.TrimSpace(match[1])
		args, unlimited = parseTypeArguments(match[2])
	}
	parsed := spec{name: name, args: args, unlimited: unlimited}
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

// oracleStarDigits is the digits Oracle's NUMBER(*,s) holds.
const oracleStarDigits = 38

// parseTypeArguments reads what a type's parentheses hold. Each argument is a number,
// perhaps with Oracle's length semantics behind it (`100 CHAR`), or SQL Server's MAX.
// Oracle's star is its largest precision where a scale follows it: `NUMBER(*)` is a bare
// NUMBER.
func parseTypeArguments(list string) (args []int, unlimited bool) {
	star := false
	for token := range strings.SplitSeq(list, ",") {
		fields := strings.Fields(token)
		switch {
		case len(fields) == 0:
		case fields[0] == "max":
			unlimited = true
		case fields[0] == "*":
			star = true
		default:
			if value, err := strconv.Atoi(fields[0]); err == nil {
				args = append(args, value)
			}
		}
	}
	if star && len(args) > 0 {
		args = append([]int{oracleStarDigits}, args...)
	}
	return args, unlimited
}

func normalizeName(raw string) string {
	clean := strings.ToLower(strings.TrimSpace(raw))
	clean = strings.TrimPrefix(clean, "pg_catalog.")
	clean = strings.ReplaceAll(clean, "character varying", "varchar")
	clean = strings.ReplaceAll(clean, "double precision", "double")
	clean = strings.ReplaceAll(clean, "unsigned", "")
	return strings.Join(strings.Fields(clean), " ")
}

// isSizedString reports whether a type carries a declared character width.
//
// SQL Server's nvarchar is here for the reason the Oracle spellings are: the
// comparison strips a width before it compares two spellings, so a width change
// on a type this function does not name is reported nowhere, and nvarchar(50)
// against nvarchar(70) reads as synced.
//
// The Oracle spellings are here because Oracle is the one dialect whose
// declared type and catalog type are never the same word: a declared
// VARCHAR(200) is stored as VARCHAR2(200). Without them, parseSpec gave a
// VARCHAR2 no kind at all, so IsNarrowing and IsWidening both answered false
// and a width change on an Oracle column -- in either direction -- was never
// reported (stokaro/ptah#1875).
func isSizedString(name string) bool {
	return name == "varchar" || name == "char" || name == "character" || name == "bpchar" ||
		name == "varchar2" || name == "nvarchar2" || name == "nchar" || name == "nvarchar"
}

// isTextType reports whether a type is an unsized character type. CLOB and
// NCLOB are Oracle's, and they are what a declared TEXT becomes there.
func isTextType(name string) bool {
	return name == "text" || name == "tinytext" || name == "mediumtext" || name == "longtext" ||
		name == "clob" || name == "nclob"
}

func integerRank(name string) int {
	switch name {
	case "tinyint":
		return 1
	case "smallint", "int2", "smallserial", "serial2":
		return 2
	case "mediumint":
		return 3
	case "int", "integer", "int4", "serial", "serial4":
		return 4
	case "bigint", "int8", "bigserial", "serial8":
		return 5
	default:
		return 0
	}
}

// fixedWidthIntegerRe matches the integer names ClickHouse and YDB give their
// fixed-width types, in the case both spell them.
//
// The case is the point. Lowered, ClickHouse's 8-bit Int8 is PostgreSQL's
// 64-bit int8, which integerRank ranks as bigint, and Int16, Int32 and Int64
// are not ranked at all. So a ClickHouse column planned from Int64 to Int32 was
// not called a narrowing, although measured on 24.10 and 26.9 the server
// accepts it and stores 30000000000 as -64771072 (stokaro/ptah#4105). Both
// sides have to match: no engine's catalog reports PostgreSQL's int8 as Int8.
var fixedWidthIntegerRe = regexp.MustCompile(`^(U?)Int(8|16|32|64|128|256)$`)

type fixedWidthInteger struct {
	unsigned bool
	bits     int
}

// fixedWidthIntegers parses both types as fixed-width integers, looking inside
// ClickHouse's Nullable(...), which changes what a column admits and not the
// values its type holds.
func fixedWidthIntegers(oldType, newType string) (oldInt, newInt fixedWidthInteger, ok bool) {
	parse := func(raw string) (fixedWidthInteger, bool) {
		raw = strings.TrimSpace(raw)
		if inner, found := strings.CutPrefix(raw, "Nullable("); found {
			raw = strings.TrimSpace(strings.TrimSuffix(inner, ")"))
		}
		match := fixedWidthIntegerRe.FindStringSubmatch(raw)
		if match == nil {
			return fixedWidthInteger{}, false
		}
		bits, _ := strconv.Atoi(match[2])
		return fixedWidthInteger{unsigned: match[1] == "U", bits: bits}, true
	}
	oldInt, oldOK := parse(oldType)
	newInt, newOK := parse(newType)
	return oldInt, newInt, oldOK && newOK
}

// fixedWidthNarrows reports whether some value of oldInt is not a value of
// newInt: a smaller width of the same signedness, any signed type made
// unsigned, and an unsigned type made signed without more bits.
func fixedWidthNarrows(oldInt, newInt fixedWidthInteger) bool {
	switch {
	case oldInt.unsigned == newInt.unsigned:
		return newInt.bits < oldInt.bits
	case !oldInt.unsigned:
		return true
	default:
		return newInt.bits <= oldInt.bits
	}
}

func decimalNarrows(oldSpec, newSpec spec, engine *Engine) bool {
	oldSize, oldOK := decimalSizeOf(oldSpec, engine)
	newSize, newOK := decimalSizeOf(newSpec, engine)
	if !oldOK || !newOK || newSize.unlimited {
		return false
	}
	if oldSize.unlimited {
		return true
	}
	oldIntegerDigits := oldSize.precision - oldSize.scale
	newIntegerDigits := newSize.precision - newSize.scale
	return newSize.precision < oldSize.precision || newSize.scale < oldSize.scale || newIntegerDigits < oldIntegerDigits
}

func decimalWidens(oldSpec, newSpec spec, engine *Engine) bool {
	oldSize, oldOK := decimalSizeOf(oldSpec, engine)
	newSize, newOK := decimalSizeOf(newSpec, engine)
	if !oldOK || !newOK || oldSize.unlimited {
		return false
	}
	if newSize.unlimited {
		return true
	}
	oldIntegerDigits := oldSize.precision - oldSize.scale
	newIntegerDigits := newSize.precision - newSize.scale
	return newSize.precision > oldSize.precision || newSize.scale > oldSize.scale || newIntegerDigits > oldIntegerDigits
}

// decimalSize is what a decimal type holds.
type decimalSize struct {
	precision, scale int
	unlimited        bool
}

// decimalSizeOf reads the size a decimal type declares. A precision given without a scale
// defaults the scale to 0 (NUMERIC(10) is NUMERIC(10,0)), so a later scale addition or
// removal is still a comparable change. A bare NUMERIC holds what the engine says, and ok
// is false where no engine says.
func decimalSizeOf(s spec, engine *Engine) (size decimalSize, ok bool) {
	switch len(s.args) {
	case 0:
		if engine == nil || !engine.Bare.Stated {
			return decimalSize{}, false
		}
		if engine.Bare.DecimalPrecision == 0 {
			return decimalSize{unlimited: true}, true
		}
		return decimalSize{precision: engine.Bare.DecimalPrecision}, true
	case 1:
		return decimalSize{precision: s.args[0]}, true
	default:
		return decimalSize{precision: s.args[0], scale: s.args[1]}, true
	}
}

// unboundedStringLength is the effective length of a bare VARCHAR (no declared
// limit); it compares greater than any real column length.
const unboundedStringLength = math.MaxInt

// stringLength returns the effective character length of a sized-string spec. A declared
// length is used as-is, and SQL Server's (MAX) and PostgreSQL's bare bpchar have no limit.
// A bare VARCHAR has no limit unless the engine says otherwise, and a bare CHAR or NCHAR
// holds one character on every engine. Without an engine, ok is false for a bare CHAR.
func stringLength(s spec, engine *Engine) (length int, ok bool) {
	switch {
	case s.unlimited:
		return unboundedStringLength, true
	case s.arg > 0:
		return s.arg, true
	case s.name == "bpchar":
		return unboundedStringLength, true
	case engine != nil && engine.Bare.Stated:
		switch s.name {
		case "char", "character", "nchar":
			return 1, true
		case "varchar", "nvarchar", "varchar2", "nvarchar2":
			if engine.Bare.VariableLength > 0 {
				return engine.Bare.VariableLength, true
			}
			return unboundedStringLength, true
		}
	case s.name == "varchar":
		return unboundedStringLength, true
	}
	return 0, false
}
