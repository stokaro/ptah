package typechange

import (
	"fmt"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/normalize"
)

// Engine says what a type that declares no size holds on one engine, which is all a
// transition needs that its two spellings do not say. The zero value states nothing about
// a bare type but its temporal digits.
type Engine struct {
	// TimeDigits is the fractional-second digits of a temporal type that declares none.
	TimeDigits int
	// Bare is what the engine's descriptor says about every other type.
	Bare descriptor.BareTypes
}

// Narrows is [IsNarrowing], as the engine reads a type that declares no size. It also
// answers a transition that declares a size on one side only, which [IsNarrowing] cannot.
func (e Engine) Narrows(oldType, newType string) bool {
	return narrows(oldType, newType, &e)
}

// Widens is [IsWidening], as the engine reads a type that declares no size.
func (e Engine) Widens(oldType, newType string) bool {
	return widens(oldType, newType, &e)
}

// Same reports that two spellings name one type, where a size one of them leaves out is
// the size the other declares: `char` and `char(1)`, or `float` and `float(53)` where
// that is what a bare FLOAT holds. It answers false for a type whose size this package
// does not read, which says nothing about whether the two differ.
func (e Engine) Same(left, right string) bool {
	if !e.Bare.Stated {
		return false
	}
	leftSize, leftOK := e.resolvedSize(left)
	rightSize, rightOK := e.resolvedSize(right)
	return leftOK && rightOK && leftSize == rightSize
}

// resolvedSize is a type as what it holds rather than how it is spelled, so that two
// spellings of one type read alike. An array is its element and a marker. It is false
// for a type with no size to resolve.
func (e Engine) resolvedSize(raw string) (string, bool) {
	clean := normalizeName(raw)
	if interval, ok := e.resolvedInterval(clean); ok {
		return interval, true
	}
	element, isArray := normalize.ArrayElement(clean)
	resolved, ok := e.resolvedElement(parseSpec(element))
	if isArray {
		resolved += "[]"
	}
	return resolved, ok
}

func (e Engine) resolvedElement(s spec) (string, bool) {
	switch {
	case s.kind == "decimal":
		size, ok := decimalSizeOf(s, &e)
		if !ok || size.unlimited {
			return "decimal", ok
		}
		return fmt.Sprintf("decimal(%d,%d)", size.precision, size.scale), true
	case isSizedString(s.name):
		length, ok := stringLength(s, &e)
		return fmt.Sprintf("%s(%d)", stringFamily(s.name), length), ok
	case s.kind == "binary" || isBitType(s.name):
		return e.resolvedBytes(s)
	case s.name == "year":
		return "year", true
	case isTextType(s.name):
		tier, ok := lobTier(s, spec{}, &e)
		return fmt.Sprintf("text(%d)", tier), ok
	}
	if bits, ok := e.floatBits(s); ok {
		return fmt.Sprintf("float(%d)", bits), true
	}
	return "", false
}

// resolvedBytes reads the binary and bit types, whose length holds what a string's does.
// Every tiered name is a blob, where a length keeps the name it was declared with.
func (e Engine) resolvedBytes(s spec) (string, bool) {
	var length int
	var ok bool
	family := s.name
	if isBitType(s.name) {
		length, ok = bitLength(s, &e)
		if family == "bit varying" {
			family = "varbit"
		}
	} else {
		length, ok = binaryLength(s, spec{}, &e)
		if _, tiered := lobTier(s, spec{}, &e); tiered {
			family = "blob"
		}
	}
	return fmt.Sprintf("%s(%d)", family, length), ok
}

// stringFamily folds the names of one string type. PostgreSQL's catalog calls `char(1)`
// `bpchar(1)`, and a bare bpchar differs from a bare char by the length it resolves to.
// NVARCHAR stands with VARCHAR as the comparison already folds it, and NCHAR stands alone
// as it does there.
func stringFamily(name string) string {
	switch name {
	case "char", "character", "bpchar":
		return "char"
	case "varchar", "nvarchar", "varchar2", "nvarchar2":
		return "varchar"
	default:
		return name
	}
}

// resolvedInterval reads an interval as the digits the engine gives it, so Oracle's
// INTERVAL DAY TO SECOND is the DAY(2) TO SECOND(6) its catalog reports.
func (e Engine) resolvedInterval(clean string) (string, bool) {
	if !strings.HasPrefix(clean, "interval") {
		return "", false
	}
	interval, ok := parseTemporal(clean)
	if !ok || interval.family != familyInterval {
		return "", false
	}
	leading, _ := interval.resolvedLeading(&e)
	digits, _ := interval.resolvedDigits(&e)
	return fmt.Sprintf("interval(%d,%d,%d)", interval.smallest, leading, digits), true
}

// floatBits is the mantissa of a floating-point type, as the engine reads a name it
// spells its own way. MySQL's FLOAT(10,2) declares digits rather than bits and has no
// answer here.
func (e Engine) floatBits(s spec) (int, bool) {
	const singleBits, doubleBits = 24, 53
	switch s.name {
	case "float4", "float32", "binary_float":
		return singleBits, true
	case "float8", "float64", "binary_double", "double":
		return doubleBits, len(s.args) == 0
	case "real":
		return e.Bare.RealBits, e.Bare.RealBits > 0
	case "float":
		switch {
		case !e.Bare.Stated:
			return 0, false
		case len(s.args) == 0:
			return e.Bare.FloatBits, e.Bare.FloatBits > 0
		case len(s.args) > 1:
			return 0, false
		case e.Bare.FloatKeepsPrecision:
			return s.arg, true
		case s.arg <= singleBits:
			return singleBits, true
		default:
			return doubleBits, true
		}
	default:
		return 0, false
	}
}
