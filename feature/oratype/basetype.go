package oratype

import (
	"regexp"
	"strings"
)

// sameBaseType reports whether a declared domain base type and the one the
// catalog reports are one type.
//
// Oracle stores a domain's base type in its own spelling, and an alias the
// declaration wrote does not come back; see [canonicalType] for the
// spellings. A length the declaration gives in characters agrees with a bare
// length of the same number: ALL_DOMAIN_COLS has no CHAR_USED, so the catalog
// cannot always show the unit, for instance on a single-byte database or at
// VARCHAR2(4000 CHAR). A catalog spelling that names the unit is compared as
// it is.
func sameBaseType(declared, held string) bool {
	declared, held = canonicalType(declared), canonicalType(held)
	if declared == held {
		return true
	}
	return !strings.HasSuffix(held, " CHAR)") && strings.HasSuffix(declared, " CHAR)") &&
		strings.TrimSuffix(declared, " CHAR)")+")" == held
}

// sameAttributeType reports whether a declared object type attribute type and
// the one the catalog reports are one type, once both are reduced by
// [canonicalType]. ALL_TYPE_ATTRS reports the length unit in CHAR_USED, so a
// length in characters and one in bytes always differ here.
func sameAttributeType(declared, held string) bool {
	return canonicalType(declared) == canonicalType(held)
}

var (
	// baseTypeSpace matches the spaces around a parenthesis or a comma.
	baseTypeSpace = regexp.MustCompile(`\s*([(,])\s*|\s+(\))`)
	numberType    = regexp.MustCompile(`^(?:NUMBER|DECIMAL|NUMERIC)\((\*|\d+)(?:,(-?\d+))?\)$`)
	timestampType = regexp.MustCompile(`^TIMESTAMP( WITH (?:LOCAL )?TIME ZONE)?$`)
	dayInterval   = regexp.MustCompile(`^INTERVAL DAY(\(\d+\))? TO SECOND(\(\d+\))?$`)
	yearInterval  = regexp.MustCompile(`^INTERVAL YEAR(\(\d+\))? TO MONTH$`)
	varcharType   = regexp.MustCompile(`^VARCHAR2?\((\d+)( BYTE| CHAR)?\)$`)
	charType      = regexp.MustCompile(`^CHAR(?:\((\d+)( BYTE| CHAR)?\))?$`)
)

// fixedBaseTypes are the spellings the catalog reports as another one.
var fixedBaseTypes = map[string]string{
	"INTEGER":          "NUMBER(*,0)",
	"INT":              "NUMBER(*,0)",
	"SMALLINT":         "NUMBER(*,0)",
	"NUMERIC":          "NUMBER(*,0)",
	"DECIMAL":          "NUMBER(*,0)",
	"FLOAT":            "FLOAT(126)",
	"DOUBLE PRECISION": "FLOAT(126)",
	"REAL":             "FLOAT(63)",
}

// canonicalType is a type in one spelling for every alias Oracle accepts for
// it: upper case, one space between words and none around a parenthesis or a
// comma, and an alias replaced by the spelling ALL_DOMAIN_COLS reports for it.
// A spelling it does not know is returned in that form, so it is compared as
// written.
//
// Measured on 23.26.3.0.0, ALL_DOMAIN_COLS reports INTEGER, INT, SMALLINT,
// NUMERIC and DECIMAL as NUMBER(*,0), DECIMAL(10,2) as NUMBER(10,2),
// NUMBER(10,0) as NUMBER(10), FLOAT and DOUBLE PRECISION as FLOAT(126), REAL as
// FLOAT(63), TIMESTAMP as TIMESTAMP(6) with or without a time zone, INTERVAL
// DAY TO SECOND as INTERVAL DAY(2) TO SECOND(6), INTERVAL YEAR TO MONTH as
// INTERVAL YEAR(2) TO MONTH, VARCHAR(10) and VARCHAR2(10 BYTE) as VARCHAR2(10),
// and CHAR as CHAR(1). ALL_TYPE_ATTRS keeps some aliases instead -- INTEGER,
// SMALLINT, DECIMAL, REAL, DOUBLE PRECISION and FLOAT come back by those names
// -- and reduces to the same spelling here.
func canonicalType(spelling string) string {
	canonical := strings.ToUpper(strings.Join(strings.Fields(spelling), " "))
	canonical = baseTypeSpace.ReplaceAllString(canonical, "$1$2")
	if fixed, ok := fixedBaseTypes[canonical]; ok {
		return fixed
	}
	if match := numberType.FindStringSubmatch(canonical); match != nil {
		precision, scale := match[1], match[2]
		if scale == "" || (scale == "0" && precision != "*") {
			return "NUMBER(" + precision + ")"
		}
		return "NUMBER(" + precision + "," + scale + ")"
	}
	if match := timestampType.FindStringSubmatch(canonical); match != nil {
		return "TIMESTAMP(6)" + match[1]
	}
	if match := dayInterval.FindStringSubmatch(canonical); match != nil {
		return "INTERVAL DAY" + orDefault(match[1], "(2)") + " TO SECOND" + orDefault(match[2], "(6)")
	}
	if match := yearInterval.FindStringSubmatch(canonical); match != nil {
		return "INTERVAL YEAR" + orDefault(match[1], "(2)") + " TO MONTH"
	}
	if match := varcharType.FindStringSubmatch(canonical); match != nil {
		return "VARCHAR2(" + match[1] + charUnit(match[2]) + ")"
	}
	if match := charType.FindStringSubmatch(canonical); match != nil {
		return "CHAR(" + orDefault(match[1], "1") + charUnit(match[2]) + ")"
	}
	return canonical
}

// orDefault is value, or fallback where value is empty.
func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// charUnit keeps a length unit of characters and drops one of bytes, which is
// what a length without a unit means.
func charUnit(unit string) string {
	if unit == " CHAR" {
		return unit
	}
	return ""
}
