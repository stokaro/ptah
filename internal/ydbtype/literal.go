package ydbtype

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/platform/capability"
)

// Literal writes value as the YQL literal a column of ydbType takes as its
// default on a target with caps.
//
// value is the default's text with any SQL quoting already removed: `active`,
// `42`, `2026-01-02 03:04:05`. YDB types a default strictly and refuses one
// whose literal type differs from the column's -- measured, `Int32 DEFAULT
// 5000000000` answers `type mismatch, expected: Int32, actual: Int64` and
// `Float DEFAULT 1.5` answers `expected: Float, actual: Double` -- so every
// literal is written in the column's own type:
//
//   - integers carry YQL's width suffix (`5t`, `5s`, `5`, `5l`, `5ut`, `5us`,
//     `5u`, `5ul`), measured on every line;
//   - Float, Double, Decimal, Uuid, Json, JsonDocument, Yson, DyNumber and the
//     date and time types use the type's constructor over a string, which the
//     server folds to a literal at CREATE TABLE (measured: `Timestamp('...')`
//     reads back as `from_literal`);
//   - String and Utf8 are quoted strings, `'...'` and `'...'u`, with YQL's
//     backslash escapes.
//
// A value the type cannot hold, a type whose literal default the target lacks,
// and a Serial column, which YDB refuses a default on (`Default setting for id
// column is already set: DEFAULT_KIND_SEQUENCE`), are each a *[Refusal].
func Literal(ydbType, value string, caps capability.Capabilities) (string, error) {
	switch ydbType {
	case Int16, Uint16:
		if !caps.Has(capability.SmallIntegerDefaults) {
			return "", &Refusal{Declared: "a default of type " + ydbType, Key: capability.SmallIntegerDefaults,
				Reason: "this line fails such a CREATE TABLE with INTERNAL_ERROR (`Unexpected type slot Int16`)"}
		}
	case JSONDocument, DyNumber:
		if !caps.Has(capability.DocumentTypeDefaults) {
			return "", &Refusal{Declared: "a default of type " + ydbType, Key: capability.DocumentTypeDefaults,
				Reason: "this line answers `Unsupported type of literal: " + ydbType + "`"}
		}
	case Serial, BigSerial, SmallSerial:
		return "", &Refusal{Declared: "a default of type " + ydbType,
			Reason: "a Serial column takes its value from its sequence, and YDB refuses a second default on it"}
	}
	return literalFor(ydbType, value)
}

// literalFor writes the literal once the target is known to take it.
func literalFor(ydbType, value string) (string, error) {
	if suffix, ok := integerSuffixes[ydbType]; ok {
		return integerLiteral(ydbType, value, suffix)
	}
	switch ydbType {
	case Bool:
		return boolLiteral(value)
	case Float, Double, DyNumber:
		return numberConstructor(ydbType, value)
	case String:
		return quote(value), nil
	case Utf8:
		return quote(value) + "u", nil
	case JSON, JSONDocument, Yson:
		return ydbType + "(" + quote(value) + ")", nil
	case UUID:
		return uuidLiteral(value)
	case Date, Date32:
		return dateLiteral(ydbType, value)
	case Datetime, Datetime64:
		return instantLiteral(ydbType, value, false)
	case Timestamp, Timestamp64:
		return instantLiteral(ydbType, value, true)
	case Interval, Interval64:
		return intervalLiteral(ydbType, value)
	}
	if precision, scale, ok := decimalArguments(ydbType); ok {
		return decimalLiteral(value, precision, scale)
	}
	return "", &Refusal{Declared: "a default of type " + ydbType, Reason: "Ptah writes no literal of that type"}
}

// integerSuffixes are YQL's integer literal suffixes, with the bit size and
// signedness each one carries.
var integerSuffixes = map[string]integerKind{
	Int8:   {suffix: "t", bits: 8},
	Int16:  {suffix: "s", bits: 16},
	Int32:  {suffix: "", bits: 32},
	Int64:  {suffix: "l", bits: 64},
	Uint8:  {suffix: "ut", bits: 8, unsigned: true},
	Uint16: {suffix: "us", bits: 16, unsigned: true},
	Uint32: {suffix: "u", bits: 32, unsigned: true},
	Uint64: {suffix: "ul", bits: 64, unsigned: true},
}

type integerKind struct {
	suffix   string
	bits     int
	unsigned bool
}

func integerLiteral(ydbType, value string, kind integerKind) (string, error) {
	text := strings.TrimSpace(value)
	var err error
	if kind.unsigned {
		_, err = strconv.ParseUint(text, 10, kind.bits)
	} else {
		_, err = strconv.ParseInt(text, 10, kind.bits)
	}
	if err != nil {
		return "", valueRefusal(ydbType, value)
	}
	return text + kind.suffix, nil
}

func boolLiteral(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true", "t", "1", "yes", "y", "on":
		return "true", nil
	case "false", "f", "0", "no", "n", "off":
		return "false", nil
	}
	return "", valueRefusal(Bool, value)
}

// decimalNumber is the number spelling YDB's Float, Double and DyNumber
// constructors read. Measured on 26.2.1.14 and 25.1.4.7: `1e3`, `.5`, `5.` and
// `+5` are accepted, and `1_000`, `0x1p-2`, `nan` and `inf` are each refused
// with `Invalid value`, though Go's ParseFloat reads all of them.
var decimalNumber = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$`)

// numberConstructor checks the value is a number YDB reads before wrapping it,
// so a word in a numeric default is refused here rather than by the server.
func numberConstructor(ydbType, value string) (string, error) {
	text := strings.TrimSpace(value)
	if !decimalNumber.MatchString(text) {
		return "", valueRefusal(ydbType, value)
	}
	return ydbType + "(" + quote(text) + ")", nil
}

// decimalArguments reads the precision and scale out of `Decimal(p,s)`.
func decimalArguments(ydbType string) (precision, scale int, ok bool) {
	inner, found := strings.CutPrefix(ydbType, "Decimal(")
	if !found || !strings.HasSuffix(inner, ")") {
		return 0, 0, false
	}
	p, s, found := strings.Cut(strings.TrimSuffix(inner, ")"), ",")
	if !found {
		return 0, 0, false
	}
	precision, errP := strconv.Atoi(p)
	scale, errS := strconv.Atoi(s)
	return precision, scale, errP == nil && errS == nil
}

// decimalLiteral writes `Decimal('v', p, s)` for a value the type holds
// exactly. A value with more fractional digits than the scale, or more integer
// digits than the precision leaves room for, is refused rather than rounded,
// because the rounding would be the server's and not the author's.
func decimalLiteral(value string, precision, scale int) (string, error) {
	text := strings.TrimSpace(value)
	declared := fmt.Sprintf("Decimal(%d,%d)", precision, scale)
	if !decimalPattern.MatchString(text) {
		return "", valueRefusal(declared, value)
	}
	integerPart, fraction, _ := strings.Cut(strings.TrimLeft(text, "+-"), ".")
	integerPart = strings.TrimLeft(integerPart, "0")
	if len(strings.TrimRight(fraction, "0")) > scale || len(integerPart) > precision-scale {
		return "", valueRefusal(declared, value)
	}
	return fmt.Sprintf("Decimal(%s, %d, %d)", quote(text), precision, scale), nil
}

var decimalPattern = regexp.MustCompile(`^[+-]?\d+(\.\d+)?$`)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func uuidLiteral(value string) (string, error) {
	text := strings.TrimSpace(value)
	if !uuidPattern.MatchString(text) {
		return "", valueRefusal(UUID, value)
	}
	return UUID + "(" + quote(strings.ToLower(text)) + ")", nil
}

// narrowStart and narrowEnd bound the 32-bit Date, Datetime and Timestamp.
// Measured on 26.2.1.14 and 25.1.4.7: Date('1970-01-01') and
// Timestamp('2105-12-31T23:59:59.999999Z') are accepted, and Date('1969-12-31')
// and Datetime('2106-01-01T00:00:00Z') answer `Invalid value`. The 64-bit
// types reach far past both ends of what a declared date can spell.
var (
	narrowStart = time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC)
	narrowEnd   = time.Date(2106, time.January, 1, 0, 0, 0, 0, time.UTC)
)

// outOfNarrowRange reports a value a 32-bit date or time type cannot hold.
func outOfNarrowRange(ydbType string, instant time.Time) bool {
	switch ydbType {
	case Date, Datetime, Timestamp:
		return instant.Before(narrowStart) || !instant.Before(narrowEnd)
	default:
		return false
	}
}

func dateLiteral(ydbType, value string) (string, error) {
	text := strings.TrimSpace(value)
	day, err := time.Parse(time.DateOnly, text)
	if err != nil || outOfNarrowRange(ydbType, day) {
		return "", valueRefusal(ydbType, value)
	}
	return ydbType + "(" + quote(text) + ")", nil
}

// instantLayouts are the spellings a declared instant default arrives in: ISO
// 8601 with or without a zone, and SQL's space-separated form. A value without
// a zone is read as UTC, which is the only zone a YDB Timestamp stores.
var instantLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05.999999999",
	time.DateOnly,
}

// instantLiteral writes an instant in UTC with a trailing Z, which is the form
// YDB's constructors read. A Datetime keeps seconds and a Timestamp keeps
// microseconds; a value more precise than its type is refused rather than
// truncated, because the truncated default is not the one declared.
func instantLiteral(ydbType, value string, micros bool) (string, error) {
	text := strings.TrimSpace(value)
	for _, layout := range instantLayouts {
		instant, err := time.Parse(layout, text)
		if err != nil {
			continue
		}
		instant = instant.UTC()
		switch {
		case micros && instant.Nanosecond()%1000 != 0, !micros && instant.Nanosecond() != 0,
			outOfNarrowRange(ydbType, instant):
			return "", valueRefusal(ydbType, value)
		case micros:
			return ydbType + "(" + quote(instant.Format("2006-01-02T15:04:05.999999Z")) + ")", nil
		default:
			return ydbType + "(" + quote(instant.Format("2006-01-02T15:04:05Z")) + ")", nil
		}
	}
	return "", valueRefusal(ydbType, value)
}

// isoDuration is an ISO 8601 duration as YDB's Interval reads it: an optional
// sign, P, and day and time parts. Measured: `Interval('P1DT2H')` and
// `Interval('-PT1S')` read back as 93600000000 and -1000000 microseconds.
var isoDuration = regexp.MustCompile(`^-?P(\d+W)?(\d+D)?(T(\d+H)?(\d+M)?(\d+(\.\d{1,6})?S)?)?$`)

func intervalLiteral(ydbType, value string) (string, error) {
	text := strings.TrimSpace(value)
	if !isoDuration.MatchString(text) || strings.HasSuffix(text, "P") || strings.HasSuffix(text, "T") {
		return "", &Refusal{Declared: fmt.Sprintf("default %q", value),
			Reason: ydbType + " takes an ISO 8601 duration such as P1D or PT30M"}
	}
	return ydbType + "(" + quote(text) + ")", nil
}

func valueRefusal(ydbType, value string) *Refusal {
	return &Refusal{Declared: fmt.Sprintf("default %q", value), Reason: "it is not a " + ydbType + " value"}
}

// quote writes value as a single-quoted YQL string. YQL reads backslash
// escapes in a string and does not read a quote written twice: measured, the
// SQL spelling of "it's" with a doubled quote is a parse error. So a quote and
// a backslash are escaped with a backslash, and a control character is written
// as `\xHH`; `'a\x01b'u` and `'by\x00te'` read back as the bytes they name.
func quote(value string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c == '\\' || c == '\'':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
