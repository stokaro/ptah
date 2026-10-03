package ydbtype

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/defaultlit"
)

// Literal writes value as the YQL literal a column of ydbType takes as its
// default on a target with caps.
//
// The literal is canonical: two spellings of one value write one literal, so
// `05`, `+5` and `5` are all `5t` on an Int8 column, `PT26H` and `P1DT2H` are
// both `Interval('P1DT2H')`, and `1.50` is `Decimal('1.5', 10, 2)`. The schema
// reader writes a default YDB stores through this function too, which is what
// lets a comparison decide equality by comparing the two literals.
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
	if precision, scale, ok := DecimalArguments(ydbType); ok {
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
	if kind.unsigned {
		number, err := strconv.ParseUint(text, 10, kind.bits)
		if err != nil {
			return "", valueRefusal(ydbType, value)
		}
		return strconv.FormatUint(number, 10) + kind.suffix, nil
	}
	number, err := strconv.ParseInt(text, 10, kind.bits)
	if err != nil {
		return "", valueRefusal(ydbType, value)
	}
	return strconv.FormatInt(number, 10) + kind.suffix, nil
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
// A Float or a Double is written as the shortest text that reads back as the
// same binary value at the column's width; a DyNumber keeps its digits, since
// it is a decimal type and its digits are the value.
func numberConstructor(ydbType, value string) (string, error) {
	text := strings.TrimSpace(value)
	if !decimalNumber.MatchString(text) {
		return "", valueRefusal(ydbType, value)
	}
	switch ydbType {
	case Float, Double:
		bits := 64
		if ydbType == Float {
			bits = 32
		}
		number, err := strconv.ParseFloat(text, bits)
		if err != nil {
			return "", valueRefusal(ydbType, value)
		}
		text = strconv.FormatFloat(number, 'g', -1, bits)
	}
	return ydbType + "(" + quote(text) + ")", nil
}

// DecimalArguments reads the precision and scale out of a YDB type spelled
// `Decimal(p,s)`, and reports false for any other type.
func DecimalArguments(ydbType string) (precision, scale int, ok bool) {
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
	negative := strings.HasPrefix(text, "-")
	integerPart, fraction, _ := strings.Cut(strings.TrimLeft(text, "+-"), ".")
	integerPart = strings.TrimLeft(integerPart, "0")
	fraction = strings.TrimRight(fraction, "0")
	if len(fraction) > scale || len(integerPart) > precision-scale {
		return "", valueRefusal(declared, value)
	}
	return fmt.Sprintf("Decimal(%s, %d, %d)", quote(DecimalText(negative, integerPart, fraction)), precision, scale), nil
}

// DecimalText writes a decimal value from its sign and its digits, with no
// leading zero in the integer part, no trailing zero in the fraction, and no
// sign on zero: `-0012.340` is -, 0012 and 340, and writes as -12.34.
func DecimalText(negative bool, integerDigits, fractionDigits string) string {
	integerDigits = strings.TrimLeft(integerDigits, "0")
	fractionDigits = strings.TrimRight(fractionDigits, "0")
	if integerDigits == "" {
		integerDigits = "0"
	}
	text := integerDigits
	if fractionDigits != "" {
		text += "." + fractionDigits
	}
	if negative && text != "0" {
		text = "-" + text
	}
	return text
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
	micros, ok := parseDuration(text)
	if !ok {
		return "", &Refusal{Declared: fmt.Sprintf("default %q", value),
			Reason: ydbType + " takes an ISO 8601 duration such as P1D or PT30M"}
	}
	return ydbType + "(" + quote(IntervalText(micros)) + ")", nil
}

// parseDuration reads an ISO 8601 duration in the form YDB's Interval takes
// into microseconds. A week is seven days and a day is 24 hours, which is how
// YDB stores them: `Interval('P1DT2H')` reads back as 93600000000.
func parseDuration(text string) (int64, bool) {
	match := isoDuration.FindStringSubmatch(text)
	if match == nil || strings.HasSuffix(text, "P") || strings.HasSuffix(text, "T") {
		return 0, false
	}
	const (
		second = int64(1_000_000)
		minute = 60 * second
		hour   = 60 * minute
		day    = 24 * hour
	)
	var micros int64
	for _, part := range []struct {
		text string
		unit int64
	}{
		{match[1], 7 * day}, {match[2], day}, {match[4], hour}, {match[5], minute},
	} {
		if part.text == "" {
			continue
		}
		count, err := strconv.ParseInt(part.text[:len(part.text)-1], 10, 64)
		if err != nil || count > (math.MaxInt64-micros)/part.unit {
			return 0, false
		}
		micros += count * part.unit
	}
	if seconds := match[6]; seconds != "" {
		whole, fraction, _ := strings.Cut(strings.TrimSuffix(seconds, "S"), ".")
		count, err := strconv.ParseInt(whole, 10, 64)
		if err != nil {
			return 0, false
		}
		fraction += strings.Repeat("0", 6-len(fraction))
		fractionMicros, err := strconv.ParseInt(fraction, 10, 64)
		if err != nil || count > (math.MaxInt64-micros-fractionMicros)/second {
			return 0, false
		}
		micros += count*second + fractionMicros
	}
	if strings.HasPrefix(text, "-") {
		micros = -micros
	}
	return micros, true
}

// IntervalText writes a duration of micros microseconds as the ISO 8601 text
// YDB's Interval reads, in days, hours, minutes and seconds, leaving out the
// parts that are zero: 93600000000 is P1DT2H, -1000000 is -PT1S, and 0 is
// PT0S.
func IntervalText(micros int64) string {
	var b strings.Builder
	magnitude := uint64(micros)
	if micros < 0 {
		b.WriteByte('-')
		magnitude = uint64(-(micros + 1)) + 1
	}
	const (
		second = uint64(1_000_000)
		minute = 60 * second
		hour   = 60 * minute
		day    = 24 * hour
	)
	days, rest := magnitude/day, magnitude%day
	hours, rest := rest/hour, rest%hour
	minutes, rest := rest/minute, rest%minute
	seconds, fraction := rest/second, rest%second

	b.WriteByte('P')
	if days > 0 {
		fmt.Fprintf(&b, "%dD", days)
	}
	if hours == 0 && minutes == 0 && seconds == 0 && fraction == 0 {
		if days == 0 {
			b.WriteString("T0S")
		}
		return b.String()
	}
	b.WriteByte('T')
	if hours > 0 {
		fmt.Fprintf(&b, "%dH", hours)
	}
	if minutes > 0 {
		fmt.Fprintf(&b, "%dM", minutes)
	}
	if seconds > 0 || fraction > 0 {
		fmt.Fprintf(&b, "%d", seconds)
		if fraction > 0 {
			b.WriteString("." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0"))
		}
		b.WriteByte('S')
	}
	return b.String()
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

// DeclaredValue reads a declared default's text into the value it denotes,
// and reports a bare NULL as no value at all. A struct tag stores the value
// bare and the SQL parser keeps a quoted literal, with or without a cast; both
// read to the bare value, so `active`, `'active'` and `'active'::text` are the
// same default. The renderer writes a default from this value and the schema
// comparison reads a declaration through it, so the two cannot disagree about
// what a default says.
func DeclaredValue(text string) (value string, isNull bool) {
	trimmed := strings.TrimSpace(text)
	if !defaultlit.IsSQLLiteral(trimmed) {
		return trimmed, strings.EqualFold(trimmed, "NULL")
	}
	var b strings.Builder
	for i := 1; i < len(trimmed); i++ {
		if trimmed[i] != '\'' {
			b.WriteByte(trimmed[i])
			continue
		}
		if i+1 < len(trimmed) && trimmed[i+1] == '\'' {
			b.WriteByte('\'')
			i++
			continue
		}
		break
	}
	return b.String(), false
}

// LiteralValue reads a literal [Literal] writes back into the value it was
// written from, and reports false for text that is not such a literal: an
// expression, or a literal of a form Literal does not write. Writing the value
// again with Literal and the column's type gives the literal back, which is
// what lets a description of a YDB table carry a stored default as a value a
// declaration could have written.
func LiteralValue(literal string) (string, bool) {
	text := strings.TrimSpace(literal)
	switch {
	case text == "true" || text == "false":
		return text, true
	case integerLiteralPattern.MatchString(text):
		return integerLiteralPattern.FindStringSubmatch(text)[1], true
	case strings.HasPrefix(text, "'"):
		return unquoteLiteral(strings.TrimSuffix(text, "u"))
	}
	name, inner, found := strings.Cut(text, "(")
	if !found || !strings.HasSuffix(inner, ")") {
		return "", false
	}
	inner = strings.TrimSuffix(inner, ")")
	if name == "Decimal" {
		quoted, _, found := strings.Cut(inner, ",")
		if !found {
			return "", false
		}
		return unquoteLiteral(strings.TrimSpace(quoted))
	}
	if !slices.Contains(constructorLiterals, name) {
		return "", false
	}
	return unquoteLiteral(inner)
}

// integerLiteralPattern is an integer literal with YQL's width suffix.
var integerLiteralPattern = regexp.MustCompile(`^(-?\d+)(t|s|l|ut|us|u|ul)?$`)

// constructorLiterals are the types whose literal is the type's constructor
// over a quoted string.
var constructorLiterals = []string{
	Float, Double, DyNumber, JSON, JSONDocument, Yson, UUID,
	Date, Date32, Datetime, Datetime64, Timestamp, Timestamp64, Interval, Interval64,
}

// unquoteLiteral reads a single-quoted YQL string, undoing the escapes quote
// writes. Text with anything after the closing quote is not one string.
func unquoteLiteral(text string) (string, bool) {
	if len(text) < 2 || text[0] != '\'' || text[len(text)-1] != '\'' {
		return "", false
	}
	var b strings.Builder
	body := text[1 : len(text)-1]
	for i := 0; i < len(body); i++ {
		c := body[i]
		switch {
		case c == '\'':
			return "", false
		case c != '\\':
			b.WriteByte(c)
			continue
		case i+1 == len(body):
			return "", false
		}
		i++
		switch body[i] {
		case '\\', '\'', '"':
			b.WriteByte(body[i])
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'x':
			if i+2 >= len(body) {
				return "", false
			}
			value, err := strconv.ParseUint(body[i+1:i+3], 16, 8)
			if err != nil {
				return "", false
			}
			b.WriteByte(byte(value))
			i += 2
		default:
			return "", false
		}
	}
	return b.String(), true
}
