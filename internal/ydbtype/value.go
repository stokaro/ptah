package ydbtype

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// ValueError says why a value cannot be written into a column of a YDB type.
type ValueError struct {
	// Type is the column's YDB type.
	Type string
	// Value is the value as a refusal prints it: a string quoted, anything
	// else as Go prints it.
	Value string
	// Reason says what does not fit.
	Reason string
}

// Error renders the refusal.
func (e *ValueError) Error() string {
	return fmt.Sprintf("%s cannot hold %s: %s", e.Type, e.Value, e.Reason)
}

// ValueLiteral writes value as the YQL literal a column of ydbType stores it
// from: the literal a data statement -- an INSERT, an UPDATE, a key in a WHERE
// clause -- writes for that column.
//
// YQL types every literal and converts few of them: measured on 26.2.1.14 and
// 25.1.4.7, an `UPDATE ... ON SELECT` refuses an untyped string for a Utf8
// column (`String to Optional<Utf8>`), and an Int64 value is refused by an
// Int32 column. So the literal is written in the column's own type, the way
// [Literal] writes a default: an integer with its width suffix, a Utf8 string
// as `'...'u`, a moment as `Timestamp('...')`.
//
// value is a Go value as a declaration or a read hands it over: nil, a string
// or a []byte, a bool, a Go integer or float, a time.Time, or a time.Duration.
// A nil value is NULL. A string is read as the column's type reads text, so
// `'2026-01-02'` fills a Date column and `'12.50'` a Decimal one. A []byte is
// the bytes of a String or Yson column, and the text of any other. A Serial
// column stores its integer type.
//
// A value the column cannot hold -- an integer outside the type's range, a
// negative one for an unsigned type, a moment more precise than the type, or a
// Go type with no literal for the column -- is a *[ValueError] naming the type
// and the value. It is never wrapped, rounded or truncated, because the value
// the server would then store is not the one given.
func ValueLiteral(ydbType string, value any) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	columnType := storageType(ydbType)
	switch typed := value.(type) {
	case string:
		return textValueLiteral(columnType, typed, fmt.Sprintf("%q", typed))
	case []byte:
		return textValueLiteral(columnType, string(typed), fmt.Sprintf("%q", typed))
	case bool:
		if columnType != Bool {
			return "", mismatch(columnType, value)
		}
		return strconv.FormatBool(typed), nil
	case time.Time:
		return timeValueLiteral(columnType, typed)
	case time.Duration:
		return durationValueLiteral(columnType, typed)
	}
	if number, ok := integerValue(value); ok {
		return integerValueLiteral(columnType, number, value)
	}
	if number, bits, ok := floatValue(value); ok {
		return floatValueLiteral(columnType, number, bits, value)
	}
	return "", &ValueError{Type: columnType, Value: fmt.Sprintf("%v", value),
		Reason: fmt.Sprintf("Ptah writes no YQL literal for a Go %T", value)}
}

// storageType is the type a column of ydbType stores its values as: a Serial
// column holds the integer its sequence fills.
func storageType(ydbType string) string {
	switch ydbType {
	case Serial:
		return Int32
	case BigSerial:
		return Int64
	case SmallSerial:
		return Int16
	default:
		return ydbType
	}
}

func mismatch(ydbType string, value any) *ValueError {
	return &ValueError{Type: ydbType, Value: fmt.Sprintf("%v", value),
		Reason: fmt.Sprintf("it is a Go %T, which is not a %s value", value, ydbType)}
}

// textValueLiteral writes text for a column. String and Yson hold bytes, and
// their literal escapes every byte outside printable ASCII, so any bytes reach
// the server in a query text that is valid UTF-8. Every other type reads the
// text as its own spelling of a value.
func textValueLiteral(ydbType, text, shown string) (string, error) {
	switch ydbType {
	case String:
		return quoteBytes(text), nil
	case Yson:
		return Yson + "(" + quoteBytes(text) + ")", nil
	case Utf8, JSON, JSONDocument:
		if !utf8.ValidString(text) {
			return "", &ValueError{Type: ydbType, Value: shown, Reason: "it is not valid UTF-8"}
		}
	}
	literal, err := literalFor(ydbType, text)
	if refusal, ok := errors.AsType[*Refusal](err); ok {
		return "", &ValueError{Type: ydbType, Value: shown, Reason: refusal.Reason}
	}
	return literal, err
}

// quoteBytes writes text as a single-quoted YQL String literal whose every
// byte outside printable ASCII is an \xHH escape.
func quoteBytes(text string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case c == '\\' || c == '\'':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20 || c >= 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('\'')
	return b.String()
}

// integerValue reads a Go integer of any width as a big integer, so an int64
// and a uint64 compare against one range.
func integerValue(value any) (*big.Int, bool) {
	switch typed := value.(type) {
	case int:
		return big.NewInt(int64(typed)), true
	case int8:
		return big.NewInt(int64(typed)), true
	case int16:
		return big.NewInt(int64(typed)), true
	case int32:
		return big.NewInt(int64(typed)), true
	case int64:
		return big.NewInt(typed), true
	case uint:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint8:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint16:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint32:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint64:
		return new(big.Int).SetUint64(typed), true
	default:
		return nil, false
	}
}

// integerValueLiteral writes an integer for a column: with the width suffix
// for an integer type, inside the type's range, and as text for a type that
// reads a number from text.
func integerValueLiteral(ydbType string, number *big.Int, value any) (string, error) {
	if kind, ok := integerSuffixes[ydbType]; ok {
		low, high := integerRange(kind)
		if number.Cmp(low) < 0 || number.Cmp(high) > 0 {
			return "", &ValueError{Type: ydbType, Value: number.String(),
				Reason: fmt.Sprintf("it is outside the range %s to %s", low, high)}
		}
		return number.String() + kind.suffix, nil
	}
	switch ydbType {
	case Float, Double, DyNumber:
		return numberConstructor(ydbType, number.String())
	}
	if _, _, ok := DecimalArguments(ydbType); ok {
		return textValueLiteral(ydbType, number.String(), number.String())
	}
	return "", mismatch(ydbType, value)
}

// integerRange is the smallest and the largest value an integer kind holds.
func integerRange(kind integerKind) (low, high *big.Int) {
	one := big.NewInt(1)
	if kind.unsigned {
		return big.NewInt(0), new(big.Int).Sub(new(big.Int).Lsh(one, uint(kind.bits)), one)
	}
	limit := new(big.Int).Lsh(one, uint(kind.bits-1))
	return new(big.Int).Neg(limit), new(big.Int).Sub(limit, one)
}

// floatValue reads a Go float and its width.
func floatValue(value any) (float64, int, bool) {
	switch typed := value.(type) {
	case float32:
		return float64(typed), 32, true
	case float64:
		return typed, 64, true
	default:
		return 0, 0, false
	}
}

// floatValueLiteral writes a float for a Float, Double, DyNumber or Decimal
// column. A float has no literal in an integer column, even an integral one: a
// declaration that writes 5.0 for an integer column wrote the wrong type.
func floatValueLiteral(ydbType string, number float64, bits int, value any) (string, error) {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return "", &ValueError{Type: ydbType, Value: fmt.Sprintf("%v", number),
			Reason: "YDB reads no literal for it"}
	}
	switch ydbType {
	case Float, Double, DyNumber:
		return numberConstructor(ydbType, strconv.FormatFloat(number, 'g', -1, bits))
	}
	if _, _, ok := DecimalArguments(ydbType); ok {
		text := strconv.FormatFloat(number, 'f', -1, bits)
		return textValueLiteral(ydbType, text, text)
	}
	return "", mismatch(ydbType, value)
}

// timeValueLiteral writes a moment for a date or time column, in UTC, which is
// the only zone YDB stores. A Date column holds a day, so a moment with a time
// of day in UTC is refused rather than cut to its date.
func timeValueLiteral(ydbType string, moment time.Time) (string, error) {
	utc := moment.UTC()
	shown := utc.Format(time.RFC3339Nano)
	switch ydbType {
	case Date, Date32:
		if utc.Hour() != 0 || utc.Minute() != 0 || utc.Second() != 0 || utc.Nanosecond() != 0 {
			return "", &ValueError{Type: ydbType, Value: shown, Reason: "it has a time of day, and a " +
				ydbType + " holds a day"}
		}
		return textValueLiteral(ydbType, utc.Format(time.DateOnly), shown)
	case Datetime, Datetime64, Timestamp, Timestamp64:
		return textValueLiteral(ydbType, shown, shown)
	default:
		return "", mismatch(ydbType, moment)
	}
}

// durationValueLiteral writes a duration for an Interval column. YDB keeps
// microseconds, so a duration with a finer part is refused.
func durationValueLiteral(ydbType string, duration time.Duration) (string, error) {
	switch ydbType {
	case Interval, Interval64:
	default:
		return "", mismatch(ydbType, duration)
	}
	if duration%time.Microsecond != 0 {
		return "", &ValueError{Type: ydbType, Value: duration.String(),
			Reason: "it is finer than the microsecond " + ydbType + " keeps"}
	}
	return ydbType + "(" + quote(IntervalText(duration.Microseconds())) + ")", nil
}

// CanonicalValue returns value as one Go value per value a column of ydbType
// holds, so two spellings of one value compare equal and a declared row pairs
// with the row a read hands back.
//
// A declaration and a read spell a value differently: `12.50` and the 12.5 a
// Decimal reads back as, a moment in UTC and the same moment in the local zone
// the YDB driver returns, `PT26H` and the time.Duration of an Interval, an
// upper-case UUID and the lower-case one YDB stores. Each is written as its
// literal through [ValueLiteral], which is canonical, and read back from it:
//
//   - a signed integer type is an int64 and an unsigned one a uint64;
//   - Bool is a bool, and Float and Double are a float64;
//   - Decimal and DyNumber are their digits as a string;
//   - Utf8, Json and Uuid are a string, and String and Yson a []byte;
//   - JsonDocument is its JSON with sorted keys and no spaces, the form YDB
//     returns it in (measured on 26.2.1.14 and 25.1.4.7: `{"b": 2, "a": 1.50}`
//     reads back as `{"a":1.5,"b":2}`);
//   - the date and time types are a time.Time in UTC, and the interval types a
//     time.Duration.
//
// A nil value stays nil. ValueLiteral writes the result as a literal of the
// same value, so it can stand in for value in a data statement. A value the
// column cannot hold is the *[ValueError] ValueLiteral returns for it.
func CanonicalValue(ydbType string, value any) (any, error) {
	literal, err := ValueLiteral(ydbType, value)
	if err != nil || literal == "NULL" {
		return nil, err
	}
	text, ok := LiteralValue(literal)
	if !ok {
		return nil, &ValueError{Type: ydbType, Value: literal, Reason: "Ptah cannot read back the literal it wrote"}
	}
	canonical, err := canonicalFromText(storageType(ydbType), text)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

// CanonicalRows returns copies of rows with every value of a column
// columnTypes names in the form [CanonicalValue] gives it, for a caller that
// compares rows, orders them or writes them through a data diff. A column the
// map does not name keeps its value. A value its column cannot hold is an
// error naming the row, counted from 1, and the column. A nil rows stays nil.
func CanonicalRows(columnTypes map[string]string, rows []map[string]any) ([]map[string]any, error) {
	if rows == nil {
		return nil, nil
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		typed := make(map[string]any, len(row))
		for column, value := range row {
			ydbType, ok := columnTypes[column]
			if !ok {
				typed[column] = value
				continue
			}
			canonical, err := CanonicalValue(ydbType, value)
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", i+1, column, err)
			}
			typed[column] = canonical
		}
		out[i] = typed
	}
	return out, nil
}

// canonicalFromText turns the text a canonical literal carries into the Go
// value CanonicalValue documents for the type.
func canonicalFromText(ydbType, text string) (any, error) {
	if kind, ok := integerSuffixes[ydbType]; ok {
		if kind.unsigned {
			return strconv.ParseUint(text, 10, 64)
		}
		return strconv.ParseInt(text, 10, 64)
	}
	switch ydbType {
	case Bool:
		return text == "true", nil
	case Float, Double:
		return strconv.ParseFloat(text, 64)
	case String, Yson:
		return []byte(text), nil
	case JSONDocument:
		return canonicalJSON(text)
	case Date, Date32:
		return time.Parse(time.DateOnly, text)
	case Datetime, Datetime64, Timestamp, Timestamp64:
		return time.Parse(time.RFC3339Nano, text)
	case Interval, Interval64:
		micros, ok := parseDuration(text)
		if !ok {
			return nil, &ValueError{Type: ydbType, Value: text, Reason: "it is not an ISO 8601 duration"}
		}
		return time.Duration(micros) * time.Microsecond, nil
	}
	return text, nil
}

// canonicalJSON writes a JSON document with sorted keys, no spaces and no
// HTML escaping.
func canonicalJSON(text string) (string, error) {
	var document any
	if err := json.Unmarshal([]byte(text), &document); err != nil {
		return "", &ValueError{Type: JSONDocument, Value: fmt.Sprintf("%q", text), Reason: "it is not JSON"}
	}
	var b strings.Builder
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(document); err != nil {
		return "", err
	}
	return strings.TrimSuffix(b.String(), "\n"), nil
}
