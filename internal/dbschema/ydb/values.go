package ydb

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbtype"
)

// literalCapabilities lets [ydbtype.Literal] write every literal a stored
// default can be. A line that refuses a default of some type at CREATE TABLE
// never stores one, so a stored default is written whatever the line.
var literalCapabilities = capability.Capabilities{
	capability.SmallIntegerDefaults: true,
	capability.DocumentTypeDefaults: true,
}

// defaultLiteral writes a stored default as the YQL literal
// [ydbtype.Literal] writes for the same value, which is the spelling a
// declared default is compared in.
func defaultLiteral(ydbType string, stored *Ydb.TypedValue) (string, error) {
	text, err := valueText(ydbType, stored.GetValue())
	if err != nil {
		return "", fmt.Errorf("its default: %w", err)
	}
	literal, err := ydbtype.Literal(ydbType, text, literalCapabilities)
	if err != nil {
		return "", fmt.Errorf("its default: %w", err)
	}
	return literal, nil
}

// valueText writes a stored value of ydbType as the text a declaration would
// give it.
func valueText(ydbType string, value *Ydb.Value) (string, error) {
	if _, null := value.GetValue().(*Ydb.Value_NullFlagValue); null {
		return "", fmt.Errorf("a NULL default, which YDB does not store")
	}
	switch ydbType {
	case ydbtype.Bool:
		return strconv.FormatBool(value.GetBoolValue()), nil
	case ydbtype.Int8, ydbtype.Int16, ydbtype.Int32:
		return strconv.FormatInt(int64(value.GetInt32Value()), 10), nil
	case ydbtype.Int64:
		return strconv.FormatInt(value.GetInt64Value(), 10), nil
	case ydbtype.Uint8, ydbtype.Uint16, ydbtype.Uint32:
		return strconv.FormatUint(uint64(value.GetUint32Value()), 10), nil
	case ydbtype.Uint64:
		return strconv.FormatUint(value.GetUint64Value(), 10), nil
	case ydbtype.Float:
		return strconv.FormatFloat(float64(value.GetFloatValue()), 'g', -1, 32), nil
	case ydbtype.Double:
		return strconv.FormatFloat(value.GetDoubleValue(), 'g', -1, 64), nil
	case ydbtype.String, ydbtype.Yson:
		return string(value.GetBytesValue()), nil
	case ydbtype.Utf8, ydbtype.JSON, ydbtype.JSONDocument, ydbtype.DyNumber:
		return value.GetTextValue(), nil
	case ydbtype.UUID:
		return uuidText(value.GetLow_128(), value.GetHigh_128()), nil
	case ydbtype.Date:
		return dayText(int64(value.GetUint32Value())), nil
	case ydbtype.Date32:
		return dayText(int64(value.GetInt32Value())), nil
	case ydbtype.Datetime:
		return instantText(time.Unix(int64(value.GetUint32Value()), 0)), nil
	case ydbtype.Datetime64:
		return instantText(time.Unix(value.GetInt64Value(), 0)), nil
	case ydbtype.Timestamp:
		return instantText(time.UnixMicro(int64(value.GetUint64Value()))), nil
	case ydbtype.Timestamp64:
		return instantText(time.UnixMicro(value.GetInt64Value())), nil
	case ydbtype.Interval, ydbtype.Interval64:
		return ydbtype.IntervalText(value.GetInt64Value()), nil
	}
	if precision, scale, ok := ydbtype.DecimalArguments(ydbType); ok {
		return decimalText(value.GetLow_128(), value.GetHigh_128(), precision, scale)
	}
	return "", fmt.Errorf("a value of type %s, which Ptah does not read", ydbType)
}

// uuidText writes a UUID from the two halves YDB stores it in. The first three
// fields are little-endian in the stored bytes and the last two are not:
// measured, 550e8400-e29b-41d4-a716-446655440000 is stored as low
// 0x41d4e29b550e8400 and high 0x4455664416a7.
func uuidText(low, high uint64) string {
	var raw [16]byte
	for i := range 8 {
		raw[i] = byte(low >> (8 * i))
		raw[8+i] = byte(high >> (8 * i))
	}
	ordered := []byte{
		raw[3], raw[2], raw[1], raw[0],
		raw[5], raw[4],
		raw[7], raw[6],
		raw[8], raw[9],
		raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
	}
	text := hex.EncodeToString(ordered)
	return text[0:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:32]
}

// dayText writes the date days after 1970-01-01.
func dayText(days int64) string {
	return time.Unix(days*86400, 0).UTC().Format(time.DateOnly)
}

// instantText writes an instant in UTC with as many fractional digits as it
// has, which is the form ydbtype reads.
func instantText(instant time.Time) string {
	return instant.UTC().Format("2006-01-02T15:04:05.999999Z")
}

// decimalText writes a Decimal(precision, scale) stored as a 128-bit two's
// complement integer of its scaled value. YDB stores infinity and NaN as
// values beyond the precision, which no default Ptah writes can be, so they
// are refused.
func decimalText(low, high uint64, precision, scale int) (string, error) {
	scaled := new(big.Int).Lsh(new(big.Int).SetUint64(high), 64)
	scaled.Or(scaled, new(big.Int).SetUint64(low))
	if high>>63 == 1 {
		scaled.Sub(scaled, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	negative := scaled.Sign() < 0
	digits := new(big.Int).Abs(scaled).String()
	if len(digits) > precision {
		return "", fmt.Errorf("a Decimal(%d,%d) value that is not a finite number", precision, scale)
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	split := len(digits) - scale
	return ydbtype.DecimalText(negative, digits[:split], digits[split:]), nil
}
