package ydb

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"ptah.run/internal/ydbtype"
)

// HoldsBytes reports whether a column the driver describes as databaseType
// holds bytes rather than text: YDB's String and Yson, optional or not. The
// driver names a column by its YQL type, `Optional<String>` for a nullable one,
// and hands String, Yson, Json and JsonDocument values over as []byte alike, so
// the name is what tells the bytes from the text.
func HoldsBytes(databaseType string) bool {
	name := strings.TrimSpace(databaseType)
	for {
		inner, wrapped := strings.CutPrefix(name, "Optional<")
		if !wrapped || !strings.HasSuffix(inner, ">") {
			break
		}
		name = strings.TrimSuffix(inner, ">")
	}
	return name == ydbtype.String || name == ydbtype.Yson
}

// scannedDecimal is what ydb-go-sdk hands database/sql for a Decimal value:
// the 128-bit two's complement integer of the scaled value, big-endian, with
// the column's precision and scale. The SDK's type is internal to it, so the
// value is recognized by the method.
type scannedDecimal interface {
	Decimal() (bytes [16]byte, precision uint32, scale uint32)
}

// RowValue turns a value database/sql scanned from a YDB column into the plain
// Go value Ptah compares and writes: a Decimal becomes its digits as a string,
// as ydbtype writes them, and a moment comes back in UTC, the only zone YDB
// stores, where the driver hands it over in the local zone. Every other value
// is returned as it was scanned.
func RowValue(value any) (any, error) {
	switch typed := value.(type) {
	case scannedDecimal:
		bytes, precision, scale := typed.Decimal()
		text, err := decimalText(binary.BigEndian.Uint64(bytes[8:]), binary.BigEndian.Uint64(bytes[:8]),
			int(precision), int(scale))
		if err != nil {
			return nil, fmt.Errorf("read %w", err)
		}
		return text, nil
	case time.Time:
		return typed.UTC(), nil
	default:
		return value, nil
	}
}
