// Package chtype reads ClickHouse column types for the renderer that writes
// them, the parser and reader that ask whether a column admits NULL, and the
// schema comparison that asks whether a live column already has the declared
// type.
//
// They have to give one answer. When the comparison folded types by family
// instead, Int32 and Int64 were both "integer", and a column declared Int64
// over a live Int32 recorded no change (stokaro/ptah#4105).
package chtype

import (
	"fmt"
	"strings"
)

// Mapping is the ClickHouse type a declared type becomes.
type Mapping struct {
	// Type is the ClickHouse type expression the renderer writes.
	Type string
	// JSON is set when the declaration was JSON or JSONB, which ClickHouse
	// stores as a String.
	JSON bool
	// Unrecognized is set when the declaration named no known SQL type and does
	// not look like a native ClickHouse one, so it was passed through verbatim.
	Unrecognized bool
}

// directTypeMap is a lookup table of base SQL type names (UPPER, parameter
// stripped) to their direct ClickHouse equivalents. Types whose mapping
// depends on parameters or warrants a notice are handled in Map rather than
// going through this table.
var directTypeMap = map[string]string{
	"TEXT": "String", "VARCHAR": "String", "CHAR": "String",
	"CHARACTER": "String", "STRING": "String",
	"CHARACTER VARYING": "String", "CITEXT": "String",
	"BYTEA": "String", "BLOB": "String",
	"BOOLEAN": "Bool", "BOOL": "Bool",
	"SMALLINT": "Int16", "INT2": "Int16",
	"INTEGER": "Int32", "INT": "Int32", "INT4": "Int32",
	"BIGINT": "Int64", "INT8": "Int64",
	"REAL": "Float32", "FLOAT4": "Float32",
	"DOUBLE": "Float64", "DOUBLE PRECISION": "Float64",
	"FLOAT": "Float64", "FLOAT8": "Float64",
	"DATE": "Date",
	"UUID": "UUID",
}

// Map translates a generic SQL column type spelling into the ClickHouse
// equivalent. Type names not recognized are returned verbatim; callers may
// legitimately write native ClickHouse type names in their annotations (e.g.
// `LowCardinality(String)`), and this function must not mangle them.
//
// The matcher is intentionally narrow: it splits the type on '(' so a
// `VARCHAR(255)` still maps to `String`, but anything that doesn't look like a
// known SQL type is passed through untouched.
func Map(t string) (Mapping, error) {
	upper := strings.ToUpper(strings.TrimSpace(t))
	if upper == "" {
		return Mapping{}, fmt.Errorf("clickhouse: column type is empty")
	}

	// Strip parametrisation for the base lookup, keeping it around for the
	// (small number of) types where the precision actually matters.
	base := upper
	var params string
	if idx := strings.Index(upper, "("); idx >= 0 {
		base = strings.TrimSpace(upper[:idx])
		params = strings.TrimSpace(upper[idx:])
	}

	if nativeFamilies[familyName(t)] {
		return Mapping{Type: strings.TrimSpace(t)}, nil
	}

	if direct, ok := directTypeMap[base]; ok {
		return Mapping{Type: direct}, nil
	}

	switch base {
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return Mapping{}, fmt.Errorf("clickhouse: %s has no auto-increment equivalent; use UUID/Int64 + an explicit value or use a ReplacingMergeTree pattern", upper)
	case "NUMERIC", "DECIMAL":
		if params == "" {
			return Mapping{Type: "Decimal(38, 10)"}, nil
		}
		return Mapping{Type: "Decimal" + params}, nil
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		// Pin TZ-aware columns to UTC so the round-trip with a TZ-naive
		// DateTime64 doesn't silently drop time-zone information.
		return Mapping{Type: "DateTime64(3, 'UTC')"}, nil
	case "TIMESTAMP", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE":
		return Mapping{Type: "DateTime64(3)"}, nil
	case "TIME":
		// ClickHouse has no plain TIME type; surface this rather than silently
		// pick something lossy.
		return Mapping{}, fmt.Errorf("clickhouse: TIME has no direct equivalent; map to String or DateTime64 explicitly via platform.clickhouse.type")
	case "JSON", "JSONB":
		// Treat JSON as a String; ClickHouse's native JSON type is still
		// experimental. Users who want it can override via platform.clickhouse.type.
		return Mapping{Type: "String", JSON: true}, nil
	}

	// Pass through native ClickHouse types untouched. Flag pass-throughs that
	// don't look like a CH-native composite type so users see when an unknown
	// spelling has been forwarded as-is.
	return Mapping{Type: t, Unrecognized: !looksNative(t)}, nil
}

// looksNative is a heuristic that recognizes common ClickHouse-native type
// spellings (LowCardinality, Array, Map, Nullable, Enum8/16, FixedString,
// Tuple, Nested, ...). It only needs to avoid false-positively warning on
// legitimate native types; precise validation is the database's job.
func looksNative(t string) bool {
	t = strings.TrimSpace(t)
	if t == "" {
		return false
	}
	// Native CH types are conventionally PascalCase; treat the presence of an
	// uppercase letter followed by lowercase as a strong hint.
	for i := 0; i < len(t)-1; i++ {
		if t[i] >= 'A' && t[i] <= 'Z' && t[i+1] >= 'a' && t[i+1] <= 'z' {
			return true
		}
	}
	return false
}

// nativeFamilies are ClickHouse's own type family names, spelled the way
// ClickHouse spells them, read from system.data_type_families on 24.10.4 and
// 26.9.8.
//
// A declaration that names one of them exactly is that type, and the portable
// map above does not see it. The map matches upper case, and three native
// spellings upper-case into a portable name meaning something else: measured on
// 26.9.8, a schema file declaring Int8 created an Int64 column, DateTime and
// DateTime('UTC') a DateTime64(3) one, and a bare Decimal a Decimal(38, 10) one
// where ClickHouse makes Decimal(10, 0). The portable spellings INT8, DATETIME
// and DECIMAL keep their mapping.
//
// JSON is left out on purpose: the map writes a declared JSON as String, and
// ClickHouse's own JSON type is opted into through platform.clickhouse.type.
// Time and Time64 are left out because 24.10 reads TIME as an alias of Int64
// and 26.9 as a type of its own, and the map refuses TIME on every line.
var nativeFamilies = map[string]bool{
	"AggregateFunction": true, "Array": true, "BFloat16": true, "Bool": true,
	"Date": true, "Date32": true, "DateTime": true, "DateTime32": true, "DateTime64": true,
	"Decimal": true, "Decimal32": true, "Decimal64": true, "Decimal128": true, "Decimal256": true,
	"Dynamic": true, "Enum": true, "Enum8": true, "Enum16": true, "FixedString": true,
	"Float32": true, "Float64": true, "Geometry": true,
	"Int8": true, "Int16": true, "Int32": true, "Int64": true, "Int128": true, "Int256": true,
	"IntervalDay": true, "IntervalHour": true, "IntervalMicrosecond": true,
	"IntervalMillisecond": true, "IntervalMinute": true, "IntervalMonth": true,
	"IntervalNanosecond": true, "IntervalQuarter": true, "IntervalSecond": true,
	"IntervalWeek": true, "IntervalYear": true, "IPv4": true, "IPv6": true,
	"LineString": true, "LowCardinality": true, "Map": true, "MultiLineString": true,
	"MultiPoint": true, "MultiPolygon": true, "Nested": true, "Nothing": true,
	"Nullable": true, "Object": true, "Point": true, "Polygon": true, "QBit": true,
	"Ring": true, "SimpleAggregateFunction": true, "String": true, "Tuple": true,
	"UInt8": true, "UInt16": true, "UInt32": true, "UInt64": true, "UInt128": true,
	"UInt256": true, "UUID": true, "Variant": true,
}

// familyName is the type name before its parameter list, as written.
func familyName(t string) string {
	t = strings.TrimSpace(t)
	if idx := strings.Index(t, "("); idx >= 0 {
		return strings.TrimSpace(t[:idx])
	}
	return t
}
