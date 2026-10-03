package ydb_test

import (
	"context"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// The values are the ones YDB 26.2.1.14 stored for defaults written as the
// literal on the right, read back through a raw DescribeTable call. Each must
// read back as the literal ydbtype writes for it.
func TestReader_DefaultLiterals(t *testing.T) {
	tests := []struct {
		name  string
		typ   *Ydb.Type
		value *Ydb.Value
		want  string
	}{
		{name: "bool", typ: primitive(Ydb.Type_BOOL), value: &Ydb.Value{Value: &Ydb.Value_BoolValue{BoolValue: true}},
			want: "true"},
		{name: "int16", typ: primitive(Ydb.Type_INT16), value: &Ydb.Value{Value: &Ydb.Value_Int32Value{Int32Value: 5}},
			want: "5s"},
		{name: "int64", typ: primitive(Ydb.Type_INT64), value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: -5000000000}},
			want: "-5000000000l"},
		{name: "uint8", typ: primitive(Ydb.Type_UINT8), value: &Ydb.Value{Value: &Ydb.Value_Uint32Value{Uint32Value: 5}},
			want: "5ut"},
		{name: "uint64", typ: primitive(Ydb.Type_UINT64), value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 5}},
			want: "5ul"},
		{name: "float", typ: primitive(Ydb.Type_FLOAT), value: &Ydb.Value{Value: &Ydb.Value_FloatValue{FloatValue: 1.5}},
			want: "Float('1.5')"},
		{name: "a float that is not exact in binary", typ: primitive(Ydb.Type_FLOAT),
			value: &Ydb.Value{Value: &Ydb.Value_FloatValue{FloatValue: 0.1}}, want: "Float('0.1')"},
		{name: "double", typ: primitive(Ydb.Type_DOUBLE), value: &Ydb.Value{Value: &Ydb.Value_DoubleValue{DoubleValue: 2.5}},
			want: "Double('2.5')"},
		{name: "string with a zero byte", typ: primitive(Ydb.Type_STRING),
			value: &Ydb.Value{Value: &Ydb.Value_BytesValue{BytesValue: []byte("by\x00te")}}, want: `'by\x00te'`},
		{name: "utf8 with a quote", typ: primitive(Ydb.Type_UTF8),
			value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "it's"}}, want: `'it\'s'u`},
		{name: "json", typ: primitive(Ydb.Type_JSON),
			value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: `{"a":1}`}}, want: `Json('{"a":1}')`},
		{name: "json document", typ: primitive(Ydb.Type_JSON_DOCUMENT),
			value: &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: `{"b":2}`}}, want: `JsonDocument('{"b":2}')`},
		{name: "yson", typ: primitive(Ydb.Type_YSON),
			value: &Ydb.Value{Value: &Ydb.Value_BytesValue{BytesValue: []byte("[1;2]")}}, want: "Yson('[1;2]')"},
		{name: "uuid", typ: primitive(Ydb.Type_UUID),
			value: &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 4743665464302797824}, High_128: 75133578647207},
			want:  "Uuid('550e8400-e29b-41d4-a716-446655440000')"},
		{name: "date", typ: primitive(Ydb.Type_DATE), value: &Ydb.Value{Value: &Ydb.Value_Uint32Value{Uint32Value: 20455}},
			want: "Date('2026-01-02')"},
		{name: "datetime", typ: primitive(Ydb.Type_DATETIME),
			value: &Ydb.Value{Value: &Ydb.Value_Uint32Value{Uint32Value: 1767323045}}, want: "Datetime('2026-01-02T03:04:05Z')"},
		{name: "timestamp", typ: primitive(Ydb.Type_TIMESTAMP),
			value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 1767323045123456}},
			want:  "Timestamp('2026-01-02T03:04:05.123456Z')"},
		{name: "interval", typ: primitive(Ydb.Type_INTERVAL),
			value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: 93600000000}}, want: "Interval('P1DT2H')"},
		{name: "date32 before 1970", typ: primitive(Ydb.Type_DATE32),
			value: &Ydb.Value{Value: &Ydb.Value_Int32Value{Int32Value: -25566}}, want: "Date32('1900-01-02')"},
		{name: "datetime64 before 1970", typ: primitive(Ydb.Type_DATETIME64),
			value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: -2208891355}},
			want:  "Datetime64('1900-01-02T03:04:05Z')"},
		{name: "timestamp64 before 1970", typ: primitive(Ydb.Type_TIMESTAMP64),
			value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: -2208891354999999}},
			want:  "Timestamp64('1900-01-02T03:04:05.000001Z')"},
		{name: "a negative interval64", typ: primitive(Ydb.Type_INTERVAL64),
			value: &Ydb.Value{Value: &Ydb.Value_Int64Value{Int64Value: -1000000}}, want: "Interval64('-PT1S')"},
		{name: "decimal", typ: decimal(22, 9), value: &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 1500000000}},
			want: "Decimal('1.5', 22, 9)"},
		{name: "a negative decimal", typ: decimal(22, 9),
			value: &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 1<<64 - 1500000000}, High_128: 1<<64 - 1},
			want:  "Decimal('-1.5', 22, 9)"},
		{name: "a decimal below one", typ: decimal(10, 2), value: &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 5}},
			want: "Decimal('0.05', 10, 2)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := plainTable(&Ydb_Table.ColumnMeta{Name: "c", Type: optional(test.typ),
				DefaultValue: literal(test.typ, test.value)})
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
			}

			db := readFrom(c, source)

			c.Assert(db.Tables[0].Columns[1].ColumnDefault, qt.DeepEquals, new(test.want))
		})
	}
}

func TestReader_DefaultLiterals_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		typ     *Ydb.Type
		value   *Ydb.Value
		wantErr string
	}{
		{
			// YDB stores a decimal's infinity and NaN past its precision.
			name:    "a decimal that is not a finite number",
			typ:     decimal(10, 2),
			value:   &Ydb.Value{Value: &Ydb.Value_Low_128{Low_128: 100000000000000000}, High_128: 0},
			wantErr: `YDB table /local/t: column "c": its default: a Decimal\(10,2\) value that is not a finite number`,
		},
		{
			// A YDB Timestamp stops before 2106, so a value past what int64
			// microseconds hold is not one YDB wrote.
			name:    "a Timestamp past any YDB stores",
			typ:     primitive(Ydb.Type_TIMESTAMP),
			value:   &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: math.MaxUint64}},
			wantErr: `YDB table /local/t: column "c": its default: a Timestamp 18446744073709551615 microseconds after 1970, which is past any Timestamp YDB stores`,
		},
		{
			name:    "a NULL default",
			typ:     primitive(Ydb.Type_UTF8),
			value:   &Ydb.Value{Value: &Ydb.Value_NullFlagValue{}},
			wantErr: `YDB table /local/t: column "c": its default: a NULL default, which YDB does not store`,
		},
		{
			// A value Literal refuses is refused, rather than carried in a
			// spelling no declaration could write.
			name:    "a DyNumber that is not a number",
			typ:     primitive(Ydb.Type_DYNUMBER),
			value:   &Ydb.Value{Value: &Ydb.Value_TextValue{TextValue: "not a number"}},
			wantErr: `YDB table /local/t: column "c": its default: default "not a number" has no YDB counterpart: it is not a DyNumber value`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			described := plainTable(&Ydb_Table.ColumnMeta{Name: "c", Type: optional(test.typ),
				DefaultValue: literal(test.typ, test.value)})
			source := fakeSource{
				directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
				tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
			}

			db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).
				ReadSchemaContext(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
