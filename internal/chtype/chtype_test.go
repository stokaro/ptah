package chtype_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/chtype"
)

func TestMap_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     chtype.Mapping
	}{
		{name: "portable integer", declared: "INTEGER", want: chtype.Mapping{Type: "Int32"}},
		{name: "portable 64-bit integer", declared: "INT8", want: chtype.Mapping{Type: "Int64"}},
		{name: "portable bigint in lower case", declared: "bigint", want: chtype.Mapping{Type: "Int64"}},
		{name: "portable string with a length", declared: "VARCHAR(255)", want: chtype.Mapping{Type: "String"}},
		{name: "portable timestamp", declared: "DATETIME", want: chtype.Mapping{Type: "DateTime64(3)"}},
		{name: "portable timestamp with a time zone", declared: "TIMESTAMPTZ", want: chtype.Mapping{Type: "DateTime64(3, 'UTC')"}},
		{name: "portable decimal", declared: "DECIMAL", want: chtype.Mapping{Type: "Decimal(38, 10)"}},
		{name: "portable decimal with precision", declared: "NUMERIC(10,2)", want: chtype.Mapping{Type: "Decimal(10,2)"}},
		{name: "portable float", declared: "FLOAT", want: chtype.Mapping{Type: "Float64"}},
		{name: "portable JSON", declared: "JSONB", want: chtype.Mapping{Type: "String", JSON: true}},
		// ClickHouse's own names, which upper-case into a portable name meaning
		// something else.
		{name: "native 8-bit integer", declared: "Int8", want: chtype.Mapping{Type: "Int8"}},
		{name: "native DateTime", declared: "DateTime", want: chtype.Mapping{Type: "DateTime"}},
		{name: "native DateTime with a time zone", declared: "DateTime('UTC')", want: chtype.Mapping{Type: "DateTime('UTC')"}},
		{name: "native bare Decimal", declared: "Decimal", want: chtype.Mapping{Type: "Decimal"}},
		{name: "native wrapper", declared: " Nullable(INT) ", want: chtype.Mapping{Type: "Nullable(INT)"}},
		{name: "native composite", declared: "LowCardinality(String)", want: chtype.Mapping{Type: "LowCardinality(String)"}},
		{name: "unknown spelling", declared: "GEOGRAPHY", want: chtype.Mapping{Type: "GEOGRAPHY", Unrecognized: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chtype.Map(tt.declared)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestMap_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		wantErr  string
	}{
		{name: "empty", declared: " ", wantErr: `clickhouse: column type is empty`},
		{name: "serial", declared: "BIGSERIAL", wantErr: `clickhouse: BIGSERIAL has no auto-increment equivalent.*`},
		{name: "time", declared: "TIME", wantErr: `clickhouse: TIME has no direct equivalent.*`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := chtype.Map(tt.declared)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(got, qt.Equals, chtype.Mapping{})
		})
	}
}
