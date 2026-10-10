package ydbttl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbttl"
)

func quote(name string) string { return sqlident.Quote("ydb", name) }

// The intervals YDB takes, and the seconds it keeps of each, as measured on
// 25.1.4.7 and 26.2.1.14.
func TestIntervalSeconds_HappyPath(t *testing.T) {
	tests := []struct {
		interval string
		want     uint64
	}{
		{interval: "P1D", want: 86400},
		{interval: "PT1H", want: 3600},
		{interval: "PT90M", want: 5400},
		{interval: "PT3600S", want: 3600},
		{interval: "P1W", want: 604800},
		{interval: "P2W3D", want: 1468800},
		{interval: "P1DT2H30M15S", want: 95415},
		{interval: "PT0S", want: 0},
		{interval: "P0D", want: 0},
		{interval: "PT1.0S", want: 1},
		{interval: "PT1000000000S", want: 1000000000},
		{interval: "P49672DT23H59M59S", want: ydbttl.MaxSeconds},
	}
	for _, test := range tests {
		t.Run(test.interval, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbttl.IntervalSeconds(test.interval)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The spellings YDB refuses, and a fraction of a second, which it would keep
// as fewer seconds than were declared.
func TestIntervalSeconds_FailurePath(t *testing.T) {
	tests := []struct {
		interval string
		wantErr  string
	}{
		{interval: "30 days", wantErr: `.*it does not begin with P`},
		{interval: "P1M", wantErr: `.*designator "M" is out of place.*`},
		{interval: "P1Y", wantErr: `.*designator "Y" is out of place.*`},
		{interval: "p1d", wantErr: `.*it does not begin with P`},
		{interval: "P1D ", wantErr: `.*" " is not a number followed by a designator`},
		{interval: "-P1D", wantErr: `.*it does not begin with P`},
		{interval: "P", wantErr: `.*it names no weeks, days, hours, minutes or seconds`},
		{interval: "PT", wantErr: `.*T is not followed by hours, minutes or seconds`},
		{interval: "P1DT", wantErr: `.*T is not followed by hours, minutes or seconds`},
		{interval: "PT1H1H", wantErr: `.*designator "H" is out of place.*`},
		{interval: "P1D1W", wantErr: `.*designator "W" is out of place.*`},
		{interval: "P1.5D", wantErr: `.*"1.5" is not a number YDB takes there`},
		{interval: "PT1.5S", wantErr: `interval "PT1.5S" has a fraction of a second, and YDB keeps whole seconds only .*`},
		{interval: "PT0.000001S", wantErr: `interval "PT0.000001S" has a fraction of a second.*`},
		{interval: "P49673D", wantErr: `interval "P49673D" is longer than YDB's Interval type holds \(P49672DT23H59M59S\)`},
	}
	for _, test := range tests {
		t.Run(test.interval, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbttl.IntervalSeconds(test.interval)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, uint64(0))
		})
	}
}

// FormatInterval writes what YDB's SHOW CREATE TABLE writes, measured on
// 26.2.1.14, and what it writes reads back to the same seconds.
func TestFormatInterval_HappyPath(t *testing.T) {
	tests := []struct {
		seconds uint64
		want    string
	}{
		{seconds: 0, want: "PT0S"},
		{seconds: 1, want: "PT1S"},
		{seconds: 3600, want: "PT1H"},
		{seconds: 86400, want: "P1D"},
		{seconds: 604800, want: "P7D"},
		{seconds: 95415, want: "P1DT2H30M15S"},
		{seconds: 1000000000, want: "P11574DT1H46M40S"},
		{seconds: 90061, want: "P1DT1H1M1S"},
		{seconds: 60, want: "PT1M"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbttl.FormatInterval(test.seconds), qt.Equals, test.want)
			back, err := ydbttl.IntervalSeconds(test.want)
			c.Assert(err, qt.IsNil)
			c.Assert(back, qt.Equals, test.seconds)
		})
	}
}

func TestUnit_HappyPath(t *testing.T) {
	tests := []struct {
		declared string
		want     string
	}{
		{declared: "", want: ""},
		{declared: "seconds", want: ydbttl.Seconds},
		{declared: " Milliseconds ", want: ydbttl.Milliseconds},
		{declared: "MICROSECONDS", want: ydbttl.Microseconds},
		{declared: "nanoseconds", want: ydbttl.Nanoseconds},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbttl.Unit(test.declared)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestUnit_FailurePath(t *testing.T) {
	c := qt.New(t)
	got, err := ydbttl.Unit("ms")
	c.Assert(err, qt.ErrorMatches, `unit "ms" is not one YDB takes: use SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS`)
	c.Assert(got, qt.Equals, "")
}

// Setting writes the value of the TTL setting, the interval as declared and
// the unit after AS for an integer column.
func TestSetting(t *testing.T) {
	tests := []struct {
		name                   string
		column, interval, unit string
		want                   string
	}{
		{name: "a date column", column: "created_at", interval: "P30D", want: "Interval(\"P30D\") ON `created_at`"},
		{name: "an integer column", column: "expires", interval: "PT1H", unit: "MILLISECONDS", want: "Interval(\"PT1H\") ON `expires` AS MILLISECONDS"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbttl.Setting(test.column, test.interval, test.unit, quote), qt.Equals, test.want)
		})
	}
}

// The column types and units YDB takes, measured on 25.1.4.7 and 26.2.1.14.
func TestColumnRefusal(t *testing.T) {
	tests := []struct {
		name    string
		ydbType string
		unit    string
		want    string
	}{
		{name: "Timestamp", ydbType: "Timestamp"},
		{name: "Date", ydbType: "Date"},
		{name: "Datetime", ydbType: "Datetime"},
		{name: "Date32", ydbType: "Date32"},
		{name: "Datetime64", ydbType: "Datetime64"},
		{name: "Timestamp64", ydbType: "Timestamp64"},
		{name: "Uint32 in seconds", ydbType: "Uint32", unit: "SECONDS"},
		{name: "Uint64 in nanoseconds", ydbType: "Uint64", unit: "NANOSECONDS"},
		{name: "DyNumber in milliseconds", ydbType: "DyNumber", unit: "MILLISECONDS"},
		{
			name: "Timestamp with a unit", ydbType: "Timestamp", unit: "SECONDS",
			want: "date type, and YDB takes no unit",
		},
		{name: "Uint64 without a unit", ydbType: "Uint64", want: "an integer type, and YDB needs the unit"},
		{name: "Int64", ydbType: "Int64", unit: "SECONDS", want: "(`Unsupported column type`)"},
		{name: "Interval", ydbType: "Interval", want: "(`Unsupported column type`)"},
		{name: "Utf8", ydbType: "Utf8", want: "(`Unsupported column type`)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbttl.ColumnRefusal("c", test.ydbType, test.unit)
			c.Assert(got, qt.Contains, test.want)
			c.Assert(got == "", qt.Equals, test.want == "")
		})
	}
}
