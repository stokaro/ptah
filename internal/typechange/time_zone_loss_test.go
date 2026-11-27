package typechange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/typechange"
)

func TestDropsTimeZone(t *testing.T) {
	tests := []struct {
		name, held, declared string
		want                 bool
	}{
		{"time offset", "timetz(6)", "time(6)", true},
		{"timestamp instant", "timestamptz", "timestamp", true},
		{"SQL time spelling", "TIME(3) WITH TIME ZONE", "TIME(3) WITHOUT TIME ZONE", true},
		{"SQL timestamp spelling", "TIMESTAMP WITH TIME ZONE", "timestamp", true},
		{"SQL Server offset", "datetimeoffset(7)", "datetime2(7)", true},
		{"offset and time removed", "datetimeoffset(7)", "date", true},
		{"zone added", "timestamp", "timestamptz", false},
		{"same zone", "timestamp(6) with time zone", "timestamptz(6)", false},
		{"zone retained at fewer digits", "datetimeoffset(7)", "datetimeoffset(0)", false},
		{"zone-free pair", "timestamp(6)", "timestamp(0)", false},
		{"arrays", "timestamptz[]", "timestamp[]", false},
		{"interval", "timetz", "interval", false},
		{"unknown target", "timetz", "custom_type", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(typechange.DropsTimeZone(test.held, test.declared), qt.Equals, test.want)
		})
	}
}
