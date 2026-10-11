package mysqlroutine_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/mysqlroutine"
)

// TestFromCharacteristics_ReadsWhatAStatementStates pins the declared half of
// the round trip: what a routine written in SQL states, read as the catalog
// reports the same routine.
//
// The SQL reader kept the characteristics as text and read none of them, so a
// routine declared SQL SECURITY DEFINER and DETERMINISTIC reached the model
// with the defaults, INVOKER and VOLATILE, and was created that way.
func TestFromCharacteristics_ReadsWhatAStatementStates(t *testing.T) {
	for _, test := range []struct {
		name            string
		characteristics []string
		wantSecurity    string
		wantVolatility  string
	}{
		{name: "nothing stated", characteristics: nil},
		{name: "invoker", characteristics: []string{"SQL SECURITY INVOKER"}, wantSecurity: "INVOKER"},
		{name: "definer, spelled loosely", characteristics: []string{"sql  security\tdefiner"}, wantSecurity: "DEFINER"},
		{name: "deterministic", characteristics: []string{"DETERMINISTIC"}, wantVolatility: "IMMUTABLE"},
		{name: "deterministic with data access", characteristics: []string{"DETERMINISTIC", "READS SQL DATA"}, wantVolatility: "IMMUTABLE"},
		{name: "not deterministic, no sql", characteristics: []string{"NOT DETERMINISTIC", "NO SQL"}, wantVolatility: "STABLE"},
		{name: "no sql alone", characteristics: []string{"NO SQL"}, wantVolatility: "STABLE"},
		{name: "reads sql data", characteristics: []string{"READS SQL DATA"}, wantVolatility: "VOLATILE"},
		{name: "modifies sql data", characteristics: []string{"MODIFIES SQL DATA"}, wantVolatility: "VOLATILE"},
		{name: "contains sql", characteristics: []string{"CONTAINS SQL"}, wantVolatility: "VOLATILE"},
		{name: "not deterministic alone", characteristics: []string{"NOT DETERMINISTIC"}, wantVolatility: "VOLATILE"},
		{name: "the last one counts", characteristics: []string{"DETERMINISTIC", "SQL SECURITY DEFINER", "NOT DETERMINISTIC", "SQL SECURITY INVOKER"},
			wantSecurity: "INVOKER", wantVolatility: "VOLATILE"},
		{name: "others are skipped", characteristics: []string{"COMMENT ' SQL SECURITY DEFINER NO SQL '", "LANGUAGE SQL"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			security, volatility := mysqlroutine.FromCharacteristics(test.characteristics)

			c.Assert(security, qt.Equals, test.wantSecurity)
			c.Assert(volatility, qt.Equals, test.wantVolatility)
		})
	}
}

// TestFromCharacteristics_InvertsCharacteristic closes the round trip a
// renderer starts: what [mysqlroutine.Characteristic] writes for a volatility
// reads back as that volatility.
func TestFromCharacteristics_InvertsCharacteristic(t *testing.T) {
	for _, volatility := range []string{mysqlroutine.Immutable, mysqlroutine.Stable, mysqlroutine.Volatile} {
		t.Run(volatility, func(t *testing.T) {
			c := qt.New(t)
			written, err := mysqlroutine.Characteristic(volatility)
			c.Assert(err, qt.IsNil)

			_, read := mysqlroutine.FromCharacteristics([]string{written})

			c.Assert(read, qt.Equals, volatility)
		})
	}
}
