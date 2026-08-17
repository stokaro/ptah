package crdbinterval_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/crdbinterval"
)

// TestEqual_AgreesWithWhatTheServerStored is the table this package exists for.
//
// Every row is a (declared, stored) pair measured on live CockroachDB CCL
// v25.4.14 and v26.2.5: the left value was written into `ttl_expire_after` and
// the right one is what pg_class.reloptions reported back. The two must compare
// equal, or a declaration using that spelling can never converge and the plan
// re-issues the change forever.
//
// This is the whole reason stokaro/ptah#1027 refused the parameter and
// stokaro/ptah#1605 could accept it: comparing the VALUES rather than
// predicting the stored spelling.
func TestEqual_AgreesWithWhatTheServerStored(t *testing.T) {
	tests := []struct {
		declared string
		stored   string
	}{
		{declared: "3 days", stored: "3 days"},
		{declared: "72 hours", stored: "72:00:00"},
		{declared: "5 minutes", stored: "00:05:00"},
		{declared: "1 day 2 hours", stored: "1 day 02:00:00"},
		{declared: "1 mon", stored: "1 mon"},
		{declared: "1 month", stored: "1 mon"},
		{declared: "1 year", stored: "1 year"},
		{declared: "2 years 3 months", stored: "2 years 3 mons"},
		{declared: "00:05:00", stored: "00:05:00"},
		{declared: "1 day 02:00:00", stored: "1 day 02:00:00"},
		{declared: "30 seconds", stored: "00:00:30"},
		{declared: "90 seconds", stored: "00:01:30"},
		{declared: "1500 milliseconds", stored: "00:00:01.5"},
		{declared: "1 week", stored: "7 days"},
		{declared: "2 weeks", stored: "14 days"},
		{declared: "P1D", stored: "1 day"},
		{declared: "P1Y2M3D", stored: "1 year 2 mons 3 days"},
		{declared: "PT1H30M", stored: "01:30:00"},
		{declared: "1 year 2 mons 3 days 04:05:06", stored: "1 year 2 mons 3 days 04:05:06"},
		{declared: "1.5 hours", stored: "01:30:00"},
		{declared: "0 seconds", stored: "00:00:00"},
		{declared: "1 day 1 hour 1 minute 1 second", stored: "1 day 01:01:01"},
		{declared: "24 hours", stored: "24:00:00"},
		{declared: "36 hours", stored: "36:00:00"},
	}

	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)

			equal, err := crdbinterval.Equal(test.declared, test.stored)

			c.Assert(err, qt.IsNil)
			c.Assert(equal, qt.IsTrue)
		})
	}
}

// TestParse_KeepsTheThreeFieldsApart pins the property a duration type would
// lose.
//
// PostgreSQL intervals are (months, days, microseconds) and the three do not
// convert into one another: a month is not thirty days and a day is not
// twenty-four hours across a DST boundary. The server respects that — measured,
// `72 hours` stays `72:00:00` rather than becoming `3 days` — so a comparison
// that collapsed the triple would call two different retention policies equal.
func TestParse_KeepsTheThreeFieldsApart(t *testing.T) {
	tests := []struct {
		name string
		text string
		want crdbinterval.Value
	}{
		{name: "days stay days", text: "3 days", want: crdbinterval.Value{Days: 3}},
		{name: "hours stay microseconds", text: "72 hours", want: crdbinterval.Value{Micros: 72 * 3600 * 1_000_000}},
		{name: "months stay months", text: "1 mon", want: crdbinterval.Value{Months: 1}},
		{name: "years fold into months", text: "2 years", want: crdbinterval.Value{Months: 24}},
		{name: "weeks fold into days, as the server folds them", text: "1 week", want: crdbinterval.Value{Days: 7}},
		{
			name: "a mixed literal fills all three",
			text: "1 year 2 mons 3 days 04:05:06",
			want: crdbinterval.Value{
				Months: 14,
				Days:   3,
				Micros: 4*3600*1_000_000 + 5*60*1_000_000 + 6*1_000_000,
			},
		},
		{name: "fractional seconds", text: "00:00:01.5", want: crdbinterval.Value{Micros: 1_500_000}},
		{name: "the zero interval", text: "0 seconds", want: crdbinterval.Value{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, err := crdbinterval.Parse(test.text)

			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.Equals, test.want)
		})
	}
}

// TestEqual_SeparatesIntervalsTheServerSeparates is the control on the table
// above. Without it, an Equal that returned true for everything would satisfy
// every row there.
func TestEqual_SeparatesIntervalsTheServerSeparates(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
	}{
		{name: "a month is not thirty days", a: "1 mon", b: "30 days"},
		{name: "a day is not twenty-four hours", a: "1 day", b: "24 hours"},
		{name: "different day counts", a: "3 days", b: "4 days"},
		{name: "different clock times", a: "00:05:00", b: "00:06:00"},
		{name: "a week is not a month", a: "4 weeks", b: "1 mon"},
		{name: "fractional seconds are not whole ones", a: "00:00:01.5", b: "00:00:01"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			equal, err := crdbinterval.Equal(test.a, test.b)

			c.Assert(err, qt.IsNil)
			c.Assert(equal, qt.IsFalse)
		})
	}
}

// TestParse_RefusesWhatItCannotRead pins that an unreadable spelling is an
// error rather than a value.
//
// A declaration this package could not read would differ from every read of the
// table it describes, so the plan would re-issue the change forever. Refusing at
// declaration time is what turns that silent non-convergence into a message.
func TestParse_RefusesWhatItCannotRead(t *testing.T) {
	tests := []struct {
		name    string
		text    string
		wantErr string
	}{
		{
			// Not a zero value: the zero interval is a real declaration,
			// spelled `0 seconds`, so reading emptiness as one would turn a
			// typo into a policy.
			name:    "an empty literal",
			text:    "",
			wantErr: `empty interval`,
		},
		{
			name:    "a quantity with no unit",
			text:    "3",
			wantErr: `(?s).*quantity "3" has no unit.*`,
		},
		{
			name:    "an unknown unit",
			text:    "3 fortnights",
			wantErr: `(?s).*unknown unit "fortnights".*`,
		},
		{
			// A bare `m` is minutes to some tools and months to others. Two
			// different retention policies, so it is refused rather than
			// guessed.
			name:    "an ambiguous abbreviation",
			text:    "3 m",
			wantErr: `(?s).*unknown unit "m".*`,
		},
		{
			name:    "a fractional month, which the server resolves into units Ptah would have to predict",
			text:    "1.5 months",
			wantErr: `(?s).*a fractional months is resolved by the server.*`,
		},
		{
			name:    "a malformed clock time",
			text:    "1:2:3:4",
			wantErr: `(?s).*a clock time is HH:MM or HH:MM:SS.*`,
		},
		{
			name:    "an ISO designator this package does not know",
			text:    "P1Q",
			wantErr: `(?s).*unknown designator "Q".*`,
		},
		{
			name:    "an ISO quantity with no designator",
			text:    "P1Y2",
			wantErr: `(?s).*trailing quantity "2" has no designator.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := crdbinterval.Parse(test.text)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestParse_ReadsEveryFormTheCatalogStores is the narrower guarantee the
// comparison depends on.
//
// The declared side may be refused, and the author then fixes it. The STORED
// side cannot be refused: it comes from the server, and a parse failure there
// would make an existing table uncomparable rather than a declaration
// unwritable. Every spelling the catalog was measured to produce is here.
func TestParse_ReadsEveryFormTheCatalogStores(t *testing.T) {
	stored := []string{
		"3 days",
		"72:00:00",
		"00:05:00",
		"1 day 02:00:00",
		"1 mon",
		"1 year",
		"2 years 3 mons",
		"1 year 2 mons 3 days 04:05:06",
		"00:00:01.5",
		"7 days",
		"14 days",
		"1 day",
		"1 year 2 mons 3 days",
		"01:30:00",
		"00:00:00",
		"00:00:30",
		"00:01:30",
		"1 day 01:01:01",
		"24:00:00",
		"36:00:00",
	}

	for _, text := range stored {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)

			_, err := crdbinterval.Parse(text)

			c.Assert(err, qt.IsNil)
		})
	}
}
