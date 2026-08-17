// Package crdbinterval reads a CockroachDB interval literal into the value it
// denotes, so two spellings of the same interval can be compared.
//
// It exists for one reason: CockroachDB REWRITES the interval a table stores.
// Measured on CockroachDB CCL v25.4.14 and v26.2.5, declaring
// `ttl_expire_after` and reading pg_class.reloptions back:
//
//	declared                        stored
//	'3 days'                        '3 days'
//	'72 hours'                      '72:00:00'
//	'5 minutes'                     '00:05:00'
//	'1 day 2 hours'                 '1 day 02:00:00'
//	'1 week'                        '7 days'
//	'2 years 3 months'              '2 years 3 mons'
//	'P1Y2M3D'                       '1 year 2 mons 3 days'
//	'PT1H30M'                       '01:30:00'
//	'1.5 hours'                     '01:30:00'
//	'1500 milliseconds'             '00:00:01.5'
//
// So a declaration compared as TEXT against the catalog can never converge, and
// stokaro/ptah#1027 refused the parameter rather than accept that. This package
// is the other half of the answer: rather than predicting the stored spelling —
// which would mean re-implementing PostgreSQL's interval RENDERING and getting
// every case right forever — it parses BOTH sides into the value they denote and
// compares those. Rendering stays verbatim: what the author wrote is what Ptah
// sends.
//
// # The value is a triple, not a duration
//
// PostgreSQL intervals are (months, days, microseconds) and the three do NOT
// convert into one another: `1 mon` is not 30 days, and `1 day` is not 24 hours
// across a DST boundary. The server's own normalization respects that, which the
// measurements above show — `72 hours` stays 72 hours rather than becoming
// `3 days`, and `2 years 3 mons` stays in months rather than becoming days. A
// comparison that collapsed the triple into one number would call `1 mon` and
// `30 days` equal and report convergence for a policy that expires rows on a
// different schedule.
//
// Weeks are the exception, and that is the server's own rule rather than one
// invented here: `1 week` is stored as `7 days`, so weeks fold into days on the
// way in.
//
// # What it refuses
//
// A spelling this package cannot read is REFUSED at declaration time rather than
// compared as text. The alternative is a silent non-convergence: an unparsable
// declaration would differ from every read of the table it describes, and the
// plan would re-issue the change forever. The accepted forms are the ones the
// measurements above cover, which is the surface CockroachDB's own documentation
// uses.
package crdbinterval

import (
	"fmt"
	"strconv"
	"strings"
)

// Value is what an interval literal denotes: a count of months, a count of
// days, and a count of microseconds, kept apart because PostgreSQL keeps them
// apart.
//
// Years are folded into months and weeks into days, because the server folds
// both: `2 years 3 mons` and `27 mons` are the same stored value, as are
// `1 week` and `7 days`. Hours are NOT folded into days, because the server
// does not fold them either — `72 hours` stays `72:00:00`.
type Value struct {
	// Months counts calendar months, including years at twelve months each.
	Months int64
	// Days counts calendar days, including weeks at seven days each.
	Days int64
	// Micros counts microseconds, including hours and minutes.
	Micros int64
}

const (
	microsPerSecond = int64(1_000_000)
	microsPerMinute = 60 * microsPerSecond
	microsPerHour   = 60 * microsPerMinute
	monthsPerYear   = int64(12)
	daysPerWeek     = int64(7)
)

// Parse reads an interval literal into the value it denotes.
//
// It accepts the two spellings CockroachDB stores and the ones its
// documentation uses to declare them: a sequence of quantity-and-unit pairs
// optionally followed by a clock time (`1 day 02:00:00`, `72 hours`,
// `2 years 3 mons`), a bare clock time (`00:05:00`), and ISO-8601 duration form
// (`P1Y2M3D`, `PT1H30M`).
//
// An empty string is not an interval and is an error rather than a zero value:
// the zero interval is a real declaration, spelled `0 seconds`, and reading a
// missing value as one would turn a typo into a policy.
func Parse(text string) (Value, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return Value{}, fmt.Errorf("empty interval")
	}
	if isISO(trimmed) {
		return parseISO(trimmed)
	}
	return parseVerbose(trimmed)
}

// Equal reports whether two literals denote the same interval.
//
// Both sides must parse. A caller comparing a declaration against a catalog
// value has already refused an unparsable declaration, and the catalog value is
// the server's own canonical form, so a parse failure here is a caller that
// skipped the refusal rather than an ordinary case.
func Equal(a, b string) (bool, error) {
	left, err := Parse(a)
	if err != nil {
		return false, err
	}
	right, err := Parse(b)
	if err != nil {
		return false, err
	}
	return left == right, nil
}

// isISO reports whether the literal is in ISO-8601 duration form. The check is
// the leading P, which no verbose spelling starts with.
func isISO(text string) bool {
	return len(text) > 0 && (text[0] == 'P' || text[0] == 'p')
}

// parseVerbose reads `[N unit]... [HH:MM:SS[.f]]`, which covers every form the
// catalog stores.
func parseVerbose(text string) (Value, error) {
	var value Value
	fields := strings.Fields(text)
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if strings.Contains(field, ":") {
			clock, err := parseClock(field)
			if err != nil {
				return Value{}, err
			}
			value.Micros += clock
			continue
		}
		if i+1 >= len(fields) {
			return Value{}, unreadable(text, fmt.Sprintf("quantity %q has no unit", field))
		}
		if err := addQuantity(&value, field, fields[i+1], text); err != nil {
			return Value{}, err
		}
		i++
	}
	return value, nil
}

// addQuantity applies one `N unit` pair.
func addQuantity(value *Value, quantity, unit, text string) error {
	amount, err := strconv.ParseFloat(quantity, 64)
	if err != nil {
		return unreadable(text, fmt.Sprintf("%q is not a number", quantity))
	}
	scale, known := unitScale(unit)
	if !known {
		return unreadable(text, fmt.Sprintf("unknown unit %q", unit))
	}
	return scale(value, amount, unit, text)
}

// scaler applies a quantity in one unit to a value.
type scaler func(*Value, float64, string, string) error

// unitScale resolves a unit word to the field it contributes to.
//
// Abbreviations the server accepts but that are ambiguous to a reader -- a bare
// `m` is minutes to some tools and months to others -- are deliberately absent.
// A declaration using one is refused with the list of accepted spellings, which
// is a better outcome than guessing which of two policies was meant.
func unitScale(unit string) (scaler, bool) {
	switch strings.ToLower(strings.TrimSuffix(unit, ",")) {
	case "year", "years", "yr", "yrs":
		return whole(func(v *Value, n int64) { v.Months += n * monthsPerYear }), true
	case "mon", "mons", "month", "months":
		return whole(func(v *Value, n int64) { v.Months += n }), true
	case "week", "weeks", "w":
		return whole(func(v *Value, n int64) { v.Days += n * daysPerWeek }), true
	case "day", "days", "d":
		return whole(func(v *Value, n int64) { v.Days += n }), true
	case "hour", "hours", "hr", "hrs", "h":
		return fractional(microsPerHour), true
	case "minute", "minutes", "min", "mins":
		return fractional(microsPerMinute), true
	case "second", "seconds", "sec", "secs", "s":
		return fractional(microsPerSecond), true
	case "millisecond", "milliseconds", "ms":
		return fractional(microsPerSecond / 1000), true
	case "microsecond", "microseconds", "us":
		return fractional(1), true
	default:
		return nil, false
	}
}

// whole applies a unit that has no sub-unit to spill into. A fractional month
// or day is refused rather than rounded: the server resolves it into a mixture
// of units this package would then have to predict, and predicting the stored
// spelling is exactly what this package exists to avoid.
func whole(apply func(*Value, int64)) scaler {
	return func(value *Value, amount float64, unit, text string) error {
		if amount != float64(int64(amount)) {
			return unreadable(text, fmt.Sprintf(
				"a fractional %s is resolved by the server into units Ptah would have to predict", unit))
		}
		apply(value, int64(amount))
		return nil
	}
}

// fractional applies a unit that spills cleanly into microseconds.
func fractional(micros int64) scaler {
	return func(value *Value, amount float64, _, _ string) error {
		value.Micros += int64(amount * float64(micros))
		return nil
	}
}

// parseClock reads `HH:MM:SS[.f]`, and the two-part `HH:MM` the server also
// accepts. The hour field is not bounded by 24: the catalog stores `72:00:00`
// for three days' worth of hours rather than folding them into days.
func parseClock(field string) (int64, error) {
	parts := strings.Split(field, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, unreadable(field, "a clock time is HH:MM or HH:MM:SS")
	}
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, unreadable(field, fmt.Sprintf("%q is not an hour count", parts[0]))
	}
	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, unreadable(field, fmt.Sprintf("%q is not a minute count", parts[1]))
	}
	micros := hours*microsPerHour + minutes*microsPerMinute
	if len(parts) == 2 {
		return micros, nil
	}
	seconds, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return 0, unreadable(field, fmt.Sprintf("%q is not a second count", parts[2]))
	}
	return micros + int64(seconds*float64(microsPerSecond)), nil
}

// parseISO reads the ISO-8601 duration form, which the server accepts and
// normalizes into its own spelling: measured, `P1Y2M3D` is stored as
// `1 year 2 mons 3 days` and `PT1H30M` as `01:30:00`.
//
// The M in the date half is months and the M in the time half is minutes, which
// is why the two halves are read separately rather than by one unit table.
func parseISO(text string) (Value, error) {
	body := text[1:]
	datePart, timePart, hasTime := strings.Cut(body, "T")
	if !hasTime {
		datePart, timePart = body, ""
	}

	var value Value
	if err := scanISO(datePart, text, map[byte]scaler{
		'Y': whole(func(v *Value, n int64) { v.Months += n * monthsPerYear }),
		'M': whole(func(v *Value, n int64) { v.Months += n }),
		'W': whole(func(v *Value, n int64) { v.Days += n * daysPerWeek }),
		'D': whole(func(v *Value, n int64) { v.Days += n }),
	}, &value); err != nil {
		return Value{}, err
	}
	if err := scanISO(timePart, text, map[byte]scaler{
		'H': fractional(microsPerHour),
		'M': fractional(microsPerMinute),
		'S': fractional(microsPerSecond),
	}, &value); err != nil {
		return Value{}, err
	}
	return value, nil
}

// scanISO reads `<number><designator>` pairs out of one half of an ISO
// duration.
func scanISO(part, text string, units map[byte]scaler, value *Value) error {
	digits := strings.Builder{}
	for i := range len(part) {
		char := part[i]
		if (char >= '0' && char <= '9') || char == '.' || char == '-' {
			digits.WriteByte(char)
			continue
		}
		scale, known := units[char]
		if !known {
			return unreadable(text, fmt.Sprintf("unknown designator %q", string(char)))
		}
		amount, err := strconv.ParseFloat(digits.String(), 64)
		if err != nil {
			return unreadable(text, fmt.Sprintf("%q is not a number", digits.String()))
		}
		digits.Reset()
		if err := scale(value, amount, string(char), text); err != nil {
			return err
		}
	}
	if digits.Len() > 0 {
		return unreadable(text, fmt.Sprintf("trailing quantity %q has no designator", digits.String()))
	}
	return nil
}

// unreadable is the one error shape this package returns, so a caller can wrap
// it once and every refusal reads the same.
//
// It names the accepted forms rather than only the problem, because the author
// is looking at a value the SERVER would accept and needs to know that Ptah's
// surface is the narrower one.
func unreadable(text, why string) error {
	return fmt.Errorf(
		"interval %q cannot be read: %s. Ptah reads a sequence of quantity-and-unit pairs "+
			"(years, mons, weeks, days, hours, minutes, seconds, milliseconds, microseconds), "+
			"an optional trailing HH:MM:SS, and the ISO-8601 form such as P1Y2M3D or PT1H30M",
		text, why)
}
