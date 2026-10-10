// Package ydbttl holds the grammar of a YDB table's TTL: the setting that
// makes YDB delete a row once an interval has passed since the time one of its
// columns holds.
//
// The YDB owner models it as [ptah.run/dialect/ydb/ydbschema.TTL]: one
// interval, one column and, for an integer column, a unit. YDB spells it as a
// table setting,
//
//	CREATE TABLE t (...) WITH (TTL = Interval("P30D") ON created_at)
//	ALTER TABLE t SET (TTL = Interval("PT1H") ON expires AS SECONDS)
//	ALTER TABLE t RESET (TTL)
//
// and DescribeTable reads it back as the column and a whole number of
// seconds, with the unit for an integer column. The owner, the renderer, the
// reader and the planner read the setting's grammar through this package, so
// they agree on what an interval and a unit are.
//
// # Measured
//
// On local-ydb 25.1.4.7 and 26.2.1.14 alike:
//
//   - The interval is an ISO 8601 duration of weeks, days, hours, minutes and
//     seconds: `P1W`, `P2W3D` and `P1DT2H30M15S` are accepted, `P1M` and `P1Y`
//     answer `Invalid value "P1M" for type Interval`, and so do `p1d` and
//     `P1D ` with a trailing space. A negative one answers `Interval value
//     cannot be negative`. The largest is one second short of 49673 days.
//   - The server keeps whole seconds and drops the rest in silence: `PT1.5S`
//     reads back as 1 second and `PT0.000001S` as 0. Ptah refuses a fraction
//     of a second rather than declare a policy that never reads back.
//   - A date column takes no unit (Date, Datetime, Timestamp, and the 64-bit
//     Date32, Datetime64 and Timestamp64 where the line has them). An integer
//     column must name one, and must be Uint32, Uint64 or DyNumber: Int64,
//     Int32, the narrower integers, Float and Utf8 answer `Unsupported column
//     type`.
//   - `SET (TTL = ...)` replaces whatever TTL the table had, so adding and
//     changing a policy are one statement. It also resets the run interval,
//     which only the SDK and the CLI can set: a table set to 1800 seconds
//     with `ydb table ttl set --run-interval` reads back with none after it.
//   - A tier that moves rows to an external data source is refused on a row
//     table (`Only DELETE via TTL is allowed for row-oriented tables`), and a
//     DELETE tier may only be the last, so a row table's TTL is always the one
//     interval and column this package models.
package ydbttl

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/internal/ydbtype"
)

// The units an integer TTL column may count in, as YQL spells them after AS.
const (
	Seconds      = "SECONDS"
	Milliseconds = "MILLISECONDS"
	Microseconds = "MICROSECONDS"
	Nanoseconds  = "NANOSECONDS"
)

// units are the accepted units, in the order a refusal lists them.
var units = []string{Seconds, Milliseconds, Microseconds, Nanoseconds}

// MaxSeconds is the longest interval YDB's Interval type holds: measured,
// `PT4291747199S` and `P49672DT23H59M59S` are accepted and `P49673D` answers
// `Invalid value "P49673D" for type Interval`.
const MaxSeconds = 4291747199

// Unit reads a declared unit in any case and returns it as YQL spells it. An
// empty unit stays empty: it is a date column's policy.
func Unit(declared string) (string, error) {
	trimmed := strings.TrimSpace(declared)
	if trimmed == "" {
		return "", nil
	}
	upper := strings.ToUpper(trimmed)
	for _, unit := range units {
		if upper == unit {
			return unit, nil
		}
	}
	return "", fmt.Errorf("unit %q is not one YDB takes: use %s", declared, strings.Join(units, ", "))
}

// IntervalSeconds reads an interval as YDB's Interval literal takes it, an ISO
// 8601 duration of weeks, days, hours, minutes and seconds, and returns the
// whole number of seconds YDB keeps of it. A spelling YDB refuses is refused
// here, and so is a fraction of a second, which YDB would drop in silence.
func IntervalSeconds(interval string) (uint64, error) {
	parsed, err := parseDuration(interval)
	if err != nil {
		return 0, fmt.Errorf("interval %q is not an ISO 8601 duration YDB takes (such as P30D or PT1H30M): %w",
			interval, err)
	}
	if parsed.fraction {
		return 0, fmt.Errorf("interval %q has a fraction of a second, and YDB keeps whole seconds only "+
			"(PT1.5S reads back as PT1S)", interval)
	}
	if parsed.seconds > MaxSeconds {
		return 0, fmt.Errorf("interval %q is longer than YDB's Interval type holds (%s)",
			interval, FormatInterval(MaxSeconds))
	}
	return parsed.seconds, nil
}

// FormatInterval writes a number of seconds the way YDB's SHOW CREATE TABLE
// writes a TTL interval: days, then hours, minutes and seconds, each left out
// when it is zero, and PT0S for no time at all. Measured on 26.2.1.14:
// `P1W` shows as `P7D`, and 1000000000 seconds as `P11574DT1H46M40S`.
func FormatInterval(seconds uint64) string {
	days := seconds / 86400
	rest := seconds % 86400
	hours, minutes, secs := rest/3600, rest%3600/60, rest%60
	var b strings.Builder
	b.WriteString("P")
	if days > 0 {
		b.WriteString(strconv.FormatUint(days, 10) + "D")
	}
	if rest == 0 {
		if days == 0 {
			return "PT0S"
		}
		return b.String()
	}
	b.WriteString("T")
	if hours > 0 {
		b.WriteString(strconv.FormatUint(hours, 10) + "H")
	}
	if minutes > 0 {
		b.WriteString(strconv.FormatUint(minutes, 10) + "M")
	}
	if secs > 0 {
		b.WriteString(strconv.FormatUint(secs, 10) + "S")
	}
	return b.String()
}

// Setting writes a TTL as the value of YQL's TTL setting, without the leading
// `TTL =`: the interval, ON and the quoted column, and for an integer column
// AS and the unit, as in `Interval("PT1H") ON expires AS SECONDS`. The interval
// is written as the author declared it, which YDB reads to the same seconds as
// the form it shows. The caller validates the parts first; quote writes the
// column name as an identifier.
func Setting(column, interval, unit string, quote func(string) string) string {
	setting := fmt.Sprintf(`Interval("%s") ON %s`, interval, quote(column))
	if unit != "" {
		setting += " AS " + unit
	}
	return setting
}

// ColumnRefusal says why a column of the given YDB type cannot carry a policy
// with unit, or returns the empty string when it can. The answers are the
// server's, measured on 25.1.4.7 and 26.2.1.14.
func ColumnRefusal(column, ydbType, unit string) string {
	switch {
	case dateTypes[ydbType] && unit != "":
		return fmt.Sprintf("column %q is %s, a date type, and YDB takes no unit for one (`To enable TTL on date type "+
			"column 'DateTypeColumnModeSettings' should be specified`)", column, ydbType)
	case dateTypes[ydbType]:
		return ""
	case epochTypes[ydbType] && unit == "":
		return fmt.Sprintf("column %q is %s, an integer type, and YDB needs the unit it counts since the Unix epoch "+
			"(`To enable TTL on integral type column 'ValueSinceUnixEpochModeSettings' should be specified`)", column, ydbType)
	case epochTypes[ydbType]:
		return ""
	default:
		return fmt.Sprintf("column %q is %s, and YDB reads a TTL from a Date, Datetime, Timestamp, Date32, Datetime64 or "+
			"Timestamp64 column, or from a Uint32, Uint64 or DyNumber column with a unit (`Unsupported column type`)",
			column, ydbType)
	}
}

// dateTypes are the column types a policy reads without a unit.
var dateTypes = map[string]bool{
	ydbtype.Date: true, ydbtype.Datetime: true, ydbtype.Timestamp: true,
	ydbtype.Date32: true, ydbtype.Datetime64: true, ydbtype.Timestamp64: true,
}

// epochTypes are the column types a policy reads with a unit.
var epochTypes = map[string]bool{ydbtype.Uint32: true, ydbtype.Uint64: true, ydbtype.DyNumber: true}

// duration is an ISO 8601 duration as YDB's Interval literal reads it.
type duration struct {
	seconds  uint64
	fraction bool
}

// parseDuration reads `P[nW][nD][T[nH][nM][n[.f]S]]`, with at least one part
// and the T present only when a time part follows it, which is the grammar
// YDB's Interval literal takes: measured, `P`, `PT`, `P1DT`, `PT1H1H`, `P1M`
// and `P1Y` are refused, and `P2W3D` is accepted.
func parseDuration(text string) (duration, error) {
	rest, ok := strings.CutPrefix(text, "P")
	if !ok {
		return duration{}, fmt.Errorf("it does not begin with P")
	}
	datePart, timePart, hasTime := strings.Cut(rest, "T")
	if hasTime && timePart == "" {
		return duration{}, fmt.Errorf("T is not followed by hours, minutes or seconds")
	}
	var parsed duration
	parts := 0
	read := func(part string, designators []designator) error {
		next := 0
		for part != "" {
			end := strings.IndexFunc(part, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
			if end <= 0 {
				return fmt.Errorf("%q is not a number followed by a designator", part)
			}
			number, letter := part[:end], part[end]
			index := designatorIndex(designators, letter, next)
			if index < 0 {
				return fmt.Errorf("designator %q is out of place or not one YDB takes (weeks, days, hours, minutes, seconds)",
					string(letter))
			}
			whole, decimals, err := readNumber(number)
			if err != nil {
				return err
			}
			if decimals != "" && !designators[index].fractional {
				return fmt.Errorf("%q is not a number YDB takes there", number)
			}
			parsed.seconds += whole * designators[index].seconds
			parsed.fraction = parsed.fraction || strings.Trim(decimals, "0") != ""
			parts++
			next = index + 1
			part = part[end+1:]
		}
		return nil
	}
	if err := read(datePart, dateDesignators); err != nil {
		return duration{}, err
	}
	if err := read(timePart, timeDesignators); err != nil {
		return duration{}, err
	}
	if parts == 0 {
		return duration{}, fmt.Errorf("it names no weeks, days, hours, minutes or seconds")
	}
	return parsed, nil
}

// designator is one unit letter of a duration.
type designator struct {
	letter     byte
	seconds    uint64
	fractional bool
}

var (
	dateDesignators = []designator{{letter: 'W', seconds: 7 * 86400}, {letter: 'D', seconds: 86400}}
	timeDesignators = []designator{
		{letter: 'H', seconds: 3600}, {letter: 'M', seconds: 60}, {letter: 'S', seconds: 1, fractional: true},
	}
)

// designatorIndex finds letter among designators at or after from, so each
// appears once and in order.
func designatorIndex(designators []designator, letter byte, from int) int {
	for i := from; i < len(designators); i++ {
		if designators[i].letter == letter {
			return i
		}
	}
	return -1
}

// readNumber reads a duration part's number: its whole part, and the digits
// after a decimal point, which only seconds may carry.
func readNumber(number string) (whole uint64, decimals string, _ error) {
	integer, decimals, hasDecimals := strings.Cut(number, ".")
	if hasDecimals && (decimals == "" || strings.Contains(decimals, ".")) {
		return 0, "", fmt.Errorf("%q is not a number YDB takes there", number)
	}
	if integer == "" {
		return 0, "", fmt.Errorf("%q has no whole part", number)
	}
	value, err := strconv.ParseUint(integer, 10, 64)
	if err != nil || value > MaxSeconds {
		return 0, "", fmt.Errorf("%q is too large", number)
	}
	return value, decimals, nil
}
