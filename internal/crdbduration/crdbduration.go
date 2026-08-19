// Package crdbduration converts a CockroachDB duration-valued storage parameter
// into the form the server stores, and refuses the values it cannot represent.
//
// It exists for `ttl_row_stats_poll_interval`, whose value the server does not
// keep as written. Every fact below was measured against live CockroachDB CCL
// v26.2.5 rather than read off a manual.
//
// The server accepts the FULL interval syntax and stores a Go duration string:
//
//	declared              stored
//	'600s'                '10m0s'
//	'5 minutes'           '5m0s'
//	'00:10:00'            '10m0s'
//	'PT10M'               '10m0s'
//	'2h45m30s'            '2h45m30s'
//	'90s'                 '1m30s'
//
// Calendar units are folded into a duration on the server's own terms, which
// are not the terms an interval uses: a month is 30 days, but a YEAR is 365.25
// days rather than twelve of those months.
//
//	'1 day'               '24h0m0s'
//	'1 week'              '168h0m0s'
//	'1 month'             '720h0m0s'
//	'11 months'           '7920h0m0s'     (11 x 720h)
//	'12 months'           '8766h0m0s'     (one year, NOT 12 x 720h)
//	'1 year'              '8766h0m0s'
//	'23 months'           '16686h0m0s'    (8766h + 11 x 720h)
//	'25 months'           '18252h0m0s'    (2 x 8766h + 720h)
//	'1 year 1 month'      '9486h0m0s'
//
// Sub-second precision is TRUNCATED toward zero, not rounded, and a value that
// truncates to zero is stored NOWHERE AT ALL -- the parameter simply does not
// appear on the table:
//
//	'2500ms'              '2s'            (truncated; rounding would give 3s)
//	'1999ms'              '1s'
//	'500ms'               absent
//	'0s'                  absent
//
// The accepted range is exactly Go's [time.Duration]: the server keeps the
// value as int64 nanoseconds and checks the sign afterwards, so a value past
// the maximum wraps and is answered with a message about the wrong thing:
//
//	'2562047h'            '2562047h0m0s'
//	'2562048h'            ERROR: "ttl_row_stats_poll_interval" must be at least 0
//	'292 years'           '2559672h0m0s'
//	'293 years'           ERROR: "ttl_row_stats_poll_interval" must be at least 0
//	'-5s'                 ERROR: "ttl_row_stats_poll_interval" must be at least 0
//
// [Canonical] refuses each of those before any SQL is produced, naming what the
// server would do rather than letting the server answer an overflow as if the
// operator had written a negative number.
package crdbduration

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/crdbinterval"
)

// The measured calendar constants. A month is thirty days and a year is 365.25
// days; twelve months is therefore a year rather than twelve month-lengths,
// which is why [fromInterval] divides before it multiplies.
const (
	hoursPerDay   = 24
	hoursPerMonth = 720
	hoursPerYear  = 8766
	monthsPerYear = 12
)

// Canonical returns the value CockroachDB stores for a declared duration.
//
// The returned text is what a later catalog read finds, so a declaration
// normalized through this function compares equal to the table it produced.
// Every failure is a value the server would not keep as written, and is
// returned as an error rather than normalized into something else: a poll
// interval silently changed is a TTL job running at a cadence nobody asked for.
func Canonical(text string) (string, error) {
	parsed, err := parse(text)
	if err != nil {
		return "", err
	}
	// Truncation before the sign check, so a value between -1s and 0 is
	// reported as the sub-second value it is rather than as a negative one.
	truncated := parsed.Truncate(time.Second)
	if truncated < 0 {
		return "", fmt.Errorf("%q is negative; CockroachDB requires the interval to be at least 0", strings.TrimSpace(text))
	}
	if truncated == 0 {
		return "", fmt.Errorf(
			"%q is below one second; CockroachDB truncates the value to whole seconds and stores no parameter at all when that leaves zero, so the declaration could never be read back",
			strings.TrimSpace(text))
	}
	return truncated.String(), nil
}

// parse reads any spelling the server accepts into the duration it denotes.
//
// The Go duration form is tried first because it is the one the server STORES,
// so it is the spelling a catalog read hands back and the one a declaration
// copied from a table will carry.
func parse(text string) (time.Duration, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0, errors.New("empty duration")
	}
	if parsed, err := time.ParseDuration(trimmed); err == nil {
		return parsed, nil
	}
	// [time.ParseDuration] rejects a well-formed Go duration for exactly one
	// other reason: the value does not fit an int64 of nanoseconds. Reporting
	// that as an unreadable spelling would send the operator looking for a typo
	// in a duration that is merely too long.
	if isGoDuration(trimmed) {
		return 0, overflow(trimmed)
	}
	value, err := crdbinterval.Parse(intervalSpelling(trimmed))
	if err != nil {
		return 0, err
	}
	return fromInterval(value, trimmed)
}

// goDuration matches the grammar [time.ParseDuration] documents, so a string it
// accepts has failed on range rather than on syntax.
var goDuration = regexp.MustCompile(`^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)` +
	`(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h))*$`)

func isGoDuration(text string) bool {
	return goDuration.MatchString(text)
}

// intervalSpelling rewrites the two spellings CockroachDB accepts for this
// parameter that [crdbinterval] does not read, both measured on v26.2.5:
// `10minutes`, whose quantity and unit are not separated, and `10 m`, whose
// unit crdbinterval refuses as ambiguous between months and minutes.
//
// The refusal is right for an interval and wrong here. This parameter holds a
// duration, and the server resolves the ambiguity itself: `10 m` is stored as
// `10m0s`, ten minutes, not ten months -- which would have been `7200h0m0s`.
//
// ISO-8601 form is returned untouched. Its units are letters that follow digits
// by design, so separating them would turn `PT10M` into something no parser
// reads.
func intervalSpelling(text string) string {
	if text[0] == 'P' || text[0] == 'p' {
		return text
	}
	var separated strings.Builder
	for index, character := range text {
		if index > 0 && isLetter(character) && isDigit(rune(text[index-1])) {
			separated.WriteByte(' ')
		}
		separated.WriteRune(character)
	}
	fields := strings.Fields(separated.String())
	for index, field := range fields {
		if field == "m" || field == "M" {
			fields[index] = "minutes"
		}
	}
	return strings.Join(fields, " ")
}

func isLetter(character rune) bool {
	return (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z')
}

func isDigit(character rune) bool {
	return character >= '0' && character <= '9'
}

// fromInterval applies the server's calendar conversion, refusing the values
// that do not fit.
//
// Overflow is detected rather than allowed to wrap. The server wraps and then
// reports "must be at least 0", which names the sign of a number the operator
// never wrote; refusing here says what actually happened.
func fromInterval(value crdbinterval.Value, text string) (time.Duration, error) {
	years := value.Months / monthsPerYear
	months := value.Months % monthsPerYear

	hours, err := addAll(
		mul(years, hoursPerYear),
		mul(months, hoursPerMonth),
		mul(value.Days, hoursPerDay),
	)
	if err != nil {
		return 0, overflow(text)
	}
	nanos, err := addAll(mul(hours, int64(time.Hour)), mul(value.Micros, int64(time.Microsecond)))
	if err != nil {
		return 0, overflow(text)
	}
	return time.Duration(nanos), nil
}

func overflow(text string) error {
	return fmt.Errorf(
		"%q is longer than %s, the largest interval CockroachDB can store for this parameter; past it the server wraps and answers that the interval must be at least 0",
		text, time.Duration(math.MaxInt64))
}

// errOverflow marks an arithmetic result that did not fit. It never reaches a
// caller: [fromInterval] replaces it with the message naming the limit.
var errOverflow = errors.New("overflow")

// result carries a value that may have overflowed, so the conversion above
// reads as the arithmetic it is rather than as a chain of error checks.
type result struct {
	value int64
	err   error
}

// mul multiplies with an overflow check, rather than letting int64 wrap the way
// the server does.
func mul(a, b int64) result {
	if a == 0 || b == 0 {
		return result{}
	}
	product := a * b
	if product/b != a {
		return result{err: errOverflow}
	}
	return result{value: product}
}

// addAll sums results, propagating any overflow and checking its own additions.
func addAll(values ...result) (int64, error) {
	total := int64(0)
	for _, value := range values {
		if value.err != nil {
			return 0, value.err
		}
		sum := total + value.value
		// Signs agreeing on the operands but not on the sum is the wrap.
		if (value.value > 0 && sum < total) || (value.value < 0 && sum > total) {
			return 0, errOverflow
		}
		total = sum
	}
	return total, nil
}
