// Package spannerttl reads and writes Spanner's row deletion policy clause, the
// one that makes the engine delete a row once an interval has passed since a
// timestamp column. The policy itself is the Spanner owner's facet,
// ptah.run/dialect/spanner/spannerschema; this package is its spelling.
//
// Spanner is the engine that has it. It is spelled as a table clause,
//
//	CREATE TABLE t (...) TTL INTERVAL '30 days' ON created_at
//
// and read back from information_schema.tables.row_deletion_policy_expression,
// which reports the clause without the leading TTL keyword.
//
// # The interval is not compared as text
//
// The server rewrites it, into a mixed-radix rendering of the same number of
// days. Measured against the Cloud Spanner emulator behind PGAdapter 0.55.2,
// declaring one policy per table and reading
// information_schema.tables.row_deletion_policy_expression back:
//
//	declared      stored
//	1 days        24 HOURS
//	7 days        7 DAYS
//	14 days       2 WEEKS
//	29 days       4 WEEKS 24 HOURS
//	30 days       4 WEEKS 2 DAYS
//	31 days       1 MONTHS 24 HOURS
//	60 days       2 MONTHS
//	90 days       3 MONTHS
//	365 days      12 MONTHS 5 DAYS
//	1 hour        refused: TTL interval must be a whole number of days
//
// So a declaration compared as TEXT against the catalog can never converge.
//
// # Why this does not reuse crdbinterval
//
// [ptah.run/internal/crdbinterval] exists for the same shape of problem
// on CockroachDB and is the WRONG answer here, which the table above is what
// shows. It compares PostgreSQL's (months, days, microseconds) triple and
// deliberately refuses to convert between the three, because in PostgreSQL
// `1 mon` is not 30 days and `1 day` is not 24 hours across a DST boundary.
// This clause is not a PostgreSQL interval: every row above reduces to a whole
// number of days at exactly 30 days to a month, 7 to a week and 24 hours to a
// day, and the server itself does the reducing. Under the triple, `60 days` and
// the `2 MONTHS` the server stored for it are different values, and a
// comparison built on it reports a difference between a database and its own
// description forever -- which is measured: `ALTER TTL INTERVAL '60 days'` was
// planned again immediately after being applied successfully.
//
// So the units here are the server's, and they are fixed rather than
// discovered: hours are the smallest one that appears, and the total is
// compared in hours so that nothing has to be expressed as a fraction
// (stokaro/ptah#2236).
package spannerttl

import (
	"fmt"
	"strconv"
	"strings"
)

// Expression is a policy as the catalog reports it: the column it reads, and
// the interval in the spelling the server stored.
type Expression struct {
	Column   string
	Interval string
}

// ParseExpression reads the catalog's policy expression.
//
// An empty expression is a table with no policy, which is not an error: it is
// what every table that never declared one reports, and found is false.
func ParseExpression(expression string) (parsed Expression, found bool, err error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return Expression{}, false, nil
	}
	rest, ok := cutFold(trimmed, "INTERVAL")
	if !ok {
		return Expression{}, false, unreadable(expression, "it does not begin with INTERVAL")
	}
	interval, rest, ok := cutQuoted(strings.TrimSpace(rest))
	if !ok {
		return Expression{}, false, unreadable(expression, "the interval is not a quoted literal")
	}
	column, ok := cutFold(strings.TrimSpace(rest), "ON")
	if !ok {
		return Expression{}, false, unreadable(expression, "the interval is not followed by ON")
	}
	column = strings.TrimSpace(column)
	if column == "" {
		return Expression{}, false, unreadable(expression, "ON names no column")
	}
	return Expression{Column: unquoteIdentifier(column), Interval: interval}, true, nil
}

// Clause is the policy as the text after a table's closing parenthesis or
// after ADD or ALTER: `TTL INTERVAL '30 days' ON "created_at"`.
//
// The interval is emitted as the author wrote it. Rendering the parsed value
// instead would write Ptah's spelling into the operator's DDL, which is what
// the comparison exists to avoid having to do.
func Clause(column, interval string, quoteIdentifier func(string) string) string {
	return fmt.Sprintf("TTL INTERVAL '%s' ON %s", interval, quoteIdentifier(column))
}

// ValidateInterval refuses an interval the server refuses or this package
// cannot compare: one it cannot read, a negative one, and one that is not a
// whole number of days (`TTL interval must be a whole number of days`,
// measured on the emulator). Zero days is accepted, as the server accepts it.
//
// An interval this package cannot read is refused rather than passed through,
// although the server may accept it: the server stores its own spelling, and a
// declaration compared with that as text would plan the same change on every
// run.
func ValidateInterval(interval string) error {
	hours, ok := IntervalHours(interval)
	switch {
	case !ok:
		return fmt.Errorf("interval %q is not a number of months, weeks, days or hours, such as 30 days", interval)
	case hours < 0:
		return fmt.Errorf("interval %q is negative", interval)
	case hours%24 != 0:
		return fmt.Errorf("interval %q is not a whole number of days, which Spanner refuses", interval)
	}
	return nil
}

// EqualIntervals reports whether two intervals are the same number of hours.
//
// An interval either side cannot parse falls back to comparing the two as text,
// rather than reporting a difference. The fallback is the conservative
// direction and it converges: two spellings this package cannot read but that
// are identical are the same policy, and two that differ plan a change that
// makes them identical. Reporting "differs" for every unreadable spelling would
// plan that change on every run forever, which is the failure this comparison
// exists to prevent.
func EqualIntervals(a, b string) bool {
	left, leftOK := IntervalHours(a)
	right, rightOK := IntervalHours(b)
	if !leftOK || !rightOK {
		return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
	}
	return left == right
}

// unitHours is the server's own arithmetic, read off the measurements in the
// package comment: a month is 30 days, a week is 7, a day is 24 hours. Nothing
// smaller than an hour appears, because the server refuses an interval that is
// not a whole number of days.
var unitHours = map[string]int64{
	"month": 30 * 24, "months": 30 * 24,
	"week": 7 * 24, "weeks": 7 * 24,
	"day": 24, "days": 24,
	"hour": 1, "hours": 1,
}

// IntervalHours reduces an interval literal to hours, and reports false for a
// spelling it does not recognize so the caller can fall back rather than
// pretend a number.
func IntervalHours(interval string) (int64, bool) {
	fields := strings.Fields(strings.TrimSpace(interval))
	if len(fields) == 0 || len(fields)%2 != 0 {
		return 0, false
	}
	var total int64
	for i := 0; i < len(fields); i += 2 {
		quantity, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return 0, false
		}
		hours, known := unitHours[strings.ToLower(fields[i+1])]
		if !known {
			return 0, false
		}
		total += quantity * hours
	}
	return total, true
}

// cutFold removes a leading keyword, whatever case the catalog printed it in.
func cutFold(text, keyword string) (string, bool) {
	if len(text) < len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) {
		return "", false
	}
	return text[len(keyword):], true
}

// cutQuoted reads a single-quoted literal and returns it with what follows.
func cutQuoted(text string) (literal, rest string, ok bool) {
	if !strings.HasPrefix(text, "'") {
		return "", "", false
	}
	end := strings.Index(text[1:], "'")
	if end < 0 {
		return "", "", false
	}
	return text[1 : end+1], text[end+2:], true
}

// unquoteIdentifier drops the double quotes a catalog puts around a column name
// that needs them, so the model carries the name rather than one spelling of it.
func unquoteIdentifier(name string) string {
	if len(name) >= 2 && strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`) {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return name
}

func unreadable(expression, why string) error {
	return fmt.Errorf("row deletion policy %q cannot be read: %s", expression, why)
}
