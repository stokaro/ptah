package chrefresh

import (
	"fmt"
	"strconv"
	"strings"
)

// unit is one ClickHouse interval unit and the ladder it belongs to.
type unit struct {
	name string
	// size is the unit's magnitude in the ladder's base: seconds for a clock
	// unit, months for a calendar one.
	size int64
	// calendar reports which ladder the unit belongs to. The two never mix in
	// one interval; see the package doc.
	calendar bool
}

// units is the ladder, largest first, which is also decomposition order.
//
// WEEK is the largest clock unit and MONTH the smallest calendar one, which is
// the boundary the server refuses to cross in either direction: a month has no
// fixed length in seconds, so five weeks stay five weeks.
var units = []unit{
	{name: "YEAR", size: 12, calendar: true},
	{name: "QUARTER", size: 3, calendar: true},
	{name: "MONTH", size: 1, calendar: true},
	{name: "WEEK", size: 604800},
	{name: "DAY", size: 86400},
	{name: "HOUR", size: 3600},
	{name: "MINUTE", size: 60},
	{name: "SECOND", size: 1},
}

func lookupUnit(name string) (unit, bool) {
	// A trailing S is accepted so a declaration may read `2 HOURS`; the server
	// stores the singular either way.
	normalized := strings.ToUpper(strings.TrimSpace(name))
	normalized = strings.TrimSuffix(normalized, "S")
	for _, candidate := range units {
		if candidate.name == normalized {
			return candidate, true
		}
	}
	return unit{}, false
}

// CanonicalInterval returns the interval as the server stores it.
//
// The input is the text between a REFRESH keyword and the next clause --
// `90 SECOND`, `1 MINUTE 30 SECOND`, `2 HOURS` -- and the output is the
// spelling create_table_query would report for it. See the package doc for the
// measured table this reproduces.
func CanonicalInterval(declared string) (string, error) {
	fields := strings.Fields(declared)
	if len(fields) == 0 {
		return "", fmt.Errorf("refresh interval is empty")
	}
	if len(fields)%2 != 0 {
		return "", fmt.Errorf("refresh interval %q is not a sequence of <count> <unit> terms", declared)
	}

	var clockSeconds, calendarMonths int64
	var sawClock, sawCalendar bool
	for i := 0; i < len(fields); i += 2 {
		count, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			return "", fmt.Errorf("refresh interval %q: %q is not a count", declared, fields[i])
		}
		if count < 0 {
			return "", fmt.Errorf("refresh interval %q: interval must be positive", declared)
		}
		found, ok := lookupUnit(fields[i+1])
		if !ok {
			return "", fmt.Errorf("refresh interval %q: %q is not an interval unit", declared, fields[i+1])
		}
		if found.calendar {
			sawCalendar = true
			calendarMonths += count * found.size
			continue
		}
		sawClock = true
		clockSeconds += count * found.size
	}

	// Both refusals are the server's own, reproduced here so a declaration is
	// answered before a statement is sent rather than after.
	if sawClock && sawCalendar {
		return "", fmt.Errorf(
			"refresh interval %q: interval shouldn't contain both calendar units and clock units", declared)
	}
	if clockSeconds == 0 && calendarMonths == 0 {
		return "", fmt.Errorf("refresh interval %q: interval must be positive", declared)
	}

	if sawCalendar {
		return decomposeCalendar(calendarMonths), nil
	}
	return decomposeClock(clockSeconds), nil
}

// decomposeCalendar writes a month count as YEAR and MONTH terms.
func decomposeCalendar(months int64) string {
	return decompose(months, calendarUnits())
}

// decomposeClock writes a second count as WEEK down to SECOND terms.
func decomposeClock(seconds int64) string {
	return decompose(seconds, clockUnits())
}

// calendarUnits and clockUnits are the two decomposition ladders, each in the
// order the server emits them. They are separate slices rather than one filtered
// by a flag because they are separate ladders: the server refuses to combine
// them, so nothing should be able to ask for "both" or pick between them at run
// time.
func calendarUnits() []unit {
	return []unit{
		{name: "YEAR", size: 12, calendar: true},
		{name: "MONTH", size: 1, calendar: true},
	}
}

func clockUnits() []unit {
	return []unit{
		{name: "WEEK", size: 604800},
		{name: "DAY", size: 86400},
		{name: "HOUR", size: 3600},
		{name: "MINUTE", size: 60},
		{name: "SECOND", size: 1},
	}
}

// decompose writes total in the largest units of ladder that divide it,
// largest first, emitting only the non-zero terms.
func decompose(total int64, ladder []unit) string {
	var terms []string
	for _, candidate := range ladder {
		count := total / candidate.size
		if count == 0 {
			continue
		}
		total -= count * candidate.size
		terms = append(terms, strconv.FormatInt(count, 10)+" "+candidate.name)
	}
	return strings.Join(terms, " ")
}
