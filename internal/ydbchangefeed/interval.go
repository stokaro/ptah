package ydbchangefeed

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Seconds reads an ISO 8601 duration the way YDB's Interval literal takes one
// for a changefeed's intervals, and returns the whole seconds it denotes.
//
// The accepted forms are weeks alone, days before the T, and hours, minutes
// and seconds after it: `P1W`, `P30D`, `PT12H`, `P1DT2H30M`, `PT90S`. Years and
// months are refused, since they are no fixed number of seconds. So is a
// duration of no length and one with a fraction of a second: YDB refuses an
// interval of zero for every changefeed setting (`retention_period must be
// positive`), and keeps a fraction as nothing at all -- measured on 25.1.4.7
// and 26.2.1.14, RETENTION_PERIOD = Interval('PT1.5S') reads back as 1s, and
// RESOLVED_TIMESTAMPS = Interval('PT0.5S') as no resolved timestamps.
func Seconds(text string) (uint64, error) {
	rest, ok := strings.CutPrefix(strings.ToUpper(strings.TrimSpace(text)), "P")
	if !ok || rest == "" {
		return 0, errors.New("takes an ISO 8601 duration such as PT12H or P1D")
	}
	date, clock, hasClock := strings.Cut(rest, "T")
	if hasClock && clock == "" {
		return 0, errors.New("takes an ISO 8601 duration with a time after T")
	}
	if strings.Contains(date, "W") && (strings.Contains(date, "D") || hasClock) {
		return 0, errors.New("takes weeks alone, as ISO 8601 writes them: P2W, not P2W1D")
	}
	total, err := scan(date, map[byte]uint64{'W': 7 * 24 * 3600, 'D': 24 * 3600})
	if err != nil {
		return 0, err
	}
	timePart, err := scan(clock, map[byte]uint64{'H': 3600, 'M': 60, 'S': 1})
	if err != nil {
		return 0, err
	}
	if total > math.MaxUint64-timePart {
		return 0, errors.New("is longer than YDB holds")
	}
	total += timePart
	if total == 0 {
		return 0, errors.New("is no time at all, and YDB takes only a positive interval")
	}
	return total, nil
}

// scan reads designator-terminated numbers in units, each unit at most once
// and in the order units lists them for ISO 8601.
func scan(part string, units map[byte]uint64) (uint64, error) {
	var total uint64
	seen := make(map[byte]bool, len(units))
	for part != "" {
		end := strings.IndexFunc(part, func(r rune) bool { return r < '0' || r > '9' })
		if end <= 0 {
			if end < 0 {
				return 0, errors.New("takes a designator after every number, as in PT12H")
			}
			if part[0] == '.' || part[0] == ',' {
				return 0, errors.New("YDB keeps whole seconds, and drops the fraction of this one")
			}
			return 0, fmt.Errorf("takes a number before the designator %q", part[:1])
		}
		designator := part[end]
		if designator == '.' || designator == ',' {
			return 0, errors.New("YDB keeps whole seconds, and drops the fraction of this one")
		}
		scale, known := units[designator]
		if !known {
			return 0, fmt.Errorf("takes no %q designator here: years and months are no fixed number of seconds",
				string(designator))
		}
		if seen[designator] {
			return 0, fmt.Errorf("names the designator %q twice", string(designator))
		}
		seen[designator] = true
		count, err := strconv.ParseUint(part[:end], 10, 64)
		if err != nil || (scale != 0 && count > math.MaxUint64/scale) {
			return 0, errors.New("is longer than YDB holds")
		}
		if total > math.MaxUint64-count*scale {
			return 0, errors.New("is longer than YDB holds")
		}
		total += count * scale
		part = part[end+1:]
	}
	return total, nil
}

// FormatSeconds writes a number of seconds as the shortest ISO 8601 duration
// in days, hours, minutes and seconds: 86400 is `P1D`, 43200 `PT12H`, 5400
// `PT1H30M`. It is how the reader writes an interval YDB reports in seconds,
// so a declaration read back from the database is one YDB takes.
func FormatSeconds(seconds uint64) string {
	if seconds == 0 {
		return "PT0S"
	}
	var b strings.Builder
	b.WriteString("P")
	if days := seconds / 86400; days > 0 {
		fmt.Fprintf(&b, "%dD", days)
		seconds %= 86400
	}
	if seconds == 0 {
		return b.String()
	}
	b.WriteString("T")
	for _, unit := range []struct {
		size uint64
		mark string
	}{{3600, "H"}, {60, "M"}, {1, "S"}} {
		if count := seconds / unit.size; count > 0 {
			fmt.Fprintf(&b, "%d%s", count, unit.mark)
			seconds %= unit.size
		}
	}
	return b.String()
}
