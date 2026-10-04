package ydbchangefeed

import (
	"fmt"
	"strings"

	"ptah.run/internal/ydbttl"
)

// Seconds reads one of a changefeed's intervals and returns the whole seconds
// it denotes, in any case.
//
// [ydbttl.IntervalSeconds] holds the grammar of YDB's Interval literal, which
// a changefeed takes as a TTL does: weeks, days, hours, minutes and seconds,
// and neither years nor months. A fraction of a second is refused because YDB
// keeps none of it: measured on 25.1.4.7 and 26.2.1.14, RETENTION_PERIOD =
// Interval('PT1.5S') reads back as 1s, and RESOLVED_TIMESTAMPS =
// Interval('PT0.5S') as no resolved timestamps. A changefeed also refuses an
// interval of no length (`retention_period must be positive`), which a TTL
// takes.
func Seconds(text string) (uint64, error) {
	seconds, err := ydbttl.IntervalSeconds(strings.ToUpper(strings.TrimSpace(text)))
	if err != nil {
		return 0, err
	}
	if seconds == 0 {
		return 0, fmt.Errorf("interval %q is no time at all, and YDB takes only a positive interval", text)
	}
	return seconds, nil
}
