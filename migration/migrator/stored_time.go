package migrator

import (
	"fmt"
	"strings"
	"time"

	"ptah.run/internal/atlasurl"
)

// atlasRevisionTimeLayout is how an Atlas revision row spells executed_at where
// the column is text: the layout the pinned community binary writes.
const atlasRevisionTimeLayout = "2006-01-02 15:04:05.999999999-07:00"

// storedTimeLayouts are the spellings without an offset that the text of a
// time column arrives in, read in the zone the URL names.
var storedTimeLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
	time.RFC3339Nano,
}

// zeroDateTime is the zero date of MySQL and MariaDB at its longest. A column of
// lower precision hands back a prefix of it, and a DATE column the shortest one,
// zeroDate. It names no moment, so go-sql-driver reads it as the zero time.
const (
	zeroDateTime = "0000-00-00 00:00:00.000000"
	zeroDate     = "0000-00-00"
)

// parseStoredTime reads what a driver scanned from the time column named column
// in one of Ptah's own tables.
//
// Every reader of those tables scans the column into an any and comes through
// here, because no URL has to say parseTime=true: most drivers hand back a
// time.Time, and go-sql-driver hands back the server's text unless the URL asks
// otherwise, which a *time.Time refuses. A row reads the same either way, in the
// same zone. See [atlasurl.TextTimeLocation].
func (m *Migrator) parseStoredTime(column string, value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return m.parseStoredTimeText(column, string(v))
	case string:
		return m.parseStoredTimeText(column, v)
	case nil:
		return time.Time{}, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported %s value %T", column, value)
	}
}

func (m *Migrator) parseStoredTimeText(column, value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" || (len(value) >= len(zeroDate) && strings.HasPrefix(zeroDateTime, value)) {
		return time.Time{}, nil
	}
	// A value that carries its own offset is read in UTC, so the offset stays
	// a bare offset rather than borrowing the reader's zone name: the pinned
	// community binary prints `+0900 +0900`, not `+0900 JST`, for a row written
	// at +09:00 and read under TZ=Asia/Tokyo.
	if parsed, err := time.ParseInLocation(atlasRevisionTimeLayout, value, time.UTC); err == nil {
		return parsed, nil
	}
	location := atlasurl.TextTimeLocation(m.conn.Info().URL)
	for _, layout := range storedTimeLayouts {
		if parsed, err := time.ParseInLocation(layout, value, location); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("failed to parse %s %q", column, value)
}
