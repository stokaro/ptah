package migratestatus

// White-box testing required: warnFloor is unexported, and no status read makes
// the migrator warn, so the half of the floor that passes a warning through
// cannot be observed by running the command. Without this test, a floor that
// dropped every record would pass TestMigrateStatus_ReadsWithoutWriting.

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestWarnFloor passes a record at Warn and above to the wrapped handler and
// drops one below, on the logger itself and on one derived with attributes or
// a group.
func TestWarnFloor(t *testing.T) {
	rows := []struct {
		name   string
		level  slog.Level
		derive string
		want   bool
	}{
		{name: "debug", level: slog.LevelDebug, want: false},
		{name: "info", level: slog.LevelInfo, want: false},
		{name: "warn", level: slog.LevelWarn, want: true},
		{name: "error", level: slog.LevelError, want: true},
		{name: "info with attributes", level: slog.LevelInfo, derive: "attrs", want: false},
		{name: "warn with attributes", level: slog.LevelWarn, derive: "attrs", want: true},
		{name: "info in a group", level: slog.LevelInfo, derive: "group", want: false},
		{name: "warn in a group", level: slog.LevelWarn, derive: "group", want: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			floor := slog.New(warnFloor{slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})})
			derived := map[string]*slog.Logger{
				"":      floor,
				"attrs": floor.With("k", "v"),
				"group": floor.WithGroup("g"),
			}[row.derive]

			derived.Log(c.Context(), row.level, "the record")

			c.Assert(strings.Contains(out.String(), "the record"), qt.Equals, row.want)
		})
	}
}
