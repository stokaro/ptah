//go:build integration

package migrator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

// clickHouseDefaultQuerySize is ClickHouse's default max_query_size, the
// longest statement text in bytes the server parses.
const clickHouseDefaultQuerySize = 262144

// repeatableQuerySizeBody is a widget_ids view behind a comment of padding
// bytes, a file as long as the padding makes it.
func repeatableQuerySizeBody(padding string) string {
	return "-- " + padding + "\n" + repeatableClickHouse.widgetIDs()
}

func TestRepeatableQuerySizeOnClickHouseLive_HappyPath(t *testing.T) {
	t.Run("a file whose record fits under max_query_size applies and keeps its body", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableEngineTarget(c, t, repeatableClickHouse)
		body := repeatableQuerySizeBody(strings.Repeat("x", 250*1024))

		c.Assert(r.up(c, repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": body}),
			migrator.MigrateUpOptions{}), qt.IsNil)

		c.Assert(r.body(c, "widget_ids"), qt.DeepEquals, keptBody{Body: body, Kept: true})
		c.Assert(r.columns(c, "widget_ids"), qt.Equals, 1)
	})
}

func TestRepeatableQuerySizeOnClickHouseLive_FailurePath(t *testing.T) {
	// A file just above the limit, and one under it whose backslashes the
	// escaping doubles past it.
	files := []struct {
		name    string
		padding string
	}{
		{name: "a file just above 256 KiB", padding: strings.Repeat("x", clickHouseDefaultQuerySize)},
		{name: "a file under 256 KiB that escaping widens past it", padding: strings.Repeat(`\`, 140000)},
	}
	for _, file := range files {
		t.Run(file.name+" is refused before anything runs", func(t *testing.T) {
			c := qt.New(t)
			r := newRepeatableEngineTarget(c, t, repeatableClickHouse)
			fsys := repeatableEngineMigrations(map[string]string{"R__widget_ids.sql": repeatableQuerySizeBody(file.padding)})

			err := r.up(c, fsys, migrator.MigrateUpOptions{})

			var tooLong *migrator.RepeatableStatementTooLongError
			c.Assert(err, qt.ErrorAs, &tooLong)
			c.Assert(tooLong.Limit, qt.Equals, int64(clickHouseDefaultQuerySize))
			c.Assert(tooLong.Setting, qt.Equals, "max_query_size")
			c.Assert(tooLong.Length > clickHouseDefaultQuerySize, qt.IsTrue)
			c.Assert(err, qt.ErrorMatches, `repeatable migration R__widget_ids.sql cannot be recorded: the statement `+
				`that records its body is \d+ bytes, and the server's max_query_size is 262144; raise max_query_size `+
				`for the migration user, or split the file`)
			c.Assert(r.columns(c, "widgets"), qt.Equals, 0)
			c.Assert(r.columns(c, "widget_ids"), qt.Equals, 0)
		})
	}
}
