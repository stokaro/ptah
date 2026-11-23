package generator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// The checkpoint replays its directory as files that already ran: each file in
// a transaction of its own, unless a run in tx-mode file refuses it there.
// SQLite refuses VACUUM inside a transaction, so migration 1 runs without one,
// with no mode stated. A file's directive wins over the rule.

// checkpointReplayHistory is a directory whose migration 1, which header
// leads, holds a VACUUM beside a CREATE TABLE.
func checkpointReplayHistory(header string) fstest.MapFS {
	return fstest.MapFS{
		"0000000001_items.up.sql":   {Data: []byte(header + "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\nVACUUM;\n")},
		"0000000001_items.down.sql": {Data: []byte("DROP TABLE items;\n")},
		"0000000002_note.up.sql":    {Data: []byte("ALTER TABLE items ADD COLUMN note TEXT;\n")},
		"0000000002_note.down.sql":  {Data: []byte("ALTER TABLE items DROP COLUMN note;\n")},
	}
}

func checkpointReplayOptions(c *qt.C, header string) generator.CheckpointFromShadowOptions {
	c.Helper()
	return generator.CheckpointFromShadowOptions{
		Runtime:           must.Must(builtin.New()),
		ShadowDatabaseURL: "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "shadow.db")),
		MigrationsDir:     ".",
		MigrationsFS:      checkpointReplayHistory(header),
		Dialect:           "sqlite",
	}
}

func TestGenerateCheckpointFromShadow_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "a file without a directive"},
		{name: "a file whose directive names none", header: "-- atlas:txmode none\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			up, _, err := generator.GenerateCheckpointFromShadow(c.Context(), checkpointReplayOptions(c, test.header))

			c.Assert(err, qt.IsNil)
			c.Assert(up, qt.Contains, `"note" TEXT`)
		})
	}
}

func TestGenerateCheckpointFromShadow_FailurePathKeepsTheDirective(t *testing.T) {
	c := qt.New(t)

	up, down, err := generator.GenerateCheckpointFromShadow(c.Context(), checkpointReplayOptions(c, "-- atlas:txmode file\n"))

	c.Assert(err, qt.ErrorMatches,
		`(?s)checkpoint generation failed: .*0000000001_items\.up\.sql cannot run inside a transaction: line 3 VACUUM is refused inside a transaction block.*`)
	c.Assert(up, qt.Equals, "")
	c.Assert(down, qt.Equals, "")
}
