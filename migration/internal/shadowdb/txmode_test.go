package shadowdb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/internal/shadowdb"
	"ptah.run/migration/migrator"
)

// A replay of an applied history runs in the mode the run applies its pending
// migrations in, except under all, which replays each migration in a
// transaction of its own.
func TestHistoryTxMode_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		run  migrator.MigrationTxMode
		want migrator.MigrationTxMode
	}{
		{name: "file", run: migrator.MigrationTxModeFile, want: migrator.MigrationTxModeFile},
		{name: "none", run: migrator.MigrationTxModeNone, want: migrator.MigrationTxModeNone},
		{name: "all replays as file", run: migrator.MigrationTxModeAll, want: migrator.MigrationTxModeFile},
		{name: "empty is the migrator default", run: "", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(shadowdb.HistoryTxMode(test.run), qt.Equals, test.want)
		})
	}
}
