package devclean_test

import (
	"context"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean"
)

// replaySession is a dev session of a MySQL or MariaDB server that reads
// statements as the guard does and runs as 'replay'@'%', an account no
// statement below names, holding no role. It answers the one query the
// session guard asks.
type replaySession struct{}

func (replaySession) QueryRowContext(context.Context, string, ...any) dataext.Row {
	return replaySessionRow{}
}

type replaySessionRow struct{}

func (replaySessionRow) Scan(dest ...any) error {
	for index, value := range []string{"utf8mb4", "replay@%", "[]"} {
		*dest[index].(*string) = value
	}
	return nil
}

func (replaySessionRow) Err() error { return nil }

// forSession is guard bound to replaySession, as the guard of a run is bound
// to the session it replays on. A guard that read no session refuses every
// MySQL or MariaDB statement that spells out an account.
func forSession(c *qt.C, guard *devclean.ReplayGuard) *devclean.ReplayGuard {
	c.Helper()
	bound, err := guard.ForSession(c.Context(), replaySession{})
	c.Assert(err, qt.IsNil)
	return bound
}
