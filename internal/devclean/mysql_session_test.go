package devclean_test

import (
	"context"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean"
)

// replaySession is a dev session that reads statements as the guard does. On
// a MySQL or MariaDB server it runs as 'replay'@'%', an account no statement
// below names, holding no role. It answers the one query each session guard
// asks, which it tells apart by the number of values the guard reads.
type replaySession struct{}

func (replaySession) QueryRowContext(context.Context, string, ...any) dataext.Row {
	return replaySessionRow{}
}

type replaySessionRow struct{}

// replaySessionAnswers are the values of the session guards' queries, by the
// number of values each reads: a MySQL or MariaDB guard reads the client
// character set, the account and its roles, and a PostgreSQL-family guard
// reads client_encoding and standard_conforming_strings.
var replaySessionAnswers = map[int][]string{
	3: {"utf8mb4", "replay@%", "[]"},
	2: {"UTF8", "on"},
}

func (replaySessionRow) Scan(dest ...any) error {
	for index, value := range replaySessionAnswers[len(dest)] {
		*dest[index].(*string) = value
	}
	return nil
}

func (replaySessionRow) Err() error { return nil }

// forSession is guard bound to replaySession, as the guard of a run is bound
// to the session it replays on; see session_binding_internal_test.go. A
// guard that read no session refuses every MySQL or MariaDB statement that
// spells out an account.
func forSession(c *qt.C, guard *devclean.ReplayGuard) *devclean.ReplayGuard {
	c.Helper()
	bound, err := guard.BindSessionForTest(c.Context(), replaySession{})
	c.Assert(err, qt.IsNil)
	return bound
}
