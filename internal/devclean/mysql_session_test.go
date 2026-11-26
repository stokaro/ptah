package devclean_test

import (
	"context"
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean"
)

// replaySession is a dev session that reads statements as the guard does. On
// a MySQL or MariaDB server it runs as 'replay'@'%', an account no statement
// below names, holding no role. It answers the one query each session guard
// asks, which it tells apart by the setting only the PostgreSQL family reads.
type replaySession struct{}

func (replaySession) QueryRowContext(_ context.Context, query string, _ ...any) dataext.Row {
	return replaySessionRow(replaySessionAnswers[strings.Contains(query, "standard_conforming_strings")])
}

type replaySessionRow []string

// replaySessionAnswers are the values of the session guards' queries: a MySQL
// or MariaDB guard reads the client character set, the account and its roles,
// and a PostgreSQL-family guard reads client_encoding,
// standard_conforming_strings, the catalog's names and the creation schema. A
// catalog that names nothing leaves the guard to the rule it reads without a
// session.
var replaySessionAnswers = map[bool][]string{
	false: {"utf8mb4", "replay@%", "[]"},
	true:  {"UTF8", "on", "[]", "public"},
}

func (r replaySessionRow) Scan(dest ...any) error {
	for index, value := range r {
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
