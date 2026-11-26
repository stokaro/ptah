package devclean_test

import (
	"context"
	"database/sql"
	"slices"
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/internal/devclean"
)

// replaySession is a dev session that reads statements as the guard does. On
// a MySQL or MariaDB server it runs as 'replay'@'%', an account no statement
// below names, holding no role; on SQL Server it is in the database ptah_dev
// and reaches every database a statement names. It answers each query a
// session guard asks, which it tells apart by a word only that query holds.
type replaySession struct{}

func (replaySession) QueryRowContext(_ context.Context, query string, _ ...any) dataext.Row {
	marker := slices.IndexFunc(replaySessionMarkers, func(marker string) bool { return strings.Contains(query, marker) })
	return replaySessionAnswers[marker]
}

// replaySessionMarkers tell the session guards' queries apart: the setting
// only the PostgreSQL family reads, the SQL Server guard's read of its
// database, and its question whether a name is a database the session
// reaches. A query holding none of them is the MySQL or MariaDB guard's.
var replaySessionMarkers = []string{"standard_conforming_strings", "DB_NAME()", "HAS_DBACCESS"}

// replaySessionAnswers are the rows of the session guards' queries, by the
// index of their marker: a MySQL or MariaDB guard reads the client character
// set, the account and its roles; a PostgreSQL-family guard reads
// client_encoding, standard_conforming_strings, the catalog's names and the
// creation schema, and a catalog that names nothing leaves the guard to the
// rule it reads without a session; a SQL Server guard reads the database, and
// learns that the session reaches every database, so that every name of
// another database is refused.
var replaySessionAnswers = map[int]dataext.Row{
	-1: replaySessionRow{"utf8mb4", "replay@%", "[]"},
	0:  replaySessionRow{"UTF8", "on", "[]", "public"},
	1:  replaySessionRow{"ptah_dev"},
	2:  reachedDatabaseRow{},
}

type replaySessionRow []string

func (r replaySessionRow) Scan(dest ...any) error {
	for index, value := range r {
		*dest[index].(*string) = value
	}
	return nil
}

func (replaySessionRow) Err() error { return nil }

// reachedDatabaseRow is HAS_DBACCESS answering 1.
type reachedDatabaseRow struct{}

func (reachedDatabaseRow) Scan(dest ...any) error { return dest[0].(sql.Scanner).Scan(int64(1)) }

func (reachedDatabaseRow) Err() error { return nil }

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
