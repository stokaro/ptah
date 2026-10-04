//go:build integration

package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devlock"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/ydbgap"
)

// Once a YDB connection opens, every layer behind it that does not reach YDB
// yet refuses in the words of the gap that plans it, rather than sending the
// server another dialect's SQL. Each row is reached through a connection to a
// live server, which is the only way to reach it at all.
func TestYDBConnectedLayersRefuse(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			ctx := c.Context()

			devErr := migrateclean.DevRefusal(ctx, conn)
			_, realmErr := devlock.SameRealm(ctx, conn, conn)

			c.Assert(devErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
			c.Assert(realmErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
		})
	}
}
