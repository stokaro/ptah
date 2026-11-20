package mysqlreplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/mysqlreplay"
	"ptah.run/internal/devclean/replay"
)

// guardFor returns the replay guard of the service named dialect.
func guardFor(c *qt.C, dialect string) replay.Guard {
	c.Helper()
	for _, service := range mysqlreplay.Services() {
		if service.Dialect == dialect {
			return service.Guard
		}
	}
	c.Fatalf("no replay service for %s", dialect)
	return nil
}

// Every # starts a comment on MySQL and MariaDB, so the apostrophe in a
// `#=` banner opens no literal, and the statement below it is read by the
// guard as the server reads it. A guard that read `#=` as an operator would let
// the apostrophe hide it.
func TestGuard_HashBannerDoesNotHideAStatement_FailurePath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			err := guardFor(c, dialect).Validate("#= don't\nDROP DATABASE other", replay.Scope{Database: "dev"})

			c.Assert(err, qt.IsNotNil)
		})
	}
}
