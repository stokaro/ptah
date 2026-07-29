package dblock_test

import (
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/dblock"
)

func TestAdvisoryLock_PostgresFamilyLive(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		urlEnv  string
		dialect string
	}{
		{name: "postgres", urlEnv: "POSTGRES_TEST_DSN", dialect: platform.Postgres},
		{name: "yugabytedb", urlEnv: "YUGABYTEDB_URL", dialect: platform.YugabyteDB},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			databaseURL := requireLiveLockURL(c, test.urlEnv)
			first, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(first)
			})
			second, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() {
				dbschema.CloseAndWarn(second)
			})

			firstLock, err := dblock.Acquire(
				c.Context(),
				first,
				"ptah_dblock_postgres_family_live",
				5*time.Second,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(firstLock.Supported(), qt.IsTrue)

			blockedLock, err := dblock.Acquire(
				c.Context(),
				second,
				"ptah_dblock_postgres_family_live",
				100*time.Millisecond,
			)
			c.Assert(err, qt.ErrorMatches,
				`timed out acquiring advisory lock "ptah_dblock_postgres_family_live" on `+
					test.dialect+` after 100ms`)
			c.Assert(dblock.IsTimeout(err), qt.IsTrue)
			c.Assert(blockedLock, qt.IsNil)

			c.Assert(firstLock.Release(c.Context()), qt.IsNil)

			secondLock, err := dblock.Acquire(
				c.Context(),
				second,
				"ptah_dblock_postgres_family_live",
				5*time.Second,
			)
			c.Assert(err, qt.IsNil)
			c.Assert(secondLock.Supported(), qt.IsTrue)
			c.Assert(secondLock.Release(c.Context()), qt.IsNil)
		})
	}
}

func requireLiveLockURL(c *qt.C, environmentName string) string {
	c.Helper()
	databaseURL := os.Getenv(environmentName)
	if databaseURL == "" {
		c.Skip(environmentName + " is not set")
	}
	return databaseURL
}
