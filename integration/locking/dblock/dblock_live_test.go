//go:build integration

package dblock_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dblock"
	"go.5x5.cz/ptah/internal/dbtarget"
)

func TestAdvisoryLock_PostgresFamilyLive(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL, dialect: platform.Postgres},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, dialect: platform.YugabyteDB},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			databaseURL := dbtarget.URL(c, test.engine)
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
