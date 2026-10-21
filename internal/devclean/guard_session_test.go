package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

func TestReplayGuardSessionScope_HappyPath(t *testing.T) {
	for _, statement := range []string{
		"CREATE TEMP TABLE first_ids AS SELECT id FROM events",
		"CREATE TEMPORARY TABLE second_ids AS SELECT id FROM first_ids",
		"CREATE LOCAL TEMP TABLE third_ids (id integer) ON COMMIT DROP",
		"CREATE TABLE pg_temp.fourth_ids AS SELECT id FROM second_ids",
		"INSERT INTO pg_temp.second_ids SELECT id FROM first_ids",
		"UPDATE events SET id = 2 FROM pg_temp.first_ids WHERE events.id = first_ids.id",
		"DELETE FROM events WHERE id IN (SELECT id FROM pg_temp.second_ids)",
		"CREATE INDEX scratch_idx ON pg_temp.first_ids (id)",
		"ALTER TABLE pg_temp.first_ids ADD COLUMN note text",
		"DROP TABLE pg_temp.first_ids, pg_temp.second_ids",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.Postgres}, devclean.ReplayRealmDatabase).WithSessionScope()
			c.Assert(guard.ValidateStatement(statement), qt.IsNil)
		})
	}
}

func TestReplayGuardSessionScope_FailurePath(t *testing.T) {
	for _, test := range []struct {
		statement string
		operation string
	}{
		{`CREATE TABLE pg_temp_42.escape (id integer)`, `protected namespace "pg_temp_42" mutation`},
		{`CREATE TABLE pg_catalog.escape (id integer)`, `protected namespace "pg_catalog" mutation`},
		{`CREATE SCHEMA pg_temp`, `protected namespace "pg_temp" mutation`},
		{`DROP SCHEMA pg_temp CASCADE`, `protected namespace "pg_temp" mutation`},
		{`CREATE TEMP VIEW scratch AS SELECT 1`, `TEMP object`},
		{`CREATE ROLE escape`, `CREATE ROLE`},
		{`ALTER SYSTEM SET work_mem = '1GB'`, `ALTER SYSTEM`},
		{`SET ROLE escape`, `SET ROLE`},
		{`COPY scratch FROM PROGRAM 'touch /tmp/escape'`, `external COPY`},
		{`CREATE TEMP TABLE scratch AS SELECT dblink_exec('dbname=other', 'CREATE TABLE escape (id integer)')`, `external dblink operation`},
	} {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.Postgres}, devclean.ReplayRealmDatabase).WithSessionScope()
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, `postgres migration replay rejects `+test.operation+` .*`)
		})
	}
}

func TestReplayGuardSessionScope_OriginalGuardStillRefusesTemporaryTables(t *testing.T) {
	c := qt.New(t)
	guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: platform.Postgres}, devclean.ReplayRealmDatabase)
	_ = guard.WithSessionScope()
	c.Assert(guard.ValidateStatement("CREATE TEMP TABLE scratch (id integer)"), qt.ErrorMatches, `.*rejects TEMP object .*`)
}

func TestReplayGuardSessionScope_OtherEnginesStillRefuseTemporaryTables(t *testing.T) {
	for _, dialect := range []string{platform.CockroachDB, platform.YugabyteDB, platform.Spanner, platform.MySQL, platform.SQLite} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewReplayGuard(catalog.ServerInfo{Dialect: dialect}, devclean.ReplayRealmDatabase).WithSessionScope()
			c.Assert(guard.ValidateStatement("CREATE TEMP TABLE scratch (id integer)"), qt.IsNotNil)
		})
	}
}
