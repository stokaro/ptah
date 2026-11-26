package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
	"ptah.run/internal/devclean/replay"
)

// claimedSurvivors is what the claim of a clean PostgreSQL dev database
// records: its own schemas, public, and plpgsql.
func claimedSurvivors() replay.CleanupSurvivors {
	return replay.CleanupSurvivors{
		Recorded:   true,
		Extensions: []string{"plpgsql"},
		Schemas:    []string{"information_schema", "pg_catalog", "pg_toast", "public"},
	}
}

// survivorGuard is the replay guard of a PostgreSQL dev database in realm,
// claimed with survivors.
func survivorGuard(realm devclean.ReplayRealm, survivors replay.CleanupSurvivors) *devclean.ReplayGuard {
	info := catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", URL: trustedPostgresDev.URL}
	return devclean.NewReplayGuard(info, realm).WithSurvivors(func() replay.CleanupSurvivors { return survivors })
}

// TestReplayGuardSurvivors_HappyPath pins the comments a replay accepts on an
// extension or a schema the claim did not find: the run created it, and every
// cleanup drops it with its comment. Ptah renders the quoted spelling, and
// pg_dump the unquoted one; the name is compared as the server stores it.
func TestReplayGuardSurvivors_HappyPath(t *testing.T) {
	statements := []string{
		`COMMENT ON EXTENSION "pg_trgm" IS 'Fixture extension'`,
		`COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams'`,
		`COMMENT ON EXTENSION PG_TRGM IS NULL`,
		`COMMENT ON SCHEMA "app" IS 'Application schema'`,
		`COMMENT ON SCHEMA App IS 'Application schema'`,
		`COMMENT ON SCHEMA "Public" IS 'not the public schema'`,
	}
	realms := []struct {
		name  string
		realm devclean.ReplayRealm
	}{
		{name: "database", realm: devclean.ReplayRealmDatabase},
		{name: "server", realm: devclean.ReplayRealmServer},
	}
	for _, realm := range realms {
		for _, statement := range statements {
			t.Run(realm.name+"/"+statement, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(survivorGuard(realm.realm, claimedSurvivors()).ValidateStatement(statement), qt.IsNil)
			})
		}
	}
}

// TestReplayGuardSurvivors_HappyPathTrusted pins that trust keeps the
// acceptance: trust lifts nothing for these comments, and the guard reads
// them in the realm as it does without trust.
func TestReplayGuardSurvivors_HappyPathTrusted(t *testing.T) {
	c := qt.New(t)
	trustReplaySQL(c, trustedPostgresDev.URL)
	guard := devclean.NewDevReplayGuard(trustedPostgresDev).WithSurvivors(claimedSurvivors)

	c.Assert(guard.ValidateStatement(`COMMENT ON EXTENSION "pg_trgm" IS 'Fixture extension'`), qt.IsNil)
	c.Assert(guard.ValidateStatement(`COMMENT ON SCHEMA "app" IS 'Application schema'`), qt.IsNil)
}

// TestReplayGuardSurvivors_FailurePath is the control for the tests above. A
// comment on an extension or a schema the claim found outlives the run, and
// so does one on a run whose claim recorded nothing. A schema the server owns
// is refused whatever the claim found, a Unicode escape is a name the guard
// cannot compare, and a comment on another kind of object is not one the rule
// reads.
func TestReplayGuardSurvivors_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		survivors replay.CleanupSurvivors
		statement string
		wantErr   string
	}{
		{name: "an extension the claim found", survivors: claimedSurvivors(),
			statement: `COMMENT ON EXTENSION plpgsql IS 'changed'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a schema the claim found", survivors: claimedSurvivors(),
			statement: `COMMENT ON SCHEMA public IS 'changed'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a schema the claim found, quoted", survivors: claimedSurvivors(),
			statement: `COMMENT ON SCHEMA "public" IS 'changed'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a claim that recorded nothing", survivors: replay.CleanupSurvivors{},
			statement: `COMMENT ON EXTENSION "pg_trgm" IS 'Fixture extension'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a schema the claim recorded nothing for", survivors: replay.CleanupSurvivors{},
			statement: `COMMENT ON SCHEMA "app" IS 'Application schema'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a temporary schema", survivors: claimedSurvivors(),
			statement: `COMMENT ON SCHEMA pg_temp_3 IS 'x'`, wantErr: `.*rejects protected namespace "pg_temp_3" mutation .*`},
		{name: "a catalog schema", survivors: claimedSurvivors(),
			statement: `COMMENT ON SCHEMA pg_catalog IS 'x'`, wantErr: `.*rejects protected namespace "pg_catalog" mutation .*`},
		{name: "a Unicode escape", survivors: claimedSurvivors(),
			statement: `COMMENT ON EXTENSION U&"pg_trgm" IS 'x'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a role", survivors: claimedSurvivors(),
			statement: `COMMENT ON ROLE app_owner IS 'x'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
		{name: "a database", survivors: claimedSurvivors(),
			statement: `COMMENT ON DATABASE ptah_dev IS 'x'`, wantErr: `.*rejects COMMENT ON global metadata .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := survivorGuard(devclean.ReplayRealmDatabase, test.survivors).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
