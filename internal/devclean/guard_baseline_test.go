package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

// baselineRealm is a YDB dev connection to the realm abc of the database
// /local, whose absolute path is /local/ptah_dev/abc.
var baselineRealm = catalog.ServerInfo{Dialect: platform.YDB, URL: "ydb://localhost:2136/local?dev_realm=abc"}

// provisionedServerRemedy is the end of a refusal that only a server the run
// provisions lifts.
const provisionedServerRemedy = `; use a docker:// or docker\+<driver>:// dev URL, since a server declared disposable keeps this after the run`

// ownedServerRemedy is the end of a refusal that owning the server lifts.
const ownedServerRemedy = `; if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, ` +
	`or use a docker:// or docker\+<driver>:// dev URL`

// TestBaselineGuard_RefusesOnASharedDevServer pins one refusal per class of
// statement a baseline holds when the target holds the object: on a dev
// server the run does not own, each is refused as a migration replay refuses
// it, in the baseline's own words. A routine or a trigger body can write
// outside the dev database when it runs, a comment on an extension or a schema
// is not restored by the cleanup, and a role, a user, a grant and a resource
// pool or classifier of the whole database outlive it. Owning the server lifts
// each, so each refusal names the remedy; for a comment, the server has to be
// one the run provisions.
func TestBaselineGuard_RefusesOnASharedDevServer(t *testing.T) {
	postgres := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@shared-dev:5432/ptah_dev"}
	mysql := catalog.ServerInfo{Dialect: platform.MySQL, Schema: "ptah_dev", URL: "mysql://dev@shared-dev:3306/ptah_dev"}
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
		wantErr   string
	}{
		{
			name:      "a PostgreSQL routine",
			info:      postgres,
			statement: "CREATE FUNCTION item_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM items $$",
			wantErr:   `postgres rehearsal baseline refuses CREATE routine definition because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a PostgreSQL routine change",
			info:      postgres,
			statement: "ALTER FUNCTION item_count() SECURITY DEFINER",
			wantErr:   `postgres rehearsal baseline refuses ALTER routine definition because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a MySQL trigger",
			info:      mysql,
			statement: "CREATE TRIGGER items_audit AFTER INSERT ON items FOR EACH ROW INSERT INTO app.audit VALUES (NEW.id)",
			wantErr:   `mysql rehearsal baseline refuses CREATE executable stored body because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a comment on an extension",
			info:      postgres,
			statement: "COMMENT ON EXTENSION pgcrypto IS 'hashing'",
			wantErr:   `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm` + provisionedServerRemedy,
		},
		{
			name:      "a comment on a schema",
			info:      postgres,
			statement: `COMMENT ON SCHEMA "app" IS 'application'`,
			wantErr:   `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm` + provisionedServerRemedy,
		},
		{
			name:      "a PostgreSQL role",
			info:      postgres,
			statement: `CREATE ROLE "reader" WITH NOLOGIN`,
			wantErr:   `postgres rehearsal baseline refuses CREATE ROLE because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a YDB user",
			info:      baselineRealm,
			statement: "CREATE USER `reader`;",
			wantErr:   `ydb rehearsal baseline refuses a user of the whole database because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a YDB resource pool",
			info:      baselineRealm,
			statement: "CREATE RESOURCE POOL batch WITH (concurrent_query_limit = 10);",
			wantErr:   `ydb rehearsal baseline refuses a resource pool of the whole database because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a YDB resource pool change",
			info:      baselineRealm,
			statement: "ALTER RESOURCE POOL default WITH (resource_weight = 50);",
			wantErr:   `ydb rehearsal baseline refuses a resource pool of the whole database because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a YDB resource pool classifier",
			info:      baselineRealm,
			statement: "CREATE RESOURCE POOL CLASSIFIER etl WITH (resource_pool = 'batch', rank = 1);",
			wantErr:   `ydb rehearsal baseline refuses a resource pool of the whole database because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			// The realm's root survives a reset in the middle of a run, so a
			// permission on it would reach the next target rehearsed there.
			name:      "a YDB grant on the realm's root",
			info:      baselineRealm,
			statement: "GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;",
			wantErr:   `ydb rehearsal baseline refuses GRANT permission change because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
		{
			name:      "a YDB grant on a relative path",
			info:      baselineRealm,
			statement: "GRANT 'ydb.generic.read' ON `items` TO `reader`;",
			wantErr:   `ydb rehearsal baseline refuses GRANT permission change because its effects cannot be confined to the dev database realm` + ownedServerRemedy,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewBaselineGuard(test.info).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestBaselineGuard_OwnedServer is the control on the refusals above: on a
// server the run owns, the statements whose refusal names the remedy are the
// run's own, and the guard accepts them. What reaches past the server stays
// refused there.
func TestBaselineGuard_OwnedServer(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
	}{
		{"a PostgreSQL routine", catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@owned-baseline-pg:5432/ptah_dev"},
			"CREATE FUNCTION item_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM items $$"},
		{"a MySQL trigger", catalog.ServerInfo{Dialect: platform.MySQL, Schema: "ptah_dev", URL: "mysql://dev@owned-baseline-mysql:3306/ptah_dev"},
			"CREATE TRIGGER items_touch BEFORE INSERT ON items FOR EACH ROW SET NEW.n = 1"},
		{"a YDB user", catalog.ServerInfo{Dialect: platform.YDB, URL: "ydb://owned-baseline-ydb:2136/local"}, "CREATE USER `reader`;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declareDisposable(c, test.info.URL)
			c.Assert(devclean.NewBaselineGuard(test.info).ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestBaselineGuard_DeclaredServerKeepsDevDatabaseComments pins that a comment
// on an extension or a schema of the dev database is refused on a server the
// operator declared disposable. That server outlives the run, and its reset
// keeps the extensions and schemas it found, so the comment would reach the
// next run. The refusal names the one remedy that lifts it.
func TestBaselineGuard_DeclaredServerKeepsDevDatabaseComments(t *testing.T) {
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
	}{
		{"a comment on an extension", catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@owned-baseline-pg-extension:5432/ptah_dev"},
			"COMMENT ON EXTENSION pgcrypto IS 'hashing'"},
		{"a comment on a schema", catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@owned-baseline-pg-schema:5432/ptah_dev"},
			`COMMENT ON SCHEMA "app" IS 'application'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declareDisposable(c, test.info.URL)
			c.Assert(devclean.NewBaselineGuard(test.info).ValidateStatement(test.statement), qt.ErrorMatches,
				`postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm`+
					provisionedServerRemedy)
		})
	}
}

// TestBaselineGuard_OwnedServerKeepsOtherComments pins how far the comment
// lift reaches on a server the run owns: a comment on a language, a cast or an
// event trigger changes how the run's later SQL is read, and a comment on a
// protected namespace mutates it, so each stays refused there.
func TestBaselineGuard_OwnedServerKeepsOtherComments(t *testing.T) {
	tests := []struct {
		statement string
		wantErr   string
	}{
		{statement: "COMMENT ON LANGUAGE plpgsql IS 'x'",
			wantErr: `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm`},
		{statement: "COMMENT ON CAST (int AS text) IS 'x'",
			wantErr: `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm`},
		{statement: "COMMENT ON EVENT TRIGGER audit IS 'x'",
			wantErr: `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm`},
		{statement: "COMMENT ON SCHEMA pg_catalog IS 'x'",
			wantErr: `postgres rehearsal baseline refuses protected namespace "pg_catalog" mutation because its effects cannot be confined to the dev database realm`},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			owned := catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@owned-baseline-pg-comments:5432/ptah_dev"}
			declareDisposable(c, owned.URL)

			err := devclean.NewBaselineGuard(owned).ValidateStatement(test.statement)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestBaselineGuard_OwnedServerKeepsWhatReachesPastIt pins that owning the
// server lifts nothing that reaches past it: a replication pulls from
// whatever its connection string names, wherever the dev server runs.
func TestBaselineGuard_OwnedServerKeepsWhatReachesPastIt(t *testing.T) {
	c := qt.New(t)
	owned := catalog.ServerInfo{Dialect: platform.YDB, URL: "ydb://owned-baseline-replication:2136/local"}
	declareDisposable(c, owned.URL)

	err := devclean.NewBaselineGuard(owned).ValidateStatement(
		"CREATE ASYNC REPLICATION `r` FOR `items` AS `copy` WITH (CONNECTION_STRING = 'grpcs://prod.example:2135/?database=/prod');")

	c.Assert(err, qt.ErrorMatches, `ydb rehearsal baseline refuses async replication because its effects cannot be confined to the dev database realm`)
}

// TestReplayGuard_RefusesAGrantInsideTheRealm pins that the realm grant is the
// baseline's alone: a migration file that grants on the realm's root is
// refused, as every grant in a realm is, in the replay's words.
func TestReplayGuard_RefusesAGrantInsideTheRealm(t *testing.T) {
	c := qt.New(t)

	err := devclean.NewDevReplayGuard(baselineRealm).ValidateStatement("GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;")

	c.Assert(err, qt.ErrorMatches, `ydb migration replay rejects GRANT permission change because its effects cannot be confined to the disposable database realm`+ownedServerRemedy)
}
