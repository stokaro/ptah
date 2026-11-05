package devclean_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/devclean"
)

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
