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
// outside the dev database when it runs, a comment on an extension is not
// restored by the cleanup, and a role, a user, a grant and a resource pool or
// classifier of the whole database outlive it. A comment on a schema and a
// privilege on one are refused here too, for a dev database whose reset does not
// return them; see [TestBaselineGuard_SchemaStateReturned_HappyPath]. Owning the
// server lifts each, so each refusal names the remedy; for a comment, the server
// has to be one the run provisions.
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

// sharedPostgresDev is a PostgreSQL dev database on a server the run does not
// own.
var sharedPostgresDev = catalog.ServerInfo{Dialect: platform.Postgres, URL: "postgres://dev@shared-dev:5432/ptah_dev"}

// TestBaselineGuard_SchemaStateReturned_HappyPath pins what a reset that returns
// a schema's state lets a baseline write on a server the run does not own: the
// schema's own comment and the privileges on it, in each spelling a plan renders,
// whichever schema they name. A reset returns public and the URL's schema to
// their claimed state and drops the others.
func TestBaselineGuard_SchemaStateReturned_HappyPath(t *testing.T) {
	tests := []struct{ name, statement string }{
		{"a comment on public", `COMMENT ON SCHEMA "public" IS 'our app'`},
		{"a comment on another schema", `COMMENT ON SCHEMA "app" IS 'application'`},
		{"a removed comment", `COMMENT ON SCHEMA "app" IS NULL`},
		{"a privilege for every role", `GRANT USAGE ON SCHEMA "public" TO PUBLIC`},
		{"a privilege for a role", `GRANT CREATE ON SCHEMA "public" TO "app_rw"`},
		{"a privilege with its grant option", `GRANT USAGE ON SCHEMA "app" TO "app_rw" WITH GRANT OPTION`},
		{"every privilege", `GRANT ALL ON SCHEMA "app" TO "app_rw"`},
		{"every privilege, spelled out", `GRANT ALL PRIVILEGES ON SCHEMA "app" TO "app_rw"`},
		{"two schemas", `GRANT USAGE ON SCHEMA "app", "public" TO "app_rw"`},
		{"a revoked privilege", `REVOKE USAGE ON SCHEMA "public" FROM PUBLIC`},
		{"a revoked grant option", `REVOKE GRANT OPTION FOR USAGE ON SCHEMA "app" FROM "app_rw"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewBaselineGuard(sharedPostgresDev).WithSchemaStateReturned(true)
			c.Assert(guard.ValidateStatement(test.statement), qt.IsNil)
		})
	}
}

// TestBaselineGuard_SchemaStateReturned_FailurePath pins the limits of that
// answer. Without a reset that returns the state, each statement the test above
// accepts is refused. With one, what no reset reads stays refused, whatever is
// next to it: a comment on an extension or a database, a security label, a
// privilege on the database or a language, role membership, and anything that
// names a protected namespace.
func TestBaselineGuard_SchemaStateReturned_FailurePath(t *testing.T) {
	const refused = ` because its effects cannot be confined to the dev database realm`
	tests := []struct {
		name      string
		returned  bool
		statement string
		wantErr   string
	}{
		{"a schema comment, not returned", false, `COMMENT ON SCHEMA "public" IS 'our app'`,
			`postgres rehearsal baseline refuses COMMENT ON global metadata` + refused + provisionedServerRemedy},
		{"a schema privilege, not returned", false, `GRANT USAGE ON SCHEMA "public" TO PUBLIC`,
			`postgres rehearsal baseline refuses GRANT database or global privilege` + refused + ownedServerRemedy},
		{"a revoked schema privilege, not returned", false, `REVOKE USAGE ON SCHEMA "public" FROM PUBLIC`,
			`postgres rehearsal baseline refuses REVOKE database or global privilege` + refused + ownedServerRemedy},
		{"a comment on an extension", true, `COMMENT ON EXTENSION pgcrypto IS 'hashing'`,
			`postgres rehearsal baseline refuses COMMENT ON global metadata` + refused + provisionedServerRemedy},
		{"a comment on a database", true, `COMMENT ON DATABASE ptah_dev IS 'dev'`,
			`postgres rehearsal baseline refuses COMMENT ON global metadata` + refused + `.*`},
		{"a security label on a schema", true, `SECURITY LABEL ON SCHEMA "app" IS 'secret'`,
			`postgres rehearsal baseline refuses SECURITY ON global metadata` + refused + `.*`},
		{"a privilege on the database", true, `GRANT CONNECT ON DATABASE ptah_dev TO "app_rw"`,
			`postgres rehearsal baseline refuses GRANT database or global privilege` + refused + ownedServerRemedy},
		{"a privilege on a language", true, `GRANT USAGE ON LANGUAGE plpgsql TO "app_rw"`,
			`postgres rehearsal baseline refuses GRANT database or global privilege` + refused + ownedServerRemedy},
		{"role membership", true, `GRANT "admin" TO "app_rw"`,
			`postgres rehearsal baseline refuses GRANT role membership` + refused + ownedServerRemedy},
		{"a comment on a protected namespace", true, `COMMENT ON SCHEMA pg_catalog IS 'x'`,
			`postgres rehearsal baseline refuses protected namespace "pg_catalog" mutation` + refused},
		{"a privilege on a protected namespace", true, `GRANT USAGE ON SCHEMA pg_catalog TO PUBLIC`,
			`postgres rehearsal baseline refuses protected namespace "pg_catalog" mutation` + refused},
		{"a protected namespace beside another schema", true, `GRANT USAGE ON SCHEMA "app", pg_catalog TO PUBLIC`,
			`postgres rehearsal baseline refuses protected namespace "pg_catalog" mutation` + refused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			guard := devclean.NewBaselineGuard(sharedPostgresDev).WithSchemaStateReturned(test.returned)
			c.Assert(guard.ValidateStatement(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReplayGuard_SchemaStateReturned pins that the answer reaches a rehearsed
// plan only when the rehearsal asks: a migration replay, which builds its guard
// without it, refuses the same statements.
func TestReplayGuard_SchemaStateReturned(t *testing.T) {
	const statement = `COMMENT ON SCHEMA "public" IS 'our app'`
	c := qt.New(t)
	replay := devclean.NewDevReplayGuard(sharedPostgresDev)

	c.Assert(replay.WithSchemaStateReturned(true).ValidateStatement(statement), qt.IsNil)
	c.Assert(replay.ValidateStatement(statement), qt.ErrorMatches,
		`postgres migration replay rejects COMMENT ON global metadata because .*`)
}
