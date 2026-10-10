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

// ownedServerRemedy is the end of a refusal that owning the server lifts.
const ownedServerRemedy = `; if nothing else uses this server, declare it disposable with PTAH_DEV_SERVER_DISPOSABLE=1, ` +
	`or use a docker:// or docker\+<driver>:// dev URL`

// TestBaselineGuard_RefusesOnASharedDevServer pins one refusal per class of
// statement a baseline holds when the target holds the object: on a dev
// server the run does not own, each is refused as a migration replay refuses
// it, in the baseline's own words. A routine or a trigger body can write
// outside the dev database when it runs, a comment on an extension is not
// restored by the cleanup, and a role, a user and a grant outlive it. Each
// refusal that owning the server lifts names the remedy; the comment on an
// extension stays refused on a server the run owns, so its refusal names none.
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
			statement: "COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions'",
			wantErr:   `postgres rehearsal baseline refuses COMMENT ON global metadata because its effects cannot be confined to the dev database realm`,
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

// TestBaselineGuard_AcceptsAGrantInsideTheRealm pins the one statement a
// baseline writes that a replay refuses: in a YDB dev realm, a single GRANT or
// REVOKE whose every path is the realm's root or under it, by absolute path.
// The realm's root stands in for the target's database, so the permissions the
// target holds on its root are granted there, and the realm's removal takes
// them with it.
func TestBaselineGuard_AcceptsAGrantInsideTheRealm(t *testing.T) {
	for _, statement := range []string{
		"GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;",
		"-- Recreates the permission the target holds on its root.\nGRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;",
		"GRANT 'ydb.generic.read', 'ydb.generic.list' ON `/local/ptah_dev/abc/items`, `/local/ptah_dev/abc/app/orders` TO `reader`, writers",
		"REVOKE 'ydb.generic.read' ON `/local/ptah_dev/abc/items` FROM `reader`;",
	} {
		t.Run(statement, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(devclean.NewBaselineGuard(baselineRealm).ValidateStatement(statement), qt.IsNil)
		})
	}
}

// TestBaselineGuard_RefusesAGrantOutsideTheRealm is the control on the test
// above. The statement is read whole: a path that is not absolute, not inside
// the realm, or not in its cleaned form, a second statement after the grant,
// a grant option, a `$` name, a permission that is not a quoted name, a
// translation setting, and a connection that names no realm each leave the
// grant to the replay guard, which refuses every grant in a realm.
func TestBaselineGuard_RefusesAGrantOutsideTheRealm(t *testing.T) {
	noRealm := catalog.ServerInfo{Dialect: platform.YDB, URL: "ydb://localhost:2136/local"}
	tests := []struct {
		name      string
		info      catalog.ServerInfo
		statement string
		wantErr   string
	}{
		{"a relative path", baselineRealm, "GRANT 'ydb.generic.read' ON `items` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"the database root", baselineRealm, "GRANT 'ydb.generic.read' ON `/local` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a realm whose name extends this one", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abcd` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a path that climbs out", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc/../x` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a path with a trailing slash", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc/` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"one path inside and one outside", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc/items`, `/local/items` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a second statement", baselineRealm, "GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `x`; CREATE USER `y`",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a second grant", baselineRealm, "GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `x`; GRANT 'ydb.access.grant' ON `/local` TO `x`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a grant option", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc` TO `reader` WITH GRANT OPTION;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a path named through a $ name", baselineRealm, "GRANT 'ydb.generic.read' ON $path TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a principal named through a $ name", baselineRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc` TO $who;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a permission keyword", baselineRealm, "GRANT SELECT ON `/local/ptah_dev/abc` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
		{"a translation setting", baselineRealm, "--!syntax_v1\nGRANT 'ydb.generic.read' ON `/local/ptah_dev/abc` TO `reader`;",
			`ydb rehearsal baseline refuses translation setting --!syntax_v1 .*`},
		{"a connection naming no realm", noRealm, "GRANT 'ydb.generic.read' ON `/local/ptah_dev/abc` TO `reader`;",
			`ydb rehearsal baseline refuses GRANT permission change .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := devclean.NewBaselineGuard(test.info).ValidateStatement(test.statement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReplayGuard_RefusesAGrantInsideTheRealm pins that the realm grant is the
// baseline's alone: a migration file that grants on the realm's root is
// refused, as every grant in a realm is, in the replay's words.
func TestReplayGuard_RefusesAGrantInsideTheRealm(t *testing.T) {
	c := qt.New(t)

	err := devclean.NewDevReplayGuard(baselineRealm).ValidateStatement("GRANT 'ydb.access.grant' ON `/local/ptah_dev/abc` TO `ACCESS-ADMINS`;")

	c.Assert(err, qt.ErrorMatches, `ydb migration replay rejects GRANT permission change because its effects cannot be confined to the disposable database realm`+ownedServerRemedy)
}
