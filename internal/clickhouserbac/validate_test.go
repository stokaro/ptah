package clickhouserbac_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/clickhouserbac"
)

// clickHouseRole is a declaration the parser would produce for a bare
// `//ptah:schema:role name="..."` on a ClickHouse target: Inherit true, every
// other attribute absent. Building it here rather than writing a literal per
// row keeps the rows carrying only what varies, and keeps a row from passing
// because it forgot Inherit.
func clickHouseRole(name string) goschema.Role {
	return goschema.Role{Name: name, Inherit: true}
}

func TestValidateDeclared_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		roles           []goschema.Role
		grants          []goschema.Grant
		defaultDatabase string
	}{
		{
			name:  "a bare role",
			roles: []goschema.Role{clickHouseRole("reader")},
		},
		{
			// Inherit false is the Go zero value, so a Role built in code
			// carries it without anyone declaring it. Refusing on that signal
			// would refuse the common case; this row is what says so.
			name:  "a role built in code, with Inherit left at its zero value",
			roles: []goschema.Role{{Name: "reader"}},
		},
		{
			name:  "a role with a comment, which renders as a SQL comment line",
			roles: []goschema.Role{{Name: "reader", Inherit: true, Comment: "read-only access"}},
		},
		{
			name:   "a table grant",
			roles:  []goschema.Role{clickHouseRole("reader")},
			grants: []goschema.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "shop.orders"}},
		},
		{
			name:   "a database grant with the option",
			roles:  []goschema.Role{clickHouseRole("admin")},
			grants: []goschema.Grant{{Role: "admin", Privileges: []string{"SELECT", "INSERT"}, OnSchema: "shop", WithOption: true}},
		},
		{
			name:  "the same privilege on disjoint databases",
			roles: []goschema.Role{clickHouseRole("reader")},
			grants: []goschema.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "shop"},
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "warehouse"},
			},
		},
		{
			name:  "different privileges on nested scopes, which the server keeps apart",
			roles: []goschema.Role{clickHouseRole("reader")},
			grants: []goschema.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "shop"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "shop.orders"},
			},
		},
		{
			name:  "two roles may hold the same privilege on the same scope",
			roles: []goschema.Role{clickHouseRole("reader"), clickHouseRole("auditor")},
			grants: []goschema.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "shop"},
				{Role: "auditor", Privileges: []string{"SELECT"}, OnSchema: "shop"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := clickhouserbac.ValidateDeclared(platform.ClickHouse, test.roles, test.grants, test.defaultDatabase)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateDeclared_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		roles   []goschema.Role
		grants  []goschema.Grant
		wantErr string
	}{
		{
			name:    "a password",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, Password: "hunter2"}},
			wantErr: `(?s).*role "reader" declares a password: ClickHouse roles carry no credentials.*`,
		},
		{
			name:    "login",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, Login: true}},
			wantErr: `(?s).*role "reader" declares login: a ClickHouse role carries no attributes.*`,
		},
		{
			name:    "superuser",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, Superuser: true}},
			wantErr: `(?s).*declares superuser.*`,
		},
		{
			name:    "createdb",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, CreateDB: true}},
			wantErr: `(?s).*declares createdb.*`,
		},
		{
			name:    "createrole",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, CreateRole: true}},
			wantErr: `(?s).*declares createrole.*`,
		},
		{
			name:    "replication",
			roles:   []goschema.Role{{Name: "reader", Inherit: true, Replication: true}},
			wantErr: `(?s).*declares replication.*`,
		},
		{
			name:    "the reserved principal",
			roles:   []goschema.Role{clickHouseRole("default")},
			wantErr: `(?s).*role "default" is a reserved ClickHouse principal.*`,
		},
		{
			name:    "ALL, which the server expands",
			roles:   []goschema.Role{clickHouseRole("reader")},
			grants:  []goschema.Grant{{Role: "reader", Privileges: []string{"ALL"}, OnSchema: "shop"}},
			wantErr: `(?s).*declares privilege "ALL": it expands to every individual privilege.*`,
		},
		{
			name:    "ALL PRIVILEGES",
			roles:   []goschema.Role{clickHouseRole("reader")},
			grants:  []goschema.Grant{{Role: "reader", Privileges: []string{"ALL PRIVILEGES"}, OnSchema: "shop"}},
			wantErr: `(?s).*expands to every individual privilege.*`,
		},
		{
			name:    "a column-scoped privilege",
			roles:   []goschema.Role{clickHouseRole("reader")},
			grants:  []goschema.Grant{{Role: "reader", Privileges: []string{"SELECT(id)"}, OnTable: "shop.orders"}},
			wantErr: `(?s).*column-scoped privilege "SELECT\(id\)".*`,
		},
		{
			name:    "no privilege at all",
			roles:   []goschema.Role{clickHouseRole("reader")},
			grants:  []goschema.Grant{{Role: "reader", OnSchema: "shop"}},
			wantErr: `(?s).*grant to role "reader" names no privilege.*`,
		},
		{
			name:  "the absorption pair the server would collapse",
			roles: []goschema.Role{clickHouseRole("reader")},
			grants: []goschema.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "shop"},
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "shop.orders"},
			},
			wantErr: `(?s).*declares SELECT on both shop\.\* and shop\.orders.*can never converge.*`,
		},
		{
			name:  "an exact duplicate is the same collapse",
			roles: []goschema.Role{clickHouseRole("reader")},
			grants: []goschema.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "shop.orders"},
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "shop.orders"},
			},
			wantErr: `(?s).*declares SELECT on both shop\.orders and shop\.orders.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := clickhouserbac.ValidateDeclared(platform.ClickHouse, test.roles, test.grants, "")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateDeclared_LeavesOtherDialectsAlone is the non-interference
// control, and it is the assertion this package most needs.
//
// Every refusal above describes ClickHouse. Applied to PostgreSQL they would
// refuse the role model PostgreSQL is built on — a password, LOGIN, SUPERUSER
// — so a gate that fired on the wrong dialect would not merely be noisy, it
// would break the dialect this repository has always supported. The rows below
// are declarations PostgreSQL accepts and ClickHouse refuses; every one must
// pass here.
func TestValidateDeclared_LeavesOtherDialectsAlone(t *testing.T) {
	roles := []goschema.Role{{
		Name: "app_user", Login: true, Password: "hunter2",
		Superuser: true, CreateDB: true, CreateRole: true, Replication: true,
	}}
	grants := []goschema.Grant{
		{Role: "app_user", Privileges: []string{"ALL"}, OnSchema: "public"},
		{Role: "app_user", Privileges: []string{"SELECT"}, OnSequence: "orders_id_seq"},
		{Role: "app_user", Privileges: []string{"SELECT"}, OnSchema: "public"},
		{Role: "app_user", Privileges: []string{"SELECT"}, OnTable: "public.orders"},
	}

	dialects := []string{
		platform.Postgres,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.SQLServer,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(clickhouserbac.ValidateDeclared(dialect, roles, grants, ""), qt.IsNil)
		})
	}
}

// TestValidateDeclared_RefusesTheSameDeclarationOnClickHouse is the other half
// of the control above: the identical input, on the dialect this package owns,
// must be refused. Without it, a ValidateDeclared that returned nil for every
// dialect would satisfy the non-interference test completely.
func TestValidateDeclared_RefusesTheSameDeclarationOnClickHouse(t *testing.T) {
	c := qt.New(t)

	roles := []goschema.Role{{
		Name: "app_user", Login: true, Password: "hunter2",
		Superuser: true, CreateDB: true, CreateRole: true, Replication: true,
	}}
	grants := []goschema.Grant{
		{Role: "app_user", Privileges: []string{"ALL"}, OnSchema: "public"},
		{Role: "app_user", Privileges: []string{"SELECT"}, OnSequence: "orders_id_seq"},
	}

	err := clickhouserbac.ValidateDeclared(platform.ClickHouse, roles, grants, "")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "declares a password")
	c.Assert(err.Error(), qt.Contains, "declares login")
	c.Assert(err.Error(), qt.Contains, "ClickHouse has no sequences")
}

// TestValidateDeclared_NeverEchoesACredential pins the secrets rule at the one
// place a declared credential meets an error string. The refusal names the
// role; the value must not travel with it, because this error reaches stderr, a
// plan document, and whatever collects them.
func TestValidateDeclared_NeverEchoesACredential(t *testing.T) {
	c := qt.New(t)

	const secret = "s3cr3t-value-that-must-not-appear"
	roles := []goschema.Role{{Name: "reader", Inherit: true, Password: secret}}

	err := clickhouserbac.ValidateDeclared(platform.ClickHouse, roles, nil, "")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `role "reader"`)
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
}

// TestValidateDeclared_IsDeterministic guards the diagnostic against map
// iteration order. The absorption check keys its work by role and privilege in
// a map, and a message that names a different offender on each run is one no
// test can pin and no reader can trust.
func TestValidateDeclared_IsDeterministic(t *testing.T) {
	c := qt.New(t)

	roles := []goschema.Role{clickHouseRole("a"), clickHouseRole("b"), clickHouseRole("c")}
	grants := []goschema.Grant{
		{Role: "c", Privileges: []string{"SELECT"}, OnSchema: "shop"},
		{Role: "c", Privileges: []string{"SELECT"}, OnTable: "shop.orders"},
		{Role: "a", Privileges: []string{"INSERT"}, OnSchema: "warehouse"},
		{Role: "a", Privileges: []string{"INSERT"}, OnTable: "warehouse.items"},
	}

	first := clickhouserbac.ValidateDeclared(platform.ClickHouse, roles, grants, "")
	c.Assert(first, qt.IsNotNil)

	for range 20 {
		again := clickhouserbac.ValidateDeclared(platform.ClickHouse, roles, grants, "")
		c.Assert(again, qt.IsNotNil)
		c.Assert(again.Error(), qt.Equals, first.Error())
	}
}
