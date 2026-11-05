package rolepassword_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/rolepassword"
)

func TestVariable_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		role string
		want string
	}{
		{name: "a YDB user", role: "app2", want: "PTAH_SECRET_PASSWORD_APP2"},
		{name: "a PostgreSQL role with characters a variable cannot hold", role: "app-user.ro", want: "PTAH_SECRET_PASSWORD_APP_USER_RO"},
		{name: "a role in mixed case", role: "AppUser", want: "PTAH_SECRET_PASSWORD_APPUSER"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(rolepassword.Variable(tc.role), qt.Equals, tc.want)
			c.Assert(rolepassword.Reference(tc.role), qt.Equals, "$"+tc.want)
		})
	}
}

func TestValues_HappyPath(t *testing.T) {
	c := qt.New(t)
	roles := []schemamodel.Role{
		{Name: "app", Password: "pw"},
		{Name: "ops", Password: `{"hash":"h","salt":"s","type":"argon2id"}`}, // #nosec G101 -- a made-up value, not a credential
		{Name: "readers"},
	}

	values, err := rolepassword.Values(roles)

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, map[string]string{ // #nosec G101 -- a made-up value, not a credential
		"PTAH_SECRET_PASSWORD_APP": "pw",
		"PTAH_SECRET_PASSWORD_OPS": `{"hash":"h","salt":"s","type":"argon2id"}`,
	})
}

func TestValues_FailurePath(t *testing.T) {
	c := qt.New(t)
	roles := []schemamodel.Role{{Name: "App", Password: "one-SENTINEL"}, {Name: "app", Password: "two-SENTINEL"}}

	values, err := rolepassword.Values(roles)

	c.Assert(err, qt.ErrorMatches, `roles "App" and "app" read their passwords from one environment variable, `+
		`PTAH_SECRET_PASSWORD_APP; rename one of them`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
	c.Assert(values, qt.IsNil)
}

// A run's declared password answers before the environment, and a variable
// the run did not declare comes from the environment, set and empty included.
func TestLookup_HappyPath(t *testing.T) {
	t.Setenv("PTAH_SECRET_PASSWORD_APP", "from-the-environment")
	t.Setenv("PTAH_SECRET_PASSWORD_OPS", "")
	c := qt.New(t)
	ctx := rolepassword.With(c.Context(), map[string]string{"PTAH_SECRET_PASSWORD_APP": "declared"})
	ctx = rolepassword.With(ctx, map[string]string{"PTAH_SECRET_PASSWORD_DEV": "added"})

	lookup := rolepassword.Lookup(ctx)
	app, appFound := lookup("PTAH_SECRET_PASSWORD_APP")
	dev, devFound := lookup("PTAH_SECRET_PASSWORD_DEV")
	ops, opsFound := lookup("PTAH_SECRET_PASSWORD_OPS")
	_, missingFound := lookup("PTAH_SECRET_PASSWORD_MISSING")
	plain, plainFound := rolepassword.Lookup(c.Context())("PTAH_SECRET_PASSWORD_APP")

	c.Assert([]any{app, appFound}, qt.DeepEquals, []any{"declared", true})
	c.Assert([]any{dev, devFound}, qt.DeepEquals, []any{"added", true})
	c.Assert([]any{ops, opsFound}, qt.DeepEquals, []any{"", true})
	c.Assert(missingFound, qt.IsFalse)
	c.Assert([]any{plain, plainFound}, qt.DeepEquals, []any{"from-the-environment", true})
}

// With leaves the context it was given alone, so a run that declares no
// password reads only the environment.
func TestWith_HappyPath(t *testing.T) {
	c := qt.New(t)
	base := rolepassword.With(c.Context(), map[string]string{"PTAH_SECRET_PASSWORD_APP": "first"})
	_ = rolepassword.With(base, map[string]string{"PTAH_SECRET_PASSWORD_APP": "second"})

	value, found := rolepassword.Lookup(base)("PTAH_SECRET_PASSWORD_APP")

	c.Assert([]any{value, found}, qt.DeepEquals, []any{"first", true})
	c.Assert(rolepassword.With(c.Context(), nil), qt.Equals, c.Context())
}

func TestDeclared_HappyPath(t *testing.T) {
	c := qt.New(t)

	values, err := rolepassword.Declared(&schemamodel.Database{Roles: []schemamodel.Role{{Name: "app", Password: "pw"}}})
	none, noneErr := rolepassword.Declared(nil)

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, map[string]string{"PTAH_SECRET_PASSWORD_APP": "pw"})
	c.Assert(noneErr, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

// A password written as a reference names its variable, and the statement
// names that variable; any other password is a value, named by the role's
// variable.
func TestStatement_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
		named    bool
	}{
		{name: "a value", password: "pw", want: "$PTAH_SECRET_PASSWORD_APP"},                                                                         // #nosec G101 -- a made-up value, not a credential
		{name: "a reference", password: "$PTAH_SECRET_APP_PW", want: "$PTAH_SECRET_APP_PW", named: true},                                             // #nosec G101 -- a made-up value, not a credential
		{name: "the prefix in another case is a value", password: "$ptah_secret_app_pw", want: "$PTAH_SECRET_PASSWORD_APP"},                          // #nosec G101 -- a made-up value, not a credential
		{name: "a reference with a character a variable cannot hold is a value", password: "$PTAH_SECRET_APP-PW", want: "$PTAH_SECRET_PASSWORD_APP"}, // #nosec G101 -- a made-up value, not a credential
		{name: "the prefix alone is a value", password: "$PTAH_SECRET_", want: "$PTAH_SECRET_PASSWORD_APP"},                                          // #nosec G101 -- a made-up value, not a credential
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			_, named := rolepassword.Named(tc.password)
			c.Assert(rolepassword.Statement("app", tc.password), qt.Equals, tc.want)
			c.Assert(named, qt.Equals, tc.named)
		})
	}
}

// A password that names a variable has no declared value: the execution reads
// the variable it names.
func TestValuesLeaveANamedPasswordToItsVariable_HappyPath(t *testing.T) {
	c := qt.New(t)

	values, err := rolepassword.Values([]schemamodel.Role{{Name: "app", Password: "$PTAH_SECRET_APP_PW"}, {Name: "ops", Password: "pw"}}) // #nosec G101 -- a made-up value, not a credential

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, map[string]string{"PTAH_SECRET_PASSWORD_OPS": "pw"})
}
