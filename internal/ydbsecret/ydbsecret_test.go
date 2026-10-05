package ydbsecret_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbsecret"
)

func TestParseValueEnv_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := ydbsecret.ParseValueEnv(map[string]string{"name": "pw", "value_env": " PTAH_SECRET_PG_PASSWORD "})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "PTAH_SECRET_PG_PASSWORD")
}

func TestParseValueEnv_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  map[string]string
		wantErr string
	}{
		{
			name:    "a literal value is refused without naming it",
			values:  map[string]string{"value": "s3cr3t-SENTINEL", "value_env": "PTAH_SECRET_PW"},
			wantErr: "invalid value: a secret's value is never written in a schema file; name the environment variable that holds it with value_env",
		},
		{
			name:    "no variable",
			values:  map[string]string{"name": "pw"},
			wantErr: "invalid value_env: a secret names the environment variable that holds its value",
		},
		{
			name:   "a variable outside the prefix",
			values: map[string]string{"value_env": "AWS_SECRET_ACCESS_KEY"},
			wantErr: `invalid value_env: "AWS_SECRET_ACCESS_KEY" does not start with PTAH_SECRET_ and a name after it; ` +
				`Ptah reads a secret's value only from a variable under that prefix, so a migration file cannot copy ` +
				`any other variable of the machine that applies it`,
		},
		{
			name:    "the prefix alone",
			values:  map[string]string{"value_env": "PTAH_SECRET_"},
			wantErr: `invalid value_env: "PTAH_SECRET_" does not start with PTAH_SECRET_ and a name after it; .*`,
		},
		{
			name:    "a character YQL does not take in a name",
			values:  map[string]string{"value_env": "PTAH_SECRET_PG-PASSWORD"},
			wantErr: `invalid value_env: "PTAH_SECRET_PG-PASSWORD" holds a character other than an ASCII letter, a digit or an underscore`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbsecret.ParseValueEnv(tc.values)
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(err, qt.ErrorAs, new(*ydbsecret.DeclarationError))
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestCheckName_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbsecret.CheckName("pg.password-1"), qt.IsNil)
}

func TestCheckName_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "empty", input: " ", wantErr: "invalid name: a secret needs a name"},
		{name: "a path", input: "app/pw", wantErr: `invalid name: "app/pw" holds a slash; name the directory with schema`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsecret.CheckName(tc.input), qt.ErrorMatches, tc.wantErr)
		})
	}
}

func TestDefaultValueEnv_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		secret string
		want   string
	}{
		{name: "at the root", secret: "pw", want: "PTAH_SECRET_PW"},
		{name: "in a directory, with characters a variable cannot hold", schema: "/app/ext/", secret: "pg.pass-1",
			want: "PTAH_SECRET_APP_EXT_PG_PASS_1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbsecret.DefaultValueEnv(tc.schema, tc.secret)
			c.Assert(got, qt.Equals, tc.want)
			c.Assert(ydbsecret.CheckValueEnv(got), qt.IsNil)
		})
	}
}

func TestStatements_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "create at the root", got: ydbsecret.CreateStatement("pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "create in a directory", got: ydbsecret.CreateStatement("app.pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "a dotted name at the root stays one segment",
			got:  ydbsecret.CreateStatement(`"pg.pw"`, "PTAH_SECRET_PW"),
			want: "CREATE SECRET `pg.pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "rotate", got: ydbsecret.AlterStatement("app.pw", "PTAH_SECRET_PW"),
			want: "ALTER SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "drop", got: ydbsecret.DropStatement("app.pw"), want: "DROP SECRET `app/pw`;"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tc.got, qt.Equals, tc.want)
		})
	}
}

func TestCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		valueEnv string
	}{
		{name: "a creation", valueEnv: "PTAH_SECRET_PW"},
		{name: "a drop names no variable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsecret.Check("app.pw", tc.valueEnv, capability.YDB262()), qt.IsNil)
		})
	}
}

func TestCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		secret   string
		valueEnv string
		caps     capability.Capabilities
		want     *ydbsecret.Refusal
	}{
		{name: "a line without schema secrets", secret: "app.pw", valueEnv: "PTAH_SECRET_PW",
			caps: capability.YDB251(),
			want: &ydbsecret.Refusal{Subject: "secret app.pw", Key: capability.Secrets}},
		{name: "a line whose flag is off by default", secret: "app.pw", caps: capability.YDB253(),
			want: &ydbsecret.Refusal{Subject: "secret app.pw", Key: capability.Secrets}},
		{name: "another engine", secret: "pw", valueEnv: "PTAH_SECRET_PW", caps: capability.Postgres17(),
			want: &ydbsecret.Refusal{Subject: "secret pw", Key: capability.Secrets}},
		{name: "no name", secret: " ", valueEnv: "PTAH_SECRET_PW", caps: capability.YDB262(),
			want: &ydbsecret.Refusal{Subject: "a secret", Reason: "a secret needs a name"}},
		{name: "a variable outside the prefix", secret: "pw", valueEnv: "HOME", caps: capability.YDB262(),
			want: &ydbsecret.Refusal{Subject: "secret pw", Reason: `invalid value_env: "HOME" does not start with ` +
				`PTAH_SECRET_ and a name after it; Ptah reads a secret's value only from a variable under that ` +
				`prefix, so a migration file cannot copy any other variable of the machine that applies it`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsecret.Check(tc.secret, tc.valueEnv, tc.caps), qt.DeepEquals, tc.want)
		})
	}
}
