package ydbsecret_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
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

// TestDeclare_HappyPath adds each declaration under its path, with a dot kept
// in the name and the directory read relative to the database root, and leaves
// the input collection as it was.
func TestDeclare_HappyPath(t *testing.T) {
	c := qt.New(t)
	empty := schemaext.Objects{}
	objects, err := ydbsecret.Declare(empty, " /ext/aws/ ", " s3.key ", "Credentials", "PTAH_SECRET_S3")
	c.Assert(err, qt.IsNil)
	objects, err = ydbsecret.Declare(objects, "", "pg.password-1", "", "PTAH_SECRET_PG")
	c.Assert(err, qt.IsNil)

	values, err := objects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.ContentEquals, []schemaext.Object{
		ydbsecret.DesiredObject("ext/aws", "s3.key", "Credentials", "PTAH_SECRET_S3"),
		ydbsecret.DesiredObject("", "pg.password-1", "", "PTAH_SECRET_PG"),
	})
	c.Assert(empty.Len(), qt.Equals, 0)
}

// TestDeclare_FailurePath refuses a declaration a secret cannot take, naming
// the attribute, and a second declaration of one path, naming the path.
func TestDeclare_FailurePath(t *testing.T) {
	declared := must.Must(ydbsecret.Declare(schemaext.Objects{}, "ext", "pw", "", "PTAH_SECRET_PW"))
	tests := []struct {
		name         string
		schema, leaf string
		valueEnv     string
		wantErr      string
		attribute    string
	}{
		{name: "no name", leaf: " ", valueEnv: "PTAH_SECRET_PW", wantErr: "invalid name: a secret needs a name", attribute: ydbsecret.AttributeName},
		{name: "a path as the name", leaf: "app/pw", valueEnv: "PTAH_SECRET_PW",
			wantErr: `invalid name: "app/pw" holds a slash; name the directory with schema`, attribute: ydbsecret.AttributeName},
		{name: "a parent segment as the name", leaf: "..", valueEnv: "PTAH_SECRET_PW", wantErr: `invalid name: ".." is not a path segment`, attribute: ydbsecret.AttributeName},
		{name: "an unclean directory", schema: "ext//aws", leaf: "pw", valueEnv: "PTAH_SECRET_PW",
			wantErr: `invalid schema: "ext//aws" is not a directory path relative to the database root`, attribute: ydbsecret.AttributeSchema},
		{name: "a parent directory", schema: "../ext", leaf: "pw", valueEnv: "PTAH_SECRET_PW",
			wantErr: `invalid schema: "../ext" is not a directory path relative to the database root`, attribute: ydbsecret.AttributeSchema},
		{name: "no variable", leaf: "pw", wantErr: "invalid value_env: a secret names the environment variable that holds its value", attribute: ydbsecret.AttributeValueEnv},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects, err := ydbsecret.Declare(declared, test.schema, test.leaf, "", test.valueEnv)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			declarationError, ok := errors.AsType[*ydbsecret.DeclarationError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(declarationError.Attribute, qt.Equals, test.attribute)
			c.Assert(objects.Refs(), qt.DeepEquals, declared.Refs())
		})
	}
	t.Run("a second declaration", func(t *testing.T) {
		c := qt.New(t)
		objects, err := ydbsecret.Declare(declared, "/ext/", "pw", "Other", "PTAH_SECRET_OTHER")
		c.Assert(err, qt.ErrorMatches, "secret ext/pw is declared twice")
		c.Assert(objects.Refs(), qt.DeepEquals, declared.Refs())
	})
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
		{name: "create at the root", got: ydbsecret.CreateStatement("", "pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "create in a directory", got: ydbsecret.CreateStatement("app", "pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "a dotted name at the root stays one segment",
			got:  ydbsecret.CreateStatement("", "pg.pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `pg.pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "a dotted directory stays one segment",
			got:  ydbsecret.CreateStatement("jobs.daily", "pw", "PTAH_SECRET_PW"),
			want: "CREATE SECRET `jobs.daily/pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "rotate", got: ydbsecret.AlterStatement("app", "pw", "PTAH_SECRET_PW"),
			want: "ALTER SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);"},
		{name: "drop", got: ydbsecret.DropStatement("app", "pw"), want: "DROP SECRET `app/pw`;"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tc.got, qt.Equals, tc.want)
		})
	}
}

func TestRefuse_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbsecret.Refuse("ydb", capability.YDB262(), "secret app/pw"), qt.IsNil)
}

func TestRefuse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		wantErr string
	}{
		{name: "a line without schema secrets", dialect: "ydb", caps: capability.YDB251(),
			wantErr: "secret app/pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "a line whose flag is off by default", dialect: "ydb", caps: capability.YDB253(),
			wantErr: "secret app/pw, which requires target capability secrets, unavailable on this ydb target"},
		{name: "another engine", dialect: "postgresql", caps: capability.Postgres17(),
			wantErr: "secret app/pw, which requires target capability secrets, unavailable on this postgres target"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := ydbsecret.Refuse(tc.dialect, tc.caps, "secret app/pw")
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(capability.Secrets))
		})
	}
}
