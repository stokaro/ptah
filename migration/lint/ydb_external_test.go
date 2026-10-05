package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// TestYD141_ReportsACredentialInADeprecatedSecretObject reports an external
// data source that names a credential by _SECRET_NAME, up and down alike, and
// none that names a YDB secret by its path or takes no credential.
func TestYD141_ReportsACredentialInADeprecatedSecretObject(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a password and a service account key by name",
			files: map[string]string{
				"0001_s.up.sql": "CREATE EXTERNAL DATA SOURCE `ext/pg` WITH (SOURCE_TYPE = 'PostgreSQL', " +
					"LOCATION = 'pg:5432', AUTH_METHOD = 'BASIC', LOGIN = 'u', password_secret_name = 'pw');\n" +
					"CREATE OR REPLACE EXTERNAL DATA SOURCE s3 WITH (SOURCE_TYPE = 'ObjectStorage', " +
					"AUTH_METHOD = 'SERVICE_ACCOUNT', SERVICE_ACCOUNT_ID = 'sa', SERVICE_ACCOUNT_SECRET_NAME = 'k');\n",
				"0001_s.down.sql": "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS old WITH (SOURCE_TYPE = 'ObjectStorage', " +
					"AUTH_METHOD = 'AWS', AWS_ACCESS_KEY_ID_SECRET_NAME = 'a', AWS_SECRET_ACCESS_KEY_SECRET_NAME = 'b');\n",
			},
			want: []string{"0001_s.down.sql:1:YD141", "0001_s.up.sql:1:YD141", "0001_s.up.sql:2:YD141"},
		},
		{
			name: "a secret by its path, no credential, and other statements",
			files: map[string]string{
				"0001_s.up.sql": "CREATE EXTERNAL DATA SOURCE pg WITH (SOURCE_TYPE = 'PostgreSQL', AUTH_METHOD = 'BASIC', " +
					"LOGIN = 'u', PASSWORD_SECRET_PATH = 'ext/pw');\n" +
					"CREATE EXTERNAL DATA SOURCE s3 WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'NONE');\n" +
					"CREATE TABLE t (id Int64 NOT NULL, password_secret_name Utf8, PRIMARY KEY (id));\n",
				"0001_s.down.sql": "DROP EXTERNAL DATA SOURCE pg;\n",
			},
			want: make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// TestYD141_NamesTheSourceAndThePathOptions says which options to use instead.
func TestYD141_NamesTheSourceAndThePathOptions(t *testing.T) {
	c := qt.New(t)
	target, err := lint.ResolveTarget("ydb", "")
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_s.up.sql": "CREATE EXTERNAL DATA SOURCE old WITH (SOURCE_TYPE = 'ObjectStorage', AUTH_METHOD = 'AWS', " +
			"AWS_ACCESS_KEY_ID_SECRET_NAME = 'a', AWS_SECRET_ACCESS_KEY_SECRET_NAME = 'b', AWS_REGION = 'r');\n",
	}), lint.Options{Dialect: "ydb", Target: target})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Equals, "external data source old names its credential by "+
		"AWS_ACCESS_KEY_ID_SECRET_NAME and AWS_SECRET_ACCESS_KEY_SECRET_NAME, a deprecated secret object whose value "+
		"the database administrator reads in clear from .metadata/secrets; on YDB 25.4 and later, keep the value in "+
		"a YDB secret and name it by AWS_ACCESS_KEY_ID_SECRET_PATH and AWS_SECRET_ACCESS_KEY_SECRET_PATH")
}
