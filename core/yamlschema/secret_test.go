package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbsecret"
)

// TestParse_YDBSecret_HappyPath reads a secret keyed by its name, or named
// apart from its key, with the variable its value comes from.
func TestParse_YDBSecret_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`secrets:
  pg_password:
    value_env: PTAH_SECRET_PG_PASSWORD
  s3:
    name: s3.key
    schema: ext/aws
    value_env: PTAH_SECRET_S3
`))
	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbsecret.DesiredObject("", "pg_password", "", "PTAH_SECRET_PG_PASSWORD"),
		ydbsecret.DesiredObject("ext/aws", "s3.key", "", "PTAH_SECRET_S3"),
	})
	c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "undeclared")).State, qt.Equals, schemaext.Complete)
}

// TestParse_YDBSecret_FailurePath refuses a document that writes the value,
// naming the key and never the value, and one that names no variable or one
// outside the prefix.
func TestParse_YDBSecret_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "a literal value",
			document: "secrets:\n  pw:\n    value: s3cr3t-SENTINEL\n    value_env: PTAH_SECRET_PW\n",
			wantErr: `secret "pw": invalid value: a secret's value is never written in a schema file; ` +
				`name the environment variable that holds it with value_env`,
		},
		{
			name:     "no variable",
			document: "secrets:\n  pw: {}\n",
			wantErr:  `secret "pw": invalid value_env: a secret names the environment variable that holds its value`,
		},
		{
			name:     "a variable outside the prefix",
			document: "secrets:\n  pw:\n    value_env: HOME\n",
			wantErr:  `secret "pw": invalid value_env: "HOME" does not start with PTAH_SECRET_ .*`,
		},
		{
			name:     "a path in the name",
			document: "secrets:\n  pw:\n    name: ext/pw\n    value_env: PTAH_SECRET_PW\n",
			wantErr:  `secret "pw": invalid name: "ext/pw" holds a slash; name the directory with schema`,
		},
		{
			name:     "a directory written from the server root",
			document: "secrets:\n  pw:\n    schema: /local/ext\n    value_env: PTAH_SECRET_PW\n",
			wantErr:  `secret "pw": invalid schema: "/local/ext" starts with a slash; name the directory relative to the database root, .*`,
		},
		{
			name:     "one path declared twice",
			document: "secrets:\n  a:\n    name: pw\n    value_env: PTAH_SECRET_PW\n  b:\n    name: pw\n    value_env: PTAH_SECRET_PW\n",
			wantErr:  `secret "b": secret pw is declared twice`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(test.document))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), ".*SENTINEL.*")
			c.Assert(db, qt.IsNil)
		})
	}
}
