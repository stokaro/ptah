package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// secretSource is an entity file whose holder struct carries the given secret
// annotations, on the struct and on a field.
func secretSource(onStruct, onField string) string {
	return `package entities

` + onStruct + `type Credentials struct {
` + onField + `
	_ int
}
`
}

// TestParseSource_Secret_HappyPath reads a secret's path and the variable its
// value comes from, wherever the annotation sits, into the secret owner's
// model, and claims the secret namespace the source describes.
func TestParseSource_Secret_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		onStruct string
		onField  string
		want     schemaext.Object
	}{
		{
			name:     "on the struct, at the database root",
			onStruct: "//ptah:schema:secret name=\"pg_password\" value_env=\"PTAH_SECRET_PG_PASSWORD\"\n",
			want:     ydbsecret.DesiredObject("", "pg_password", "Credentials", "PTAH_SECRET_PG_PASSWORD"),
		},
		{
			name:    "on a field, in a directory",
			onField: "\t//ptah:schema:secret name=\"s3.key\" schema=\" ext/aws \" value_env=\"PTAH_SECRET_S3\"",
			want:    ydbsecret.DesiredObject("ext/aws", "s3.key", "Credentials", "PTAH_SECRET_S3"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(ydbOwners, "secrets.go", secretSource(test.onStruct, test.onField))
			c.Assert(err, qt.IsNil)
			objects, err := db.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.DeepEquals, []schemaext.Object{test.want})
			c.Assert(db.FeatureCoverage.Lookup(ydbsecret.Kind, ydbsecret.Ref("", "undeclared")).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestParseSource_Secret_FailurePath refuses a declaration that writes the
// value, and the error names the attribute and never the value, so a secret
// typed into a source file by mistake reaches no log.
func TestParseSource_Secret_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		attribute  string
		wantErr    string
	}{
		{
			name:       "a literal value",
			annotation: `//ptah:schema:secret name="pw" value="s3cr3t-SENTINEL" value_env="PTAH_SECRET_PW"`,
			attribute:  "value",
			wantErr: `value on //ptah:schema:secret at Credentials: a secret's value is never written in a ` +
				`schema file; name the environment variable that holds it with value_env`,
		},
		{
			name:       "no variable",
			annotation: `//ptah:schema:secret name="pw"`,
			attribute:  "value_env",
			wantErr:    `missing required annotation attribute "value_env" on //ptah:schema:secret at Credentials`,
		},
		{
			name:       "a variable outside the prefix",
			annotation: `//ptah:schema:secret name="pw" value_env="DATABASE_URL"`,
			attribute:  "value_env",
			wantErr:    `invalid value_env: "DATABASE_URL" does not start with PTAH_SECRET_ and a name after it; .*`,
		},
		{
			name:       "a path in the name",
			annotation: `//ptah:schema:secret name="ext/pw" value_env="PTAH_SECRET_PW"`,
			attribute:  "name",
			wantErr:    `invalid name: "ext/pw" holds a slash; name the directory with schema on //ptah:schema:secret at Credentials`,
		},
		{
			name:       "a directory written from the server root",
			annotation: `//ptah:schema:secret name="pw" schema="/local/ext" value_env="PTAH_SECRET_PW"`,
			attribute:  "schema",
			wantErr:    `invalid schema: "/local/ext" starts with a slash; name the directory relative to the database root, .*`,
		},
		{
			name:       "an attribute a secret does not take",
			annotation: `//ptah:schema:secret name="pw" value_env="PTAH_SECRET_PW" inherit_permissions="true"`,
			attribute:  "inherit_permissions",
			wantErr:    `.*inherit_permissions.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(ydbOwners, "secrets.go", secretSource(test.annotation+"\n", ""))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.Not(qt.ErrorMatches), ".*SENTINEL.*")
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, test.attribute)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// TestParseSource_Secret_DeclaredTwice refuses a second declaration of one
// path, even one naming the same variable: a secret has one declaration.
func TestParseSource_Secret_DeclaredTwice(t *testing.T) {
	c := qt.New(t)
	annotation := `//ptah:schema:secret name="pw" schema="ext" value_env="PTAH_SECRET_PW"`
	db, err := goschema.ParseSource(ydbOwners, "secrets.go", secretSource(annotation+"\n", "\t"+annotation))
	c.Assert(err, qt.ErrorMatches, `secret ext/pw is declared twice on //ptah:schema:secret at Credentials.*`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
}
