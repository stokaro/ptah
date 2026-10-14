package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgdefaultprivilege"
)

// TestParse_DefaultPrivilegeRevoked_HappyPath pins the `revoked` key of a
// default privilege: privileges the grantee must not hold by default, with or
// without a granted list beside it.
func TestParse_DefaultPrivilegeRevoked_HappyPath(t *testing.T) {
	c := qt.New(t)
	document := `default_privileges:
  reader_tables:
    for_role: app_owner
    schema: app
    object_type: TABLES
    grantee: app_reader
    revoked: [insert, update]
`

	db, err := yamlschema.Parse(postgresOwners, []byte(document))

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value, qt.DeepEquals, &pgdefaultprivilege.Desired{Revoked: []string{"INSERT", "UPDATE"}})
}

// TestParse_DefaultPrivilegeRevoked_FailurePath pins the entries refused.
func TestParse_DefaultPrivilegeRevoked_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		extra   string
		wantErr string
	}{
		{name: "nothing granted or revoked", wantErr: `(?s).*a default privilege requires privileges or revoked.*`},
		{
			name:    "one privilege granted and revoked",
			extra:   "    privileges: [SELECT]\n    revoked: [SELECT]\n",
			wantErr: `(?s).*default privilege SELECT is both granted and revoked.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "default_privileges:\n  reader_tables:\n    for_role: app_owner\n    schema: app\n" +
				"    object_type: TABLES\n    grantee: app_reader\n" + test.extra

			db, err := yamlschema.Parse(postgresOwners, []byte(document))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
