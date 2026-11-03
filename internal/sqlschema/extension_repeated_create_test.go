package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// readCreatedExtensions reads sql for target with the built-in owners.
func readCreatedExtensions(c *qt.C, sql, target string) (schemamodel.Database, error) {
	c.Helper()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), target, nil,
		sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	return database, err
}

// TestRead_AnExtensionCreatedTwice_FailurePath holds that a file creating one
// extension twice without IF NOT EXISTS is refused where the server refuses
// the second statement. Measured on PostgreSQL 18.6 and YugabyteDB 2026.1.2.0,
// `CREATE EXTENSION pgcrypto; CREATE EXTENSION pgcrypto;` stops at the second
// statement with `extension "pgcrypto" already exists`. The two declarations
// are equal, so without the refusal they read as one extension and the file
// is accepted.
func TestRead_AnExtensionCreatedTwice_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		target string
		sql    string
	}{
		{name: "postgres", target: platform.Postgres, sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION pgcrypto;"},
		{name: "yugabytedb", target: platform.YugabyteDB, sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION pgcrypto;"},
		{
			name: "between other statements", target: platform.Postgres,
			sql: "CREATE EXTENSION pgcrypto;\nCREATE TABLE t (id int);\nCREATE EXTENSION pgcrypto;",
		},
		{name: "spelled another way", target: platform.Postgres, sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION \"pgcrypto\";"},
		{
			name: "at one version", target: platform.Postgres,
			sql: "CREATE EXTENSION pgcrypto VERSION '1.3';\nCREATE EXTENSION pgcrypto VERSION '1.3';",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readCreatedExtensions(c, test.sql, test.target)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrNameInUse)
			c.Assert(err, qt.ErrorMatches, `a name the file declares is already in use: extension "pgcrypto" is created again `+
				`without IF NOT EXISTS, and the server refuses the second declaration`)
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// TestRead_AnExtensionCreatedTwice_HappyPath is the control: a server that
// runs the second statement reads as one extension. CockroachDB 26.3.2
// accepts a second CREATE EXTENSION of one extension, and every server skips
// one written with IF NOT EXISTS. A read with no dialect applies no
// dialect's rule.
func TestRead_AnExtensionCreatedTwice_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		target string
		sql    string
		want   pgextension.DesiredExtension
	}{
		{name: "cockroachdb", target: platform.CockroachDB, sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION pgcrypto;"},
		{name: "no target", sql: "CREATE EXTENSION pgcrypto;\nCREATE EXTENSION pgcrypto;"},
		{
			name: "postgres, guarded", target: platform.Postgres,
			sql:  "CREATE EXTENSION IF NOT EXISTS pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;",
			want: pgextension.DesiredExtension{IfNotExists: true},
		},
		{
			name: "yugabytedb, guarded", target: platform.YugabyteDB,
			sql:  "CREATE EXTENSION IF NOT EXISTS pgcrypto;\nCREATE EXTENSION IF NOT EXISTS pgcrypto;",
			want: pgextension.DesiredExtension{IfNotExists: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, err := readCreatedExtensions(c, test.sql, test.target)

			c.Assert(err, qt.IsNil)
			c.Assert(pgextension.Names(database.FeatureObjects), qt.DeepEquals, []string{"pgcrypto"})
			object, found, err := database.FeatureObjects.Get(pgextension.Ref("pgcrypto"))
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(object.Value, qt.DeepEquals, &test.want)
		})
	}
}
