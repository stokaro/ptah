package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// TestRead_ViewCheckOption_HappyPath pins that a schema file's WITH CHECK
// OPTION declares the view's check option and stays out of its body, which is
// what a view declared in Go, YAML or HCL holds: the body the server reports,
// and the check option beside it.
func TestRead_ViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    schemamodel.View
	}{
		{
			name:    "PostgreSQL",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id integer PRIMARY KEY, n integer);\nCREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 WITH CHECK OPTION;",
			want:    schemamodel.View{Name: "v", Body: "SELECT id, n FROM t WHERE n > 0", WithCheck: true},
		},
		{
			name:    "MySQL, with the default level written out",
			dialect: platform.MySQL,
			sql:     "CREATE TABLE t (id int PRIMARY KEY, n int);\nCREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 WITH CASCADED CHECK OPTION;",
			want:    schemamodel.View{Name: "v", Body: "SELECT id, n FROM t WHERE n > 0", WithCheck: true},
		},
		{
			name:    "MariaDB, replacing a view without one",
			dialect: platform.MariaDB,
			sql: "CREATE TABLE t (id int PRIMARY KEY, n int);\nCREATE VIEW v AS SELECT id FROM t;\n" +
				"CREATE OR REPLACE VIEW v AS SELECT id, n FROM t WHERE n > 0 with check option;",
			want: schemamodel.View{Name: "v", Body: "SELECT id, n FROM t WHERE n > 0", WithCheck: true},
		},
		{
			name:    "no check option",
			dialect: platform.Postgres,
			sql:     "CREATE TABLE t (id integer PRIMARY KEY, n integer);\nCREATE VIEW v AS SELECT id, 'WITH CHECK OPTION' AS note FROM t;",
			want:    schemamodel.View{Name: "v", Body: "SELECT id, 'WITH CHECK OPTION' AS note FROM t"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Views, qt.HasLen, 1)
			got := database.Views[0]
			c.Assert(schemamodel.View{Name: got.Name, Body: got.Body, WithCheck: got.WithCheck}, qt.DeepEquals, test.want)
		})
	}
}

// TestRead_ViewCheckOption_FailurePath pins that WITH LOCAL CHECK OPTION is
// refused by name. A declaration holds the check option without a level and
// is created WITH CHECK OPTION, which these engines store as CASCADED: read as
// a check option, the LOCAL view would be created with the stricter check.
func TestRead_ViewCheckOption_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "PostgreSQL", dialect: platform.Postgres,
			sql: "CREATE TABLE t (id integer PRIMARY KEY, n integer);\nCREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 WITH LOCAL CHECK OPTION;"},
		{name: "MySQL, in lower case with a comment", dialect: platform.MySQL,
			sql: "CREATE TABLE t (id int PRIMARY KEY, n int);\nCREATE OR REPLACE VIEW v AS SELECT id, n FROM t WHERE n > 0 with /* level */ local check option;"},
		{name: "MariaDB", dialect: platform.MariaDB,
			sql: "CREATE TABLE t (id int PRIMARY KEY, n int);\nCREATE VIEW v AS SELECT id, n FROM t WHERE n > 0 WITH LOCAL CHECK OPTION;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := readWithOwners([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*view v: WITH LOCAL CHECK OPTION: a declared view holds its check option without a level `+
				`and is created WITH CHECK OPTION, which checks the views it reads too \(CASCADED\); declare WITH CHECK OPTION, `+
				`or create the view in a migration`)
			c.Assert(database.Views, qt.HasLen, 0)
		})
	}
}
