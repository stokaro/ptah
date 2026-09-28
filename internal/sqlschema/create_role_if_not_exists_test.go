package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// createRoleIfNotExists is the statement the MySQL-family renderer writes for a
// role, which `schema inspect --format sql` writes for every role the server
// holds.
const createRoleIfNotExists = "CREATE ROLE IF NOT EXISTS `probe_role`;\nCREATE TABLE t (id int PRIMARY KEY);"

// TestRead_CreateRoleIfNotExists_HappyPath reads the guarded role where MySQL
// and MariaDB accept it, and in a document read with no dialect. It declares
// the role, since the role exists after the statement either way. Read as a
// role named IF, the file Ptah wrote could not be read back
// (stokaro/ptah#3902).
func TestRead_CreateRoleIfNotExists_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", ""} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(createRoleIfNotExists), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Roles, qt.HasLen, 1)
			c.Assert(database.Roles[0].Name, qt.Equals, "probe_role")
		})
	}
}

// TestRead_CreateRoleIfNotExists_FailurePath refuses the guard on PostgreSQL,
// which has no CREATE ROLE IF NOT EXISTS and answers a syntax error, naming
// the clause rather than reading IF as the role.
func TestRead_CreateRoleIfNotExists_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, _, err := sqlschema.Read([]byte(`CREATE ROLE IF NOT EXISTS probe_role;`), "postgres")

	c.Assert(err, qt.ErrorMatches, `(?s).*CREATE ROLE IF NOT EXISTS at position \d+: postgres takes no IF NOT EXISTS on CREATE ROLE.*`)
}
