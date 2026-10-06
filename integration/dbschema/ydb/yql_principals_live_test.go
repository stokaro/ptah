//go:build integration

package ydb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

// Users and groups are database-wide. A desired file changes only memberships
// of declared principals, and removing declarations must not drop live users.
func TestYDBDesiredYQL_Principals(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newAccessNames(c)
			c.Cleanup(func() { removeAccess(c, conn, names) })
			file := filepath.Join(c.TempDir(), "schema.sql")
			for _, source := range []string{
				fmt.Sprintf("CREATE USER `%s` PASSWORD 'Secret1!'; CREATE USER %s NOLOGIN; CREATE GROUP %s WITH USER %s, %s;", names.user, names.blocked, names.group, names.user, names.blocked),
				fmt.Sprintf("CREATE USER %s PASSWORD 'Secret1!'; CREATE USER %s NOLOGIN; CREATE GROUP %s WITH USER %s, %s; ALTER USER %s WITH LOGIN; ALTER GROUP %s DROP USER %s;", names.user, names.blocked, names.group, names.user, names.blocked, names.blocked, names.group, names.user),
			} {
				c.Assert(os.WriteFile(file, []byte(source), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{file}, schemafile.Options{Dialect: "ydb"})
				c.Assert(err, qt.IsNil)
				changes := planAgainst(c, conn, desired, accessSchemas)
				c.Assert(changes, qt.Not(qt.HasLen), 0)
				apply(c, conn, changes)
				c.Assert(planAgainst(c, conn, desired, accessSchemas), qt.HasLen, 0)
			}
			c.Assert(os.WriteFile(file, nil, 0o600), qt.IsNil)
			empty, err := schemafile.LoadAll([]string{file}, schemafile.Options{Dialect: "ydb"})
			c.Assert(err, qt.IsNil)
			c.Assert(planAgainst(c, conn, empty, accessSchemas), qt.HasLen, 0)
			roles, memberships, _ := principalsOf(readScoped(c, conn, accessSchemas), names)
			c.Assert(roles, qt.HasLen, 3)
			c.Assert(memberships, qt.HasLen, 1)
			c.Assert(memberships[0].Member, qt.Equals, names.blocked)
		})
	}
}
