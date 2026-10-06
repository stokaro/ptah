//go:build integration

package ydb_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
	"ptah.run/internal/sqlident"
)

// YQL permissions must converge on tables, directories, and the database. The
// grant option is a separate YDB permission, including when a source revokes it.
func TestYDBDesiredYQL_Privileges(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newAccessNames(c)
			dropTables(c, conn, accessSchemas)
			c.Cleanup(func() { dropTables(c, conn, accessSchemas); removeAccess(c, conn, names) })
			root := readScoped(c, conn, accessSchemas).DatabasePath
			table := sqlident.Quote("ydb", accessSchema+"/orders")
			directory := sqlident.Quote("ydb", root+"/"+accessSchema)
			prefix := fmt.Sprintf("CREATE TABLE %s (id Int64 NOT NULL, PRIMARY KEY(id)); CREATE GROUP %s; CREATE USER %s;", table, names.group, names.user)
			first := prefix + fmt.Sprintf("GRANT SELECT ROW ON %s TO %s WITH GRANT OPTION; GRANT LIST ON %s TO %s; GRANT CONNECT ON %s TO %s;", table, names.group, directory, names.group, sqlident.Quote("ydb", root), names.user)
			second := first + fmt.Sprintf("REVOKE GRANT OPTION FOR SELECT ROW ON %s FROM %s; GRANT UPDATE ROW ON %s TO %s;", table, names.group, table, names.group)
			file := filepath.Join(c.TempDir(), "schema.sql")
			for _, source := range []string{first, second, prefix} {
				c.Assert(os.WriteFile(file, []byte(source), 0o600), qt.IsNil)
				desired, err := schemafile.LoadAll([]string{file}, schemafile.Options{Dialect: "ydb", DatabaseURL: conn.Info().URL})
				c.Assert(err, qt.IsNil)
				changes := planAgainst(c, conn, desired, accessSchemas)
				c.Assert(changes, qt.Not(qt.HasLen), 0)
				apply(c, conn, changes)
				c.Assert(planAgainst(c, conn, desired, accessSchemas), qt.HasLen, 0)
			}
			roles, _, grants := principalsOf(readScoped(c, conn, accessSchemas), names)
			c.Assert(roles, qt.HasLen, 2)
			c.Assert(grants, qt.HasLen, 0)
		})
	}
}
