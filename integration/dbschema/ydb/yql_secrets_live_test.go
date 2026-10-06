//go:build integration

package ydb_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

func TestYDBDesiredYQL_Secrets(t *testing.T) {
	t.Setenv("PTAH_SECRET_YQL_TEST", "yql-secret-fixture")
	c := qt.New(t)
	conn := connect(c, enterRealm(c, lineNamed(c, "26.2")))
	path := filepath.Join(c.TempDir(), "schema.sql")
	for _, source := range []string{
		"CREATE SECRET first WITH (value = $PTAH_SECRET_YQL_TEST); CREATE SECRET second WITH (value = $PTAH_SECRET_YQL_TEST);",
		"CREATE SECRET first WITH (value = $PTAH_SECRET_YQL_TEST);",
		"",
	} {
		c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)
		desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
		c.Assert(err, qt.IsNil)
		statements := planAgainst(c, conn, desired, nil)
		c.Assert(statements, qt.Not(qt.HasLen), 0)
		apply(c, conn, statements)
		c.Assert(planAgainst(c, conn, desired, nil), qt.HasLen, 0)
	}
}

func TestYDBDesiredYQL_SecretsRefusedOnOlderLine(t *testing.T) {
	c := qt.New(t)
	conn := connect(c, enterRealm(c, lineNamed(c, "25.1")))
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE SECRET first WITH (value = $PTAH_SECRET_YQL_TEST);"), 0o600), qt.IsNil)
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	_, err = schemadiff.CompareWithDatabaseInfo(desired, readScoped(c, conn, nil), conn.Info(), nil)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "secrets")
	c.Assert(desired.Secrets, qt.DeepEquals, []schemamodel.Secret{{Name: "first", ValueEnv: "PTAH_SECRET_YQL_TEST"}})
}
