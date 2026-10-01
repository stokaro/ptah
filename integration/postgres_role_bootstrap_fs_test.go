//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

func TestLoadSourcesRoleBootstrapOrderAndImports(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	first := writeBootstrapSource(c, dir, "first.sql", "CREATE ROLE reader NOLOGIN;")
	entry := writeBootstrapSource(c, dir, "main.sql", "-- atlas:import ./nested/roles.sql\n")
	writeBootstrapSource(c, dir, "nested/roles.sql", `DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN
 CREATE ROLE reader LOGIN;
 END IF;
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'reader') THEN
 CREATE ROLE writer NOLOGIN;
 END IF;
 END $$;`)
	tests := []struct {
		name    string
		sources []schemafile.Source
		roles   []schemamodel.Role
	}{
		{name: "earlier declaration survives", sources: []schemafile.Source{{URL: first}, {URL: entry}}, roles: []schemamodel.Role{{Name: "reader", Inherit: true}, {Name: "writer", Inherit: true}}},
		{name: "independent load has no leaked state", sources: []schemafile.Source{{URL: entry}}, roles: []schemamodel.Role{{Name: "reader", Login: true, Inherit: true}, {Name: "writer", Inherit: true}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := schemafile.LoadSources(test.sources, schemafile.Options{Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			c.Assert(db.Roles, qt.DeepEquals, test.roles)
		})
	}
}

func TestLoadSourcesRoleBootstrapFailureNamesSource(t *testing.T) {
	c := qt.New(t)
	path := writeBootstrapSource(c, c.TempDir(), "roles.sql", `DO $$ BEGIN CREATE ROLE reader; EXECUTE 'CREATE ROLE hidden'; END $$;`)
	db, err := schemafile.LoadSources([]schemafile.Source{{URL: path}}, schemafile.Options{Dialect: "postgres"})
	c.Assert(err, qt.ErrorMatches, `(?s).*desired-state DO block.*only CREATE ROLE.*`)
	c.Assert(err.Error(), qt.Contains, "roles.sql")
	c.Assert(err.Error(), qt.Contains, "body byte")
	c.Assert(db, qt.IsNil)
}

func writeBootstrapSource(c *qt.C, dir, name, body string) string {
	c.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}
