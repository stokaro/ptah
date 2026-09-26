package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// freshYugabyteDB is what a YugabyteDB 2026.1.2 database made by CREATE
// DATABASE reports, before anybody creates anything in it.
func freshYugabyteDB() *catalog.Database {
	return &catalog.Database{Extensions: []catalog.Extension{
		{Name: "pg_stat_statements", Schema: "pg_catalog", Version: "1.10-yb-2.1"},
		{Name: "plpgsql", Schema: "pg_catalog", Version: "1.0"},
		{Name: "postgres_fdw", Schema: "pg_catalog", Version: "1.1"},
	}}
}

// removedExtensionNames lists the extensions a comparison plans to drop.
func removedExtensionNames(c *qt.C, desired *schemamodel.Database, database *catalog.Database,
	dialect string, caps capability.Capabilities, opts *config.CompareOptions,
) []string {
	c.Helper()
	diff, err := schemadiff.CompareWithDatabaseInfo(desired, database,
		catalog.ServerInfo{Dialect: dialect, Capabilities: caps}, opts)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(diff.ExtensionsRemoved))
	for _, removed := range diff.ExtensionsRemoved {
		names = append(names, removed.Name)
	}
	return names
}

// A YugabyteDB database starts with pg_stat_statements and postgres_fdw, and a
// declaration that does not name them plans no DROP EXTENSION for either
// (stokaro/ptah#3687). On 2026.1.2 the server refuses both drops, so every
// `schema apply` failed before it wrote anything declared. An ignore list that
// names nothing does not bring the removal back: the extensions are the
// server's, not a configured exception. The PostgreSQL row is the control that
// the comparison still plans a removal where nothing withholds it.
func TestCompareWithDatabaseInfo_ServerInstalledExtensionsAreNotDropped(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		opts    *config.CompareOptions
		want    []string
	}{
		{
			name: "YugabyteDB with the default ignore list", dialect: platform.YugabyteDB,
			caps: capability.YugabyteDB25(), opts: nil, want: make([]string, 0),
		},
		{
			name: "YugabyteDB with an ignore list that names nothing", dialect: platform.YugabyteDB,
			caps: capability.YugabyteDB25(), opts: config.WithIgnoredExtensions(), want: []string{"plpgsql"},
		},
		{
			name: "PostgreSQL with the default ignore list", dialect: platform.Postgres,
			caps: capability.Postgres18(), opts: nil, want: []string{"pg_stat_statements", "postgres_fdw"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			removed := removedExtensionNames(c, &schemamodel.Database{}, freshYugabyteDB(), test.dialect, test.caps, test.opts)

			c.Assert(removed, qt.DeepEquals, test.want)
		})
	}
}

// Only the removal is withheld. YugabyteDB 2025.2 installs no postgres_fdw, so
// a declaration of it on that line is created as any other would be.
func TestCompareWithDatabaseInfo_AServerExtensionALineLacksIsCreated(t *testing.T) {
	c := qt.New(t)
	database := &catalog.Database{Extensions: []catalog.Extension{
		{Name: "pg_stat_statements", Schema: "pg_catalog", Version: "1.10-yb-2.1"},
		{Name: "plpgsql", Schema: "pg_catalog", Version: "1.0"},
	}}
	desired := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "postgres_fdw"}}}

	diff, err := schemadiff.CompareWithDatabaseInfo(desired, database,
		catalog.ServerInfo{Dialect: platform.YugabyteDB, Capabilities: capability.YugabyteDB25()}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ExtensionsAdded, qt.HasLen, 1)
	c.Assert(diff.ExtensionsAdded[0].Name, qt.Equals, "postgres_fdw")
	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
}
