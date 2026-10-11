package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// freshYugabyteDB is what a YugabyteDB 2026.1.2 database made by CREATE
// DATABASE reports, before anybody creates anything in it.
func freshYugabyteDB() *catalog.Database {
	return holdingOwnerExtensions("pg_catalog", "pg_stat_statements", "plpgsql", "postgres_fdw")
}

// removedExtensionNames lists the extensions a comparison plans to drop.
func removedExtensionNames(c *qt.C, desired *schemamodel.Database, database *catalog.Database,
	dialect string, caps capability.Capabilities, opts *config.CompareOptions,
) []string {
	c.Helper()
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, database,
		catalog.ServerInfo{Dialect: dialect, Capabilities: caps}, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	_, dropped := installedAndDropped(diff)
	return append(make([]string, 0, len(dropped)), dropped...)
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

			removed := removedExtensionNames(c, declaringOwnerExtensions(), freshYugabyteDB(), test.dialect, test.caps, test.opts)

			c.Assert(removed, qt.DeepEquals, test.want)
		})
	}
}

// Only the removal is withheld. YugabyteDB 2025.2 installs no postgres_fdw, so
// a declaration of it on that line is created as any other would be.
func TestCompareWithDatabaseInfo_AServerExtensionALineLacksIsCreated(t *testing.T) {
	c := qt.New(t)
	database := holdingOwnerExtensions("pg_catalog", "pg_stat_statements", "plpgsql")
	desired := declaringOwnerExtensions("postgres_fdw")

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, database,
		catalog.ServerInfo{Dialect: platform.YugabyteDB, Capabilities: capability.YugabyteDB25()}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	installed, dropped := installedAndDropped(diff)
	c.Assert(installed, qt.DeepEquals, []string{"postgres_fdw"})
	c.Assert(dropped, qt.HasLen, 0)
}
