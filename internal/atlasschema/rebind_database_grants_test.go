package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
)

// shadowGrants is a one-database read of app_shadow holding the grants a
// replay of `GRANT SELECT ON t1`, `GRANT SELECT ON v1` and `GRANT SHOW VIEW ON *`
// writes there, with a grant on a table of another database and one on that
// whole database beside them.
func shadowGrants() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t1"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app_shadow", ObjectName: "t1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "app_shadow", ObjectName: "v1", WithOption: true},
			{Role: "reader", Privilege: "SHOW VIEW", ObjectType: "SCHEMA", ObjectName: "app_shadow"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "x"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "other"},
		},
	}
}

// A grant read from the shadow database names it as the database of its
// object, and the target's read names the target's database. Each grant on an
// object of the shadow database is the target's grant on its own object: a
// table grant is left unqualified, as the table is, and a grant on the whole
// database names the target's database. A grant on another database keeps it.
func TestRebindDatabaseGrants_HappyPath(t *testing.T) {
	want := []catalog.Grant{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "v1", WithOption: true},
		{Role: "reader", Privilege: "SHOW VIEW", ObjectType: "SCHEMA", ObjectName: "app"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "other", ObjectName: "x"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "other"},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			replay := shadowGrants()

			got := atlasschema.RebindDatabase(replay,
				catalog.ServerInfo{Dialect: dialect, Schema: "app_shadow"},
				catalog.ServerInfo{Dialect: dialect, Schema: "app"},
			)

			c.Assert(got.Grants, qt.DeepEquals, want)
			c.Assert(replay, qt.DeepEquals, shadowGrants(), qt.Commentf("the read the caller holds is not modified"))
		})
	}
}

// The grants stay as read where nothing moves: a database of the same name, a
// read of a whole server, and a dialect whose schema is a namespace inside the
// connected database, where a shadow database keeps the default schema.
func TestRebindDatabaseGrants_LeftInPlace(t *testing.T) {
	tests := []struct {
		name           string
		source, target catalog.ServerInfo
	}{
		{
			name:   "a database of the same name",
			source: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app_shadow"},
			target: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app_shadow"},
		},
		{
			name:   "a whole server read",
			source: catalog.ServerInfo{Dialect: platform.MySQL, WholeServer: true},
			target: catalog.ServerInfo{Dialect: platform.MySQL, Schema: "app"},
		},
		{
			name:   "a schema that is not a database",
			source: catalog.ServerInfo{Dialect: platform.Postgres, Schema: "app_shadow"},
			target: catalog.ServerInfo{Dialect: platform.Postgres, Schema: "app"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.RebindDatabase(shadowGrants(), test.source, test.target)

			c.Assert(got.Grants, qt.DeepEquals, shadowGrants().Grants)
		})
	}
}
