package ydbsql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

const yqlGrantTable = "CREATE TABLE `shop/orders` (id Uint64 NOT NULL, PRIMARY KEY(id));"

func TestReadYQLPrivileges(t *testing.T) {
	for _, test := range []struct {
		name, sql, root string
		grants, revoked []schemamodel.Grant
	}{
		{name: "subject quotes are literal", sql: yqlGrantTable + "GRANT SELECT ON `shop/orders` TO `\"readers\"`;", grants: []schemamodel.Grant{
			{Role: "\"readers\"", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.READ"}},
		}},
		{name: "table", sql: yqlGrantTable + "GRANT SELECT ROW, 'list' ON `shop/orders` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.LIST", "YDB.GRANULAR.SELECT_ROW"}},
		}},
		{name: "directory", sql: yqlGrantTable + "GRANT LIST ON shop TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnSchema: "shop", Privileges: []string{"YDB.GENERIC.LIST"}},
		}},
		{name: "database", root: "/Root/database", sql: "GRANT CONNECT ON `/Root/database` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnDatabase: true, Privileges: []string{"YDB.DATABASE.CONNECT"}},
		}},
		{name: "absolute table", root: "/Root/database", sql: yqlGrantTable + "GRANT 'SELECT' ON `/Root/database/shop/orders` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.READ"}},
		}},
		{name: "all is one entry", sql: yqlGrantTable + "GRANT ALL PRIVILEGES ON `shop/orders` TO readers; REVOKE SELECT ROW ON `shop/orders` FROM readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.FULL"}},
		}, revoked: []schemamodel.Grant{{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}}}},
		{name: "grant option", sql: yqlGrantTable + "GRANT SELECT ON `shop/orders` TO readers WITH GRANT OPTION;", grants: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.ACCESS.GRANT", "YDB.GENERIC.READ"}},
		}},
		{name: "revoke option removes both", sql: yqlGrantTable + "GRANT SELECT ON `shop/orders` TO readers WITH GRANT OPTION; REVOKE GRANT OPTION FOR SELECT ON `shop/orders` FROM readers;", revoked: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.READ"}},
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.ACCESS.GRANT"}},
		}},
		{name: "later grant wins", sql: yqlGrantTable + "REVOKE ALL ON `shop/orders` FROM readers; GRANT 'full' ON `shop/orders` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.FULL"}},
		}},
		{name: "topic directory", sql: "CREATE TOPIC `archive/events/source`; GRANT LIST ON archive TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnSchema: "archive", Privileges: []string{"YDB.GENERIC.LIST"}},
		}},
		{name: "topic", sql: "CREATE TOPIC events; GRANT SELECT ON events TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnObject: "events", ObjectKind: "TOPIC", Privileges: []string{"YDB.GENERIC.READ"}},
		}},
		{name: "topic by its absolute path", root: "/Root/db", sql: "CREATE TOPIC `archive/events`; REVOKE SELECT ROW ON `/Root/db/archive/events` FROM readers;",
			revoked: []schemamodel.Grant{
				{Role: "readers", OnObject: "archive.events", ObjectKind: "TOPIC", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}},
			}},
		{name: "coordination node", sql: "CREATE COORDINATION NODE `app/locks`; GRANT 'use' ON `app/locks` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnObject: "app.locks", ObjectKind: "COORDINATION NODE", Privileges: []string{"YDB.GENERIC.USE"}},
		}},
		{name: "secret", sql: "CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW); GRANT SELECT ROW ON `ext/pw` TO readers;", grants: []schemamodel.Grant{
			{Role: "readers", OnObject: "ext.pw", ObjectKind: "SECRET", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}},
		}},
		{name: "external data source and table", sql: "CREATE EXTERNAL DATA SOURCE `ext/s3` WITH (SOURCE_TYPE = 'ObjectStorage', " +
			"LOCATION = 'https://storage.invalid/b/', AUTH_METHOD = 'NONE'); CREATE EXTERNAL TABLE `ext/events` (id Int64 NOT NULL) WITH " +
			"(DATA_SOURCE = 'ext/s3', LOCATION = 'e/', FORMAT = 'csv_with_names'); GRANT SELECT ROW ON `ext/s3`, `ext/events` TO readers;",
			grants: []schemamodel.Grant{
				{Role: "readers", OnObject: "ext.s3", ObjectKind: "EXTERNAL DATA SOURCE", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}},
				{Role: "readers", OnObject: "ext.events", ObjectKind: "EXTERNAL TABLE", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}},
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			load, err := readYQLLoad(c, test.root, test.sql)
			c.Assert(err, qt.IsNil)
			database := load.Database
			c.Assert(database.Grants, qt.CmpEquals(cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })), test.grants)
			c.Assert(database.RevokedGrants, qt.CmpEquals(cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })), test.revoked)
		})
	}
}

func TestReadYQLMultiplePrivilegeTargets(t *testing.T) {
	c := qt.New(t)
	database, err := readYQL(c, yqlGrantTable+"GRANT LIST ON shop, `shop/orders` TO readers, auditors;")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "readers", OnSchema: "shop", Privileges: []string{"YDB.GENERIC.LIST"}},
		{Role: "auditors", OnSchema: "shop", Privileges: []string{"YDB.GENERIC.LIST"}},
		{Role: "readers", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.LIST"}},
		{Role: "auditors", OnTable: "shop.orders", Privileges: []string{"YDB.GENERIC.LIST"}},
	})
}

func TestReadYQLPrivilegeRefusals(t *testing.T) {
	for _, sql := range []string{
		"GRANT SELECT ON unknown TO readers;",
		"GRANT SELECT ON `shop/orders` TO ` readers `;",
		"GRANT select_row ON `shop/orders` TO readers;",
		"GRANT `SELECT` ON `shop/orders` TO readers;",
		"GRANT SELECT ON 'shop/orders' TO readers;",
		"GRANT SELECT ON `shop/orders` TO 'readers';",
		"GRANT SELECT ON `/Root/db/shop/orders` TO readers;",
		"GRANT 'ALL' ON `shop/orders` TO readers;",
		"GRANT 'YDB.GENERIC.READ' ON `shop/orders` TO readers;",
		"GRANT 'SELECT ROW' ON `shop/orders` TO readers;",
		"GRANT ALL, SELECT ON `shop/orders` TO readers;",
		"GRANT SELECT(id) ON `shop/orders` TO readers;",
		"GRANT SELECT ON `shop/orders` TO readers WITH ADMIN OPTION;",
		"REVOKE SELECT ON `shop/orders` FROM readers CASCADE;",
		"GRANT SELECT ON `shop/orders` TO readers,",
		"GRANT SELECT ON `shop/orders`, TO readers;",
		"GRANT SELECT ON `shop/../orders` TO readers;",
		"CREATE VIEW v WITH (security_invoker = TRUE) AS SELECT 1; GRANT SELECT ON v TO readers;",
		"CREATE ASYNC REPLICATION r FOR `shop/orders` AS `copy/orders` WITH (CONNECTION_STRING = 'grpc://other:2135/?database=/db'," +
			" TOKEN_SECRET_NAME = 't'); GRANT SELECT ON r TO readers;",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			database, err := readYQL(c, yqlGrantTable+sql)
			c.Assert(err, qt.IsNotNil)
			c.Assert(database.Grants, qt.HasLen, 0)
		})
	}
}

func TestReadYQLPrivilegeRootBoundary(t *testing.T) {
	c := qt.New(t)
	_, err := readYQLLoad(c, "/Root/db", yqlGrantTable+"GRANT SELECT ON `/Root/db-other/shop/orders` TO readers;")
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err.Error(), qt.Contains, "outside database")
}
