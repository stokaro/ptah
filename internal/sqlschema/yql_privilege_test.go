package sqlschema_test

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
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := sqlschema.NewDocument(nil)
			document.YDBDatabasePath = test.root
			database, _, err := sqlschema.ReadOnto([]byte(test.sql), "ydb", document)
			c.Assert(err, qt.IsNil)
			c.Assert(database.Grants, qt.CmpEquals(cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })), test.grants)
			c.Assert(database.RevokedGrants, qt.CmpEquals(cmpopts.EquateEmpty(), cmpopts.SortSlices(func(a, b string) bool { return a < b })), test.revoked)
			c.Assert(database.DatabasePath, qt.Equals, "")
		})
	}
}

func TestReadYQLMultiplePrivilegeTargets(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read([]byte(yqlGrantTable+"GRANT LIST ON shop, `shop/orders` TO readers, auditors;"), "ydb")
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
		"CREATE TOPIC events; GRANT SELECT ON events TO readers;",
	} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(yqlGrantTable+sql), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(database.Grants, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestReadYQLPrivilegeRootBoundary(t *testing.T) {
	c := qt.New(t)
	document := sqlschema.NewDocument(nil)
	document.YDBDatabasePath = "/Root/db"
	_, _, err := sqlschema.ReadOnto([]byte(yqlGrantTable+"GRANT SELECT ON `/Root/db-other/shop/orders` TO readers;"), "ydb", document)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err.Error(), qt.Contains, "outside database")
}
