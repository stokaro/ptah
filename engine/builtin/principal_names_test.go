package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

func TestValidateSchema_MySQLRefusesPaddedPrincipals(t *testing.T) {
	for _, test := range []struct {
		name     string
		database schemamodel.Database
	}{
		{name: "role", database: schemamodel.Database{Roles: []schemamodel.Role{{Name: " reader", Inherit: true}}}},
		{name: "grant", database: schemamodel.Database{Grants: []schemamodel.Grant{{Role: "reader ", OnTable: "t", Privileges: []string{"SELECT"}}}}},
		{name: "revoke", database: schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "\treader", OnTable: "t", Privileges: []string{"SELECT"}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchema(&test.database, "mysql")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*principal .* has surrounding white space, which Ptah does not support for MySQL; .*`)
		})
	}
}

func TestValidateSchema_RefusesUnsupportedPrincipalSpellingsFromSQLAndYAML(t *testing.T) {
	for _, test := range []struct{ name, dialect, sql, yaml string }{
		{name: "MySQL role", dialect: "mysql", sql: "CREATE ROLE ` reader`;", yaml: "roles:\n  ' reader': {}\n"},
		{name: "MySQL grant", dialect: "mysql", sql: "GRANT SELECT ON t TO ` reader`;", yaml: "grants:\n  read:\n    role: ' reader'\n    on_table: t\n    privileges: [SELECT]\n"},
		{name: "MySQL revoke", dialect: "mysql", sql: "REVOKE SELECT ON t FROM ` reader`;", yaml: "revokes:\n  read:\n    role: ' reader'\n    on_table: t\n    privileges: [SELECT]\n"},
		{name: "MariaDB role", dialect: "mariadb", sql: "CREATE ROLE `reader `;", yaml: "roles:\n  'reader ': {}\n"},
		{name: "MariaDB grant", dialect: "mariadb", sql: "GRANT SELECT ON t TO `reader `;", yaml: "grants:\n  read:\n    role: 'reader '\n    on_table: t\n    privileges: [SELECT]\n"},
		{name: "MariaDB revoke", dialect: "mariadb", sql: "REVOKE SELECT ON t FROM `reader `;", yaml: "revokes:\n  read:\n    role: 'reader '\n    on_table: t\n    privileges: [SELECT]\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fromSQL, _, err := sqlschema.Read([]byte(test.sql), test.dialect)
			c.Assert(err, qt.IsNil)
			fromYAML, err := yamlschema.Parse(c.Context(), builtintest.Runtime().YAML(), []byte(test.yaml))
			c.Assert(err, qt.IsNil)

			c.Assert(builtin.ValidateSchema(&fromSQL, test.dialect), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(builtin.ValidateSchema(fromYAML, test.dialect), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

func TestValidateSchema_PrincipalNameAcceptanceControls(t *testing.T) {
	for _, test := range []struct{ dialect, name string }{
		{dialect: "mysql", name: "reader"},
		{dialect: "mariadb", name: " reader"},
		{dialect: "mariadb", name: "reader\t"},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{
				Roles:  []schemamodel.Role{{Name: test.name, Inherit: true}},
				Grants: []schemamodel.Grant{{Role: test.name, OnTable: "t", Privileges: []string{"SELECT"}}},
			}

			c.Assert(builtin.ValidateSchema(database, test.dialect), qt.IsNil)
		})
	}
}

func TestValidateSchema_MariaDBRefusesTrailingSpacePrincipals(t *testing.T) {
	for _, test := range []struct {
		name     string
		database schemamodel.Database
	}{
		{name: "role", database: schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader ", Inherit: true}}}},
		{name: "grant", database: schemamodel.Database{Grants: []schemamodel.Grant{{Role: "reader ", OnTable: "t", Privileges: []string{"SELECT"}}}}},
		{name: "revoke", database: schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: "reader ", OnTable: "t", Privileges: []string{"SELECT"}}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := builtin.ValidateSchema(&test.database, "mariadb")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*principal .* has trailing ASCII spaces, which CREATE ROLE removes; .*`)
		})
	}
}
