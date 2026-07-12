package yamlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/yamlschema"
)

func TestParse_IssueExampleMatchesGoAnnotations(t *testing.T) {
	c := qt.New(t)

	yamlDB, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
      email: { type: VARCHAR(255), not_null: true, unique: true }
    indexes:
      idx_users_email: { fields: [email] }
`))
	c.Assert(err, qt.IsNil)

	goDB := goschema.ParseSource("schema.go", `
package test

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
	Email string

	//migrator:schema:index name="idx_users_email" fields="email"
	_ int
}
`)

	yamlSQL := strings.Join(renderer.GetOrderedCreateStatements(yamlDB, "postgres"), "\n")
	goSQL := strings.Join(renderer.GetOrderedCreateStatements(&goDB, "postgres"), "\n")
	c.Assert(yamlSQL, qt.Equals, goSQL)
}

func TestParse_CoversCurrentSchemaIR(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
enums:
  account_status: [active, suspended]
extensions:
  pg_trgm:
    if_not_exists: true
functions:
  current_tenant:
    params: ""
    returns: TEXT
    language: SQL
    security: DEFINER
    volatility: STABLE
    body: SELECT current_setting('app.current_tenant_id')
roles:
  app_user:
    login: true
    inherit: false
tables:
  tenants:
    columns:
      id: { type: SERIAL, primary: true }
  users:
    rls_enabled: true
    columns:
      id: { type: SERIAL, primary: true }
      tenant_id:
        type: INTEGER
        not_null: true
        foreign: tenants(id)
        foreign_key_name: fk_users_tenant
        on_delete: CASCADE
      status:
        type: account_status
        not_null: true
        default: active
      email:
        type: VARCHAR(255)
        not_null: true
        unique: true
        platform:
          mysql:
            type: VARCHAR(191)
    indexes:
      idx_users_email:
        fields: [email]
        unique: true
    constraints:
      chk_users_email:
        type: CHECK
        check: "position('@' in email) > 1"
rls_policies:
  users_tenant_isolation:
    table: users
    for: ALL
    to: app_user
    using: tenant_id = current_tenant()::INTEGER
`))
	c.Assert(err, qt.IsNil)

	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Name, qt.Equals, "tenants")
	c.Assert(db.Tables[1].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 5)
	c.Assert(db.Enums, qt.DeepEquals, []goschema.Enum{{Name: "account_status", Values: []string{"active", "suspended"}}})
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Returns, qt.Equals, "text")
	c.Assert(db.Functions[0].Language, qt.Equals, "sql")
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert(db.Roles[0].Inherit, qt.IsFalse)
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Dependencies["users"], qt.DeepEquals, []string{"tenants"})

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT chk_users_email CHECK (position('@' in email) > 1)`)
	c.Assert(sql, qt.Contains, `ALTER TABLE users ENABLE ROW LEVEL SECURITY;`)
	c.Assert(sql, qt.Contains, `CREATE POLICY users_tenant_isolation ON users`)
}

func TestParse_TrimsScalarEnumValues(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
enums:
  account_status: active, suspended
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Enums, qt.DeepEquals, []goschema.Enum{{
		Name:   "account_status",
		Values: []string{"active", "suspended"},
	}})
}

func TestParse_RejectsDuplicateOrderedMappingKeys(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
      id: { type: BIGSERIAL, primary: true }
`))
	c.Assert(err, qt.ErrorMatches, `parse YAML schema: duplicate key "id"`)
}

func TestParse_RejectsDuplicateTopLevelMappingKeys(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
  users:
    columns:
      id: { type: BIGSERIAL, primary: true }
`))
	c.Assert(err, qt.ErrorMatches, `(?s)parse YAML schema: .*mapping key "users" already defined.*`)
}

func TestParse_RejectsInvalidIndexesAndConstraints(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
indexes:
  idx_users_email:
    fields: [email]
`))
	c.Assert(err, qt.ErrorMatches, `top-level index "idx_users_email" requires table`)

	_, err = yamlschema.Parse([]byte(`
constraints:
  chk_users_email:
    type: CHECK
    check: "position('@' in email) > 1"
`))
	c.Assert(err, qt.ErrorMatches, `top-level constraint "chk_users_email" requires table`)

	_, err = yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      email: { type: VARCHAR(255), not_null: true }
    constraints:
      chk_users_email:
        check: "position('@' in email) > 1"
`))
	c.Assert(err, qt.ErrorMatches, `constraint "chk_users_email" requires type`)
}

func TestParse_RejectsMultipleDocuments(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
---
tables:
  posts:
    columns:
      id: { type: SERIAL, primary: true }
`))
	c.Assert(err, qt.ErrorMatches, `parse YAML schema: multiple YAML documents are not supported`)
}

func TestParse_RejectsUnsupportedViewsAndTriggers(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
views:
  active_users: {}
`))
	c.Assert(err, qt.ErrorMatches, `yaml views are not supported by the current goschema IR`)

	_, err = yamlschema.Parse([]byte(`
triggers:
  update_timestamp: {}
`))
	c.Assert(err, qt.ErrorMatches, `yaml triggers are not supported by the current goschema IR`)
}

func TestParse_RejectsUnknownColumnAttributes(t *testing.T) {
	c := qt.New(t)

	_, err := yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id:
        type: SERIAL
        primarry: true
`))
	c.Assert(err, qt.ErrorMatches, `(?s)parse YAML schema: .*field primarry not found.*`)
}
