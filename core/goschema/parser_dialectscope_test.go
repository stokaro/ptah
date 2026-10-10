package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
)

// TestParse_DialectsIsAcceptedOnEveryStandaloneObjectDirective walks every
// directive that declares a standalone schema object and proves the scope
// survives parsing, canonicalized.
//
// Canonicalization is asserted rather than the raw text because the scope is
// compared against a target name later: a scope kept as `postgresql` while the
// target calls itself `postgres` would omit the object from the very dialect
// its author named, and no message would say so.
func TestParse_DialectsIsAcceptedOnEveryStandaloneObjectDirective(t *testing.T) {
	tests := []struct {
		name  string
		code  string
		scope func(db schemamodel.Database) []string
	}{
		{
			name: "extension",
			code: `//ptah:schema:extension name="pgcrypto" dialects="postgresql"
type Ext struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Extensions[0].Dialects },
		},
		{
			name: "function",
			code: `//ptah:schema:function name="tenant_id" returns="TEXT" language="plpgsql" body="BEGIN RETURN 'x'; END;" dialects="postgresql"
type Fn struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Functions[0].Dialects },
		},
		{
			name: "sequence",
			code: `//ptah:schema:sequence name="order_seq" dialects="postgresql"
type Seq struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Sequences[0].Dialects },
		},
		{
			name: "domain",
			code: `//ptah:schema:domain name="email_t" type="TEXT" dialects="postgresql"
type Dom struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Domains[0].Dialects },
		},
		{
			name: "composite",
			code: `//ptah:schema:composite name="address" fields="city:TEXT" dialects="postgresql"
type Comp struct{}`,
			scope: func(db schemamodel.Database) []string { return db.CompositeTypes[0].Dialects },
		},
		{
			name: "range",
			code: `//ptah:schema:range name="floatrange" subtype="float8" dialects="postgresql"
type Rng struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Ranges[0].Dialects },
		},
		{
			name: "view",
			code: `//ptah:schema:view name="active" body="SELECT 1" dialects="postgresql"
type V struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Views[0].Dialects },
		},
		{
			name: "matview",
			code: `//ptah:schema:matview name="stats" body="SELECT 1" dialects="postgresql"
type MV struct{}`,
			scope: func(db schemamodel.Database) []string { return db.MaterializedViews[0].Dialects },
		},
		{
			name: "trigger",
			code: `//ptah:schema:trigger name="touch" table="tenants" timing="BEFORE" event="UPDATE" body="RETURN NEW;" dialects="postgresql"
type Trg struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Triggers[0].Dialects },
		},
		{
			name: "rls policy",
			code: `//ptah:schema:rls:policy name="isolation" table="tenants" for="ALL" using="true" dialects="postgresql"
type Pol struct{}`,
			scope: func(db schemamodel.Database) []string { return must.Must(db.FeatureObjects.All())[0].Targets },
		},
		{
			name: "rls enable",
			code: `//ptah:schema:table name="tenants"
//ptah:schema:rls:enable table="tenants" dialects="postgresql"
type Ena struct{}`,
			scope: func(db schemamodel.Database) []string {
				return db.Tables[0].Facets.TargetScope(pgpolicy.TableStateKind)
			},
		},
		{
			name: "role",
			code: `//ptah:schema:role name="app_reader" dialects="postgresql"
type Rol struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Roles[0].Dialects },
		},
		{
			name: "grant",
			code: `//ptah:schema:grant role="app_reader" privilege="SELECT" on_table="tenants" dialects="postgresql"
type Grn struct{}`,
			scope: func(db schemamodel.Database) []string { return db.Grants[0].Dialects },
		},
		{
			name: "default privilege",
			code: `//ptah:schema:defaultprivilege for_role="app_owner" schema="app" object_type="TABLES" grantee="app_reader" privileges="SELECT" dialects="postgresql"
type Dpr struct{}`,
			scope: func(db schemamodel.Database) []string { return db.DefaultPrivileges[0].Dialects },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := mustParseSource(c, "models.go", "package test\n\n"+test.code+"\n")

			c.Assert(test.scope(database), qt.DeepEquals, []string{"postgres"})
		})
	}
}

// TestParse_FileScopedRLSDirectivesCarryTheScopeToo covers the two directives
// that have a second parse path.
//
// `rls:policy` and `rls:enable` may be written at file scope, above the package
// clause's declarations rather than attached to a struct, and that path is a
// separate function. A scope honored on one path and dropped on the other would
// mean the same annotation behaves differently depending on where it was
// written, with nothing to tell the author which one they got.
func TestParse_FileScopedRLSDirectivesCarryTheScopeToo(t *testing.T) {
	c := qt.New(t)

	database := mustParseSource(c, "models.go", `//ptah:schema:rls:enable table="tenants" dialects="postgres"
//ptah:schema:rls:policy name="isolation" table="tenants" for="ALL" using="true" dialects="postgres"

package test

//ptah:schema:table name="tenants"
type Tenant struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)

	c.Assert(database.Tables[0].Facets.TargetScope(pgpolicy.TableStateKind), qt.DeepEquals, []string{"postgres"})
	objects := must.Must(database.FeatureObjects.All())
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Targets, qt.DeepEquals, []string{"postgres"})
}

// TestParse_RowSecurityScopeSelectsTheModel pins which model holds a
// row-level security annotation. PostgreSQL-family scopes and no scope at all
// reach the row-security owner; a scope naming only SQL Server or ClickHouse
// stays a shared declaration, which those targets plan as a security policy
// or a row policy.
func TestParse_RowSecurityScopeSelectsTheModel(t *testing.T) {
	tests := []struct {
		name         string
		dialects     string
		wantOwned    int
		wantShared   int
		wantSwitches int
	}{
		{name: "no scope", dialects: "", wantOwned: 1, wantSwitches: 1},
		{name: "PostgreSQL family", dialects: `dialects="postgres,cockroachdb,yugabytedb"`, wantOwned: 1, wantSwitches: 1},
		{name: "SQL Server", dialects: `dialects="mssql"`, wantShared: 1},
		{name: "ClickHouse and SQL Server", dialects: `dialects="clickhouse,mssql"`, wantShared: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := mustParseSource(c, "models.go", `package test

//ptah:schema:table name="tenants"
//ptah:schema:rls:enable table="tenants" `+test.dialects+`
//ptah:schema:rls:policy name="isolation" table="tenants" for="ALL" using="true" `+test.dialects+`
type Tenant struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)

			c.Assert(ownerPolicies(c, &database), qt.HasLen, test.wantOwned)
			c.Assert(ownerSwitches(c, &database), qt.HasLen, test.wantSwitches)
			c.Assert(database.RLSPolicies, qt.HasLen, test.wantShared)
			c.Assert(database.RLSEnabledTables, qt.HasLen, test.wantShared)
		})
	}
}

// TestParse_RowSecurityScopeSelectsTheModel_FailurePath pins the refusal of a
// scope naming PostgreSQL-family targets beside others: no one model holds
// that declaration, and the refusal says how to split it.
func TestParse_RowSecurityScopeSelectsTheModel_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, err := goschema.ParseSource(noOwners, "models.go", `package test

//ptah:schema:table name="tenants"
//ptah:schema:rls:policy name="isolation" table="tenants" for="ALL" using="true" dialects="postgres,mssql"
type Tenant struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*scoped to postgres,sqlserver mixes PostgreSQL-family targets with others.*declare one scoped to postgres and another scoped to sqlserver.*`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}

// TestParse_ADialectScopeThatNamesNothingIsRefused pins the fail-closed half.
//
// Both refused values have a plausible quiet reading, and both readings hide
// the mistake: a typo read as "belongs to nothing" removes the object from
// every target, and an empty scope read as "belongs to everything" makes the
// attribute the author typed do nothing at all. Either way every command still
// exits 0 and the schema is silently not what was written.
func TestParse_ADialectScopeThatNamesNothingIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		code    string
		message string
	}{
		{
			name: "a misspelled dialect",
			code: `//ptah:schema:function name="tenant_id" returns="TEXT" language="sql" body="SELECT 1" dialects="postgress"
type Fn struct{}`,
			message: `invalid "dialects" value "postgress" on //ptah:schema:function at Fn: "postgress" names no supported dialect`,
		},
		{
			name: "one bad name among good ones",
			code: `//ptah:schema:role name="app_reader" dialects="postgres,myssql"
type Rol struct{}`,
			message: `invalid "dialects" value "postgres,myssql" on //ptah:schema:role at Rol: "myssql" names no supported dialect`,
		},
		{
			name: "an empty scope",
			code: `//ptah:schema:extension name="pgcrypto" dialects=""
type Ext struct{}`,
			message: `invalid "dialects" value "" on //ptah:schema:extension at pgcrypto: names no dialect`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource(noOwners, "models.go", "package test\n\n"+test.code+"\n")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err.Error(), qt.Contains, test.message)
		})
	}
}
