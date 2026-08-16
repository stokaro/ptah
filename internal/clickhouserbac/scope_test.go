package clickhouserbac_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/clickhouserbac"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// TestScope_String_QuotesTheIdentifiersAndNotTheWildcard pins the one rendering
// decision that changes meaning rather than formatting.
//
// `db`.* is a database scope; `db`.`*` is a table literally named `*`. Quoting
// the wildcard would silently turn the first into the second, and the server
// would accept it — creating a grant on a table nobody has, while the plan
// claimed a database-wide grant.
func TestScope_String_QuotesTheIdentifiersAndNotTheWildcard(t *testing.T) {
	tests := []struct {
		name  string
		scope clickhouserbac.Scope
		want  string
	}{
		{
			name:  "database scope leaves the wildcard bare",
			scope: clickhouserbac.Scope{Database: "shop"},
			want:  "`shop`.*",
		},
		{
			name:  "table scope quotes both parts",
			scope: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			want:  "`shop`.`orders`",
		},
		{
			name:  "a backtick in a name cannot terminate the identifier",
			scope: clickhouserbac.Scope{Database: "sh`op", Table: "or`ders"},
			want:  "`sh``op`.`or``ders`",
		},
		{
			name:  "a name that looks like syntax stays an identifier",
			scope: clickhouserbac.Scope{Database: "shop", Table: "*"},
			want:  "`shop`.`*`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.scope.String(), qt.Equals, test.want)
		})
	}
}

// TestScope_Contains_IsTheServerAbsorptionRule pins the rule ClickHouse applies
// silently: a narrower grant is absorbed into a broader one for the same
// privilege, measured in both orders on 24.10 and 26.7.
//
// Ptah has to know it to refuse the pair, so this is the rule's one definition.
func TestScope_Contains_IsTheServerAbsorptionRule(t *testing.T) {
	tests := []struct {
		name  string
		outer clickhouserbac.Scope
		inner clickhouserbac.Scope
		want  bool
	}{
		{
			name:  "a database scope covers a table in it",
			outer: clickhouserbac.Scope{Database: "shop"},
			inner: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			want:  true,
		},
		{
			name:  "a scope covers itself",
			outer: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			inner: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			want:  true,
		},
		{
			name:  "a table scope does not cover its database",
			outer: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			inner: clickhouserbac.Scope{Database: "shop"},
			want:  false,
		},
		{
			name:  "a table scope does not cover a sibling table",
			outer: clickhouserbac.Scope{Database: "shop", Table: "orders"},
			inner: clickhouserbac.Scope{Database: "shop", Table: "items"},
			want:  false,
		},
		{
			name:  "a database scope does not reach another database",
			outer: clickhouserbac.Scope{Database: "shop"},
			inner: clickhouserbac.Scope{Database: "warehouse", Table: "orders"},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.outer.Contains(test.inner), qt.Equals, test.want)
		})
	}
}

func TestScopeOf_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		grant           goschema.Grant
		defaultDatabase string
		want            clickhouserbac.Scope
	}{
		{
			name:  "on_schema is a database scope",
			grant: goschema.Grant{Role: "reader", OnSchema: "shop"},
			want:  clickhouserbac.Scope{Database: "shop"},
		},
		{
			name:  "a qualified on_table names its own database",
			grant: goschema.Grant{Role: "reader", OnTable: "shop.orders"},
			want:  clickhouserbac.Scope{Database: "shop", Table: "orders"},
		},
		{
			name:            "an unqualified on_table resolves against the default",
			grant:           goschema.Grant{Role: "reader", OnTable: "orders"},
			defaultDatabase: "shop",
			want:            clickhouserbac.Scope{Database: "shop", Table: "orders"},
		},
		{
			name:            "a qualified on_table outranks the default",
			grant:           goschema.Grant{Role: "reader", OnTable: "warehouse.orders"},
			defaultDatabase: "shop",
			want:            clickhouserbac.Scope{Database: "warehouse", Table: "orders"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope, err := clickhouserbac.ScopeOf(test.grant, test.defaultDatabase)
			c.Assert(err, qt.IsNil)
			c.Assert(scope, qt.Equals, test.want)
		})
	}
}

func TestScopeOf_FailurePath(t *testing.T) {
	tests := []struct {
		name            string
		grant           goschema.Grant
		defaultDatabase string
		wantErr         string
	}{
		{
			name:    "sequences do not exist",
			grant:   goschema.Grant{Role: "reader", OnSequence: "order_id_seq"},
			wantErr: `grant to role "reader" names on_sequence "order_id_seq": ClickHouse has no sequences`,
		},
		{
			name:    "two scopes at once",
			grant:   goschema.Grant{Role: "reader", OnSchema: "shop", OnTable: "shop.orders"},
			wantErr: `.*names both on_schema "shop" and on_table "shop.orders".*`,
		},
		{
			name:    "no object at all",
			grant:   goschema.Grant{Role: "reader"},
			wantErr: `grant to role "reader" names no object.*`,
		},
		{
			name:    "an unqualified table with no default to resolve against",
			grant:   goschema.Grant{Role: "reader", OnTable: "orders"},
			wantErr: `.*names table "orders" with no database.*`,
		},
		{
			name:    "three parts",
			grant:   goschema.Grant{Role: "reader", OnTable: "cluster.shop.orders"},
			wantErr: `.*a ClickHouse scope has at most two parts`,
		},
		{
			// A trailing dot must not read as an empty table name. An empty
			// Table is how Scope spells "the whole database", so accepting
			// this would widen one table's privilege to every table in the
			// database — silently, from a typo.
			name:    "a trailing dot, which would widen the scope",
			grant:   goschema.Grant{Role: "reader", OnTable: "shop."},
			wantErr: `.*names table "shop\." with no table part.*`,
		},
		{
			name:            "a trailing dot is refused even with a default database",
			grant:           goschema.Grant{Role: "reader", OnTable: "shop."},
			defaultDatabase: "warehouse",
			wantErr:         `.*with no table part.*`,
		},
		{
			name:    "a wildcard database",
			grant:   goschema.Grant{Role: "reader", OnTable: "*.orders"},
			wantErr: `.*not wildcard scopes`,
		},
		{
			name:    "a wildcard schema",
			grant:   goschema.Grant{Role: "reader", OnSchema: "*"},
			wantErr: `.*not wildcard scopes`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := clickhouserbac.ScopeOf(test.grant, test.defaultDatabase)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestScopeOfLive_ReadsTheCatalogShape pins the reader's side of the same
// model, and it reads ObjectType rather than guessing from which field is
// populated.
//
// The two object types put the database in DIFFERENT fields, and that is the
// shared [types.DBGrant] contract rather than anything ClickHouse decided: a
// SCHEMA-typed row carries its target in ObjectName with Schema empty — which
// is what [types.DBGrant.QualifiedTarget] returns, what the PostgreSQL reader
// writes, and what internal/convert/dbschematogo reads — while a TABLE-typed
// row carries the schema in Schema and the table in ObjectName.
//
// The rows below are deliberately spelled BOTH ways for a database scope. An
// earlier version of this function read (Schema, ObjectName) positionally, and
// a test that only offered `{Schema: "shop"}` passed against it while every
// database-scoped grant compared unequal to itself on a live server.
func TestScopeOfLive_ReadsTheCatalogShape(t *testing.T) {
	tests := []struct {
		name  string
		grant types.DBGrant
		want  clickhouserbac.Scope
	}{
		{
			name:  "a table row",
			grant: types.DBGrant{ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			want:  clickhouserbac.Scope{Database: "shop", Table: "orders"},
		},
		{
			name:  "a database row, spelled the way the shared contract spells it",
			grant: types.DBGrant{ObjectType: "SCHEMA", ObjectName: "shop"},
			want:  clickhouserbac.Scope{Database: "shop"},
		},
		{
			name:  "the object type decides, whatever case it is written in",
			grant: types.DBGrant{ObjectType: "schema", ObjectName: "shop"},
			want:  clickhouserbac.Scope{Database: "shop"},
		},
		{
			// A row with no object type at all is a table row, because that is
			// the only shape a reader that does not set the field can mean.
			name:  "an untyped row falls back to the table spelling",
			grant: types.DBGrant{Schema: "shop", ObjectName: "orders"},
			want:  clickhouserbac.Scope{Database: "shop", Table: "orders"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(clickhouserbac.ScopeOfLive(test.grant), qt.Equals, test.want)
		})
	}
}

// TestScopeOfLive_AgreesWithScopeOfAcrossTheBoundary is the assertion the
// convergence of every ClickHouse grant rests on: a declared grant and the live
// row it produces must resolve to the SAME scope, or the comparator sees a
// grant it just issued as still missing and re-issues it forever.
//
// It is written as a round trip rather than as two independent expectations
// because the defect it guards against was exactly a disagreement between the
// two sides, each of which looked right on its own.
func TestScopeOfLive_AgreesWithScopeOfAcrossTheBoundary(t *testing.T) {
	tests := []struct {
		name    string
		grant   goschema.Grant
		live    types.DBGrant
		wantSQL string
	}{
		{
			name:    "a database scope",
			grant:   goschema.Grant{Role: "reader", Privileges: []string{"SELECT"}, OnSchema: "shop"},
			live:    types.DBGrant{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "shop"},
			wantSQL: "`shop`.*",
		},
		{
			name:    "a table scope",
			grant:   goschema.Grant{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "shop.orders"},
			live:    types.DBGrant{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "shop", ObjectName: "orders"},
			wantSQL: "`shop`.`orders`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, err := clickhouserbac.ScopeOf(test.grant, "")
			c.Assert(err, qt.IsNil)
			live := clickhouserbac.ScopeOfLive(test.live)

			c.Assert(live, qt.Equals, declared)
			c.Assert(declared.String(), qt.Equals, test.wantSQL)

			// And the third side of the same triangle, driven through the real
			// converter rather than a copy of its rule: describing the live row
			// as a declaration and resolving THAT has to land on the same scope.
			// This is the path `ptah db read` output takes back into a
			// comparison, and a reader that filled the wrong field would break
			// it while both assertions above still passed.
			described := dbschematogo.ConvertDBSchemaToGoSchema(
				&types.DBSchema{Grants: []types.DBGrant{test.live}},
			)
			c.Assert(described.Grants, qt.HasLen, 1)
			redeclared, err := clickhouserbac.ScopeOf(described.Grants[0], "")
			c.Assert(err, qt.IsNil)
			c.Assert(redeclared, qt.Equals, declared)
		})
	}
}
