package clickhouserbac_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/clickhouserbac"
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
// model. A database-wide row reports an empty table, exactly as system.grants
// records it with table = NULL.
func TestScopeOfLive_ReadsTheCatalogShape(t *testing.T) {
	tests := []struct {
		name  string
		grant types.DBGrant
		want  clickhouserbac.Scope
	}{
		{
			name:  "a table row",
			grant: types.DBGrant{Schema: "shop", ObjectName: "orders"},
			want:  clickhouserbac.Scope{Database: "shop", Table: "orders"},
		},
		{
			name:  "a database row carries no table",
			grant: types.DBGrant{Schema: "shop"},
			want:  clickhouserbac.Scope{Database: "shop"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(clickhouserbac.ScopeOfLive(test.grant), qt.Equals, test.want)
		})
	}
}
