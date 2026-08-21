package compare

// White-box testing required: identity key construction is a package-local
// primitive with no exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
)

// TestTableMemberKey_FoldsTheMemberWhereTheEngineDoes holds the half of the key
// that was compared verbatim.
//
// The table half has always been normalized through the dialect's identifier
// semantics; the member half was the raw string. That is correct only where the
// engine compares names verbatim, and Oracle does not: an unquoted name is
// folded to upper case, so a declaration writing `orders_total_check` and a
// catalog reporting ORDERS_TOTAL_CHECK are the same constraint. Comparing the
// two strings made every apply drop one and add the other, forever
// (stokaro/ptah#1875).
//
// The rows are one per comparison rule rather than one per dialect, because
// what decides the answer is the rule and not the name of the engine carrying
// it. PostgreSQL is here to prove the fold does not spread: an exact dialect
// must still tell two spellings apart, and a test that only checked Oracle
// would pass against a key that lower-cased everything.
func TestTableMemberKey_FoldsTheMemberWhereTheEngineDoes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		// wantEqual is whether the two spellings name the same member.
		wantEqual bool
	}{
		{name: "oracle folds", dialect: platform.Oracle, wantEqual: true},
		{name: "sqlite folds", dialect: platform.SQLite, wantEqual: true},
		{name: "postgres does not", dialect: platform.Postgres, wantEqual: false},
		{name: "mysql does not", dialect: platform.MySQL, wantEqual: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics := identifier.ForDialect(test.dialect)
			semantics.DefaultSchema = "app"

			declared := newTableMemberKey("orders", "orders_total_check", semantics)
			catalog := newTableMemberKey("orders", "ORDERS_TOTAL_CHECK", semantics)

			c.Assert(declared == catalog, qt.Equals, test.wantEqual)

			// The control every row shares: two members that are genuinely
			// different names must never collide, whatever the fold.
			other := newTableMemberKey("orders", "orders_status_check", semantics)
			c.Assert(declared, qt.Not(qt.Equals), other)
		})
	}
}
