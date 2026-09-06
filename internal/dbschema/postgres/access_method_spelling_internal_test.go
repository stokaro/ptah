package postgres

// White-box testing required: accessMethodSpelling is consulted while a
// catalog row is being mapped, and nothing at the package boundary separates
// its answer from the rest of an index read. The exported path needs a live
// CockroachDB to reach it, which integration/cockroachdb covers; this pins the
// mapping itself, including the direction that must NOT fire.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// The expectations are one live measurement each, taken 2026-08-30 against
// CockroachDB v26.3.1 and PostgreSQL 18 and tabulated at
// catalogOnlyAccessMethods.
func TestAccessMethodSpellingRewritesCockroachCatalogNames(t *testing.T) {
	tests := []struct {
		name   string
		amname string
		want   string
	}{
		{name: "prefix is btree", amname: "prefix", want: "btree"},
		{name: "inverted is gin", amname: "inverted", want: "gin"},
		{name: "mixed case", amname: "PREFIX", want: "btree"},
		{name: "padded", amname: "  inverted  ", want: "gin"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := accessMethodSpelling(platform.CockroachDB, test.amname)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A name CockroachDB does accept back is left alone. Without this, a rewrite
// that returned "btree" for everything would satisfy the table above.
func TestAccessMethodSpellingLeavesAcceptedCockroachNames(t *testing.T) {
	tests := []struct {
		name   string
		amname string
	}{
		{name: "btree", amname: "btree"},
		{name: "gin", amname: "gin"},
		{name: "empty", amname: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := accessMethodSpelling(platform.CockroachDB, test.amname)

			c.Assert(got, qt.Equals, test.amname)
		})
	}
}

// The direction that must not fire. PostgreSQL permits CREATE ACCESS METHOD, so
// an extension may install one genuinely called `prefix`; rewriting it there
// would corrupt a real name. The same two inputs are used as above, so this is
// the inverse of that table rather than a different question.
func TestAccessMethodSpellingLeavesOtherDialectsAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "yugabytedb", dialect: platform.YugabyteDB},
		{name: "spanner", dialect: platform.Spanner},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(accessMethodSpelling(test.dialect, "prefix"), qt.Equals, "prefix")
			c.Assert(accessMethodSpelling(test.dialect, "inverted"), qt.Equals, "inverted")
			c.Assert(accessMethodSpelling(test.dialect, "btree"), qt.Equals, "btree")
		})
	}
}
