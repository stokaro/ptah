package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/deporder"
)

// TestViewLikeCycleIn reads which objects of a set read one another in a ring,
// in the order they read each other, and finds none in a set that only chains.
func TestViewLikeCycleIn(t *testing.T) {
	tests := []struct {
		name    string
		objects []deporder.ViewLike
		want    []string
	}{
		{
			name: "two views reading each other",
			objects: []deporder.ViewLike{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT n FROM a_v"},
			},
			want: []string{"a_v", "b_v"},
		},
		{
			name: "a ring of three behind a view that only reads it",
			objects: []deporder.ViewLike{
				{Name: "head_v", Body: "SELECT n FROM c_v"},
				{Name: "c_v", Body: "SELECT n FROM d_v"},
				{Name: "d_v", Body: "SELECT n FROM e_v"},
				{Name: "e_v", Body: "SELECT n FROM c_v"},
			},
			want: []string{"c_v", "d_v", "e_v"},
		},
		{
			name: "a declared dependency closing the ring",
			objects: []deporder.ViewLike{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT n FROM t", DependsOn: []string{"a_v"}},
			},
			want: []string{"a_v", "b_v"},
		},
		{
			name: "a chain",
			objects: []deporder.ViewLike{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT n FROM c_v"},
				{Name: "c_v", Body: "SELECT n FROM t"},
			},
		},
		{
			name: "a ring through a view outside the set",
			objects: []deporder.ViewLike{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
			},
		},
		{
			name: "a name only inside a string",
			objects: []deporder.ViewLike{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT 'a_v' AS label, n FROM t"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := deporder.ViewLikeCycleIn(test.objects, "mysql", identifier.ForDialect("mysql"))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestViewLikesForCreateForDialect_ReadsAStringAsTheDialectDoes holds the
// order to where the dialect ends a string. Read with another dialect's
// rule, the string swallows the name after it and the reader is created
// first.
func TestViewLikesForCreateForDialect_ReadsAStringAsTheDialectDoes(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		reader  string
	}{
		{
			// MySQL reads "x\"y" as the string x"y. Read as a quoted name,
			// which takes no backslash, it ends at the escaped quote and the
			// rest of the body is a name that never closes.
			name:    "a double-quoted string with an escaped quote on MySQL",
			dialect: "mysql",
			reader:  `SELECT "x\"y" AS s, n FROM b_v`,
		},
		{
			name:    "a double-quoted string with an escaped quote on MariaDB",
			dialect: "mariadb",
			reader:  `SELECT "x\"y" AS s, n FROM b_v`,
		},
		{
			// SQLite has no escape string: E before a quote is a name, and
			// the backslash is a character, so the string ends at its second
			// quote.
			name:    "a backslash before the closing quote after E on SQLite",
			dialect: "sqlite",
			reader:  `SELECT e'a\' AS s, n FROM b_v`,
		},
		{
			name:    "an escape string on PostgreSQL",
			dialect: "postgres",
			reader:  `SELECT E'x\'y' AS s, n FROM b_v`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{
				{Name: "a_v", Body: test.reader},
				{Name: "b_v", Body: "SELECT n FROM t"},
			}

			got := deporder.ViewLikesForCreateForDialect(objects, test.dialect)

			c.Assert(viewLikeNames(got), qt.DeepEquals, []string{"b_v", "a_v"})
		})
	}
}
