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

// TestViewLikesForCreateForDialect_ReadsAViewOnlyWhereARelationCanAppear
// orders a_v after b_v only where a_v's query reads b_v as a relation or as
// the row type of one. A name in any other position, such as an alias, a
// column a table alias qualifies, or a common table expression of the same
// name, reads no view, and a_v keeps its place.
func TestViewLikesForCreateForDialect_ReadsAViewOnlyWhereARelationCanAppear(t *testing.T) {
	reads := []string{"b_v", "a_v"}
	keeps := []string{"a_v", "b_v"}
	tests := []struct {
		name    string
		dialect string
		reader  string
		want    []string
	}{
		{name: "a column alias after AS", dialect: "mysql", reader: "SELECT n AS b_v FROM t", want: keeps},
		{name: "a bare column alias", dialect: "mysql", reader: "SELECT n b_v FROM t", want: keeps},
		{name: "a table alias after AS qualifying a column", dialect: "mysql", reader: "SELECT b_v.n FROM t AS b_v", want: keeps},
		{name: "a bare table alias qualifying a column", dialect: "oracle", reader: "SELECT b_v.n FROM t b_v", want: keeps},
		{name: "an alias after a CAST", dialect: "mysql", reader: "SELECT CAST(n AS signed) AS b_v FROM t", want: keeps},
		{name: "a function of the same name", dialect: "postgres", reader: "SELECT n FROM b_v(1)", want: keeps},
		{
			name: "a common table expression of the same name", dialect: "mysql",
			reader: "WITH b_v AS (SELECT n FROM t) SELECT n FROM b_v", want: keeps,
		},
		{
			name: "a recursive common table expression of the same name", dialect: "postgres",
			reader: "WITH RECURSIVE b_v(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM b_v WHERE n < 3) SELECT n FROM b_v",
			want:   keeps,
		},
		{name: "after FROM", dialect: "mysql", reader: "SELECT n FROM b_v", want: reads},
		{name: "in a FROM list after a comma", dialect: "mysql", reader: "SELECT t.n FROM t, b_v WHERE t.n = b_v.n", want: reads},
		{name: "after a JOIN and its ON", dialect: "mysql", reader: "SELECT t.n FROM t AS x JOIN u ON x.n = u.n, b_v", want: reads},
		{name: "after LEFT JOIN", dialect: "sqlserver", reader: "SELECT t.n FROM t LEFT JOIN b_v ON t.n = b_v.n", want: reads},
		{name: "after CROSS APPLY", dialect: "sqlserver", reader: "SELECT t.n FROM t CROSS APPLY b_v", want: reads},
		{name: "in a parenthesized join", dialect: "postgres", reader: "SELECT t.n FROM (t JOIN b_v USING (n))", want: reads},
		{name: "after FROM ONLY", dialect: "postgres", reader: "SELECT n FROM ONLY b_v", want: reads},
		{name: "in a subquery", dialect: "oracle", reader: "SELECT n FROM t WHERE n IN (SELECT n FROM b_v)", want: reads},
		{name: "in a derived table", dialect: "mysql", reader: "SELECT x.n FROM (SELECT n FROM b_v) AS x", want: reads},
		{name: "quoted after FROM", dialect: "mysql", reader: "SELECT n FROM `b_v`", want: reads},
		{name: "after IN with no parenthesis", dialect: "clickhouse", reader: "SELECT n FROM t WHERE n IN b_v", want: reads},
		{name: "as the row type of a CAST", dialect: "postgres", reader: "SELECT CAST(NULL AS b_v) AS m FROM t", want: reads},
		{name: "as the row type after ::", dialect: "postgres", reader: "SELECT NULL::b_v AS m FROM t", want: reads},
		{
			name: "inside a common table expression's definition", dialect: "mysql",
			reader: "WITH x AS (SELECT n FROM b_v) SELECT n FROM x", want: reads,
		},
		{
			// The name of a common table expression is not in scope inside
			// its own definition, so b_v there is the view.
			name: "inside the definition of a common table expression of the same name", dialect: "postgres",
			reader: "WITH b_v AS (SELECT n FROM b_v) SELECT n FROM b_v", want: reads,
		},
		{
			name: "outside the subquery whose common table expression has the same name", dialect: "postgres",
			reader: "SELECT y.n FROM (WITH b_v AS (SELECT 1 AS n) SELECT n FROM b_v) AS y JOIN b_v ON y.n = b_v.n",
			want:   reads,
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

			c.Assert(viewLikeNames(got), qt.DeepEquals, test.want)
		})
	}
}

// TestViewLikeCycleIn_FindsNoRingThroughNamesThatReadNoView holds two views
// whose aliases or common table expressions name each other to no ring: the
// servers apply them, since neither reads the other.
func TestViewLikeCycleIn_FindsNoRingThroughNamesThatReadNoView(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		a, b    string
	}{
		{name: "crossed column aliases", dialect: "mysql", a: "SELECT n AS b_v FROM t", b: "SELECT n AS a_v FROM t"},
		{name: "crossed bare column aliases", dialect: "sqlserver", a: "SELECT n b_v FROM t", b: "SELECT n a_v FROM t"},
		{name: "crossed table aliases", dialect: "oracle", a: "SELECT b_v.n FROM t b_v", b: "SELECT a_v.n FROM t a_v"},
		{
			name: "crossed common table expressions", dialect: "postgres",
			a: "WITH b_v AS (SELECT n FROM t) SELECT n FROM b_v",
			b: "WITH a_v AS (SELECT n FROM t) SELECT n FROM a_v",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{{Name: "a_v", Body: test.a}, {Name: "b_v", Body: test.b}}

			got := deporder.ViewLikeCycleIn(objects, test.dialect, identifier.ForDialect(test.dialect))

			c.Assert(got, qt.IsNil)
		})
	}
}
