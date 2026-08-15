package dialectscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/dialectscope"
)

// TestParse_ResolvesEverySpellingAndRefusesTheQuietReadings pins both halves of
// the parse contract at once, because they are the same decision seen from two
// sides: what a scope MEANS is exactly what it must refuse to guess.
//
// The alias rows matter on their own. Ptah accepts 24 spellings of 9 dialects,
// and a scope that matched the raw string would scope an object to `postgresql`
// while the target called itself `postgres` -- an object silently missing from
// the dialect its author named, which is the failure this attribute exists to
// remove rather than to introduce a second time.
func TestParse_ResolvesEverySpellingAndRefusesTheQuietReadings(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
		err  string
	}{
		{
			name: "single dialect",
			raw:  "postgres",
			want: []string{"postgres"},
		},
		{
			name: "alias resolves to the canonical name",
			raw:  "postgresql",
			want: []string{"postgres"},
		},
		{
			name: "several aliases of several dialects",
			raw:  "pgx, crdb ,ysql",
			want: []string{"cockroachdb", "postgres", "yugabytedb"},
		},
		{
			name: "two spellings of one dialect collapse",
			raw:  "sqlite,sqlite3",
			want: []string{"sqlite"},
		},
		{
			name: "case and surrounding space are not part of the name",
			raw:  "  MariaDB  ",
			want: []string{"mariadb"},
		},
		{
			name: "result is sorted regardless of how it was written",
			raw:  "sqlserver,mysql,clickhouse",
			want: []string{"clickhouse", "mysql", "sqlserver"},
		},
		{
			name: "a name that is no dialect is refused",
			raw:  "postgress",
			err:  `"postgress" names no supported dialect`,
		},
		{
			name: "one bad name in a good list is still refused",
			raw:  "postgres,myssql",
			err:  `"myssql" names no supported dialect`,
		},
		{
			name: "an empty scope is refused rather than read as every dialect",
			raw:  "",
			err:  "names no dialect",
		},
		{
			name: "a scope of only separators is refused too",
			raw:  " , , ",
			err:  "names no dialect",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			scope, err := dialectscope.Parse(test.raw)

			c.Assert(scopeOutcome(scope, err), qt.DeepEquals, parseOutcome{Scope: test.want, Error: test.err})
		})
	}
}

// TestIncludes_EmptyScopeBelongsEverywhereAndNarrowingOnlyNarrows holds the
// membership rule that makes this feature backward compatible: a declaration
// written before the attribute existed carries no scope and must keep reaching
// every target.
func TestIncludes_EmptyScopeBelongsEverywhereAndNarrowingOnlyNarrows(t *testing.T) {
	tests := []struct {
		name    string
		scope   []string
		dialect string
		want    bool
	}{
		{
			name:    "no scope reaches a dialect it never named",
			scope:   nil,
			dialect: "mysql",
			want:    true,
		},
		{
			name:    "an empty scope slice is the same as no scope",
			scope:   []string{},
			dialect: "clickhouse",
			want:    true,
		},
		{
			name:    "a named dialect is included",
			scope:   []string{"postgres"},
			dialect: "postgres",
			want:    true,
		},
		{
			name:    "an alias of a named dialect is included",
			scope:   []string{"postgres"},
			dialect: "postgresql",
			want:    true,
		},
		{
			name:    "an unnamed dialect is excluded",
			scope:   []string{"postgres"},
			dialect: "mysql",
			want:    false,
		},
		{
			name:    "a postgres scope does not carry the postgres family with it",
			scope:   []string{"postgres"},
			dialect: "cockroachdb",
			want:    false,
		},
		{
			name:    "one member of a multi-dialect scope is included",
			scope:   []string{"cockroachdb", "postgres", "yugabytedb"},
			dialect: "yugabyte",
			want:    true,
		},
		{
			name:    "a target that names no platform is never projected away",
			scope:   []string{"postgres"},
			dialect: "not-a-database",
			want:    true,
		},
		{
			name:    "an empty target is never projected away",
			scope:   []string{"postgres"},
			dialect: "",
			want:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dialectscope.Includes(test.scope, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestAttribute_IsTheSpellingTheAnnotationUses pins the shared constant, which
// the directive table, the parser and the exporter all read instead of writing
// the word themselves.
func TestAttribute_IsTheSpellingTheAnnotationUses(t *testing.T) {
	c := qt.New(t)

	c.Assert(dialectscope.Attribute, qt.Equals, "dialects")
}

// parseOutcome renders a Parse result as one comparable value, so a table row
// asserts the scope and the error together rather than branching on which of
// the two it expected.
type parseOutcome struct {
	Scope []string
	Error string
}

func scopeOutcome(scope []string, err error) parseOutcome {
	return parseOutcome{Scope: scope, Error: errorText(err)}
}

func errorText(err error) string {
	messages := map[bool]func() string{
		true:  func() string { return err.Error() },
		false: func() string { return "" },
	}
	return messages[err != nil]()
}
