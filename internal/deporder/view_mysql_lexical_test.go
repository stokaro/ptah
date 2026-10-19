package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/deporder"
)

// The MySQL family reads its text by rules the PostgreSQL-oriented scanner
// does not share, and its descriptor states them. Measured on MySQL 8.4.11 and
// MariaDB 11.8.9: the catalog writes a view's strings with backslash escapes,
// as 'it\'s'; a # comment runs to the end of the line; and a backslash in a
// backtick-quoted name is a character of the name. Each body below reads
// z_base, or mentions a_report only where the server reads no reference, so
// z_base comes first.
func TestViewLikesForCreateForDialect_MySQLFamilyStringsAndComments(t *testing.T) {
	tests := []struct {
		name   string
		report string
		base   string
	}{
		{
			// Read without the escape, the string ends at \' and the next
			// quote hides the rest of the body, the reference included.
			name:   "a string the catalog wrote with an escaped quote",
			report: "select 'it\\'s' AS `x`,`app`.`z_base`.`id` AS `id` from `app`.`z_base`",
			base:   "select 1 AS `id`",
		},
		{
			name:   "a hash comment naming the other view",
			report: "select `id` from `app`.`z_base`",
			base:   "select 1 AS `id` # read by `app`.`a_report`\n",
		},
		{
			// Read with the escape, the backtick after the backslash would
			// not close the name, which would run to the end of the body and
			// hide the reference.
			name:   "a name ending in a backslash",
			report: "select `odd\\` AS x from app.z_base",
			base:   "select 1 AS `id`",
		},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				objects := deporder.ViewLikesForCreateForDialect([]deporder.ViewLike{
					{Name: "app.a_report", Body: test.report},
					{Name: "app.z_base", Body: test.base},
				}, dialect)
				c.Assert(viewLikeNames(objects), qt.DeepEquals, []string{"app.z_base", "app.a_report"})
			})
		}
	}
}
