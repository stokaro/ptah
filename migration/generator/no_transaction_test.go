package generator

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestWithNoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	sql := "ALTER TYPE status ADD VALUE 'archived';\n"
	got := withNoTransactionDirective(sql)

	c.Assert(got, qt.Equals, "-- +ptah no_transaction\n"+sql)
	c.Assert(migrator.ParseFileDirectives(got), qt.DeepEquals, map[string]string{
		migrator.DirectiveNoTransaction: "true",
	})
	c.Assert(withNoTransactionDirective(got), qt.Equals, got)
	c.Assert(withNoTransactionDirective(""), qt.Equals, "")
}
