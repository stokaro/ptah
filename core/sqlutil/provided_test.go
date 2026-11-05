package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
)

// providedScript holds strings the provider's rules read whole and the shared
// rules do not: a semicolon inside an @@...@@ string and one inside a
// double-quoted string, which the scantest rules, like YQL's, read as a string.
const providedScript = "UPSERT INTO `shop/items` (id, s) VALUES (1, @@a;b@@);\nUPSERT INTO `shop/items` (id, s) VALUES (2, \"c;d\");\n"

// The text of an engine an executable provider serves is split with the
// scanner its provider registered, under each of its names, so a semicolon
// inside one of its strings ends nothing; a delimiter directive is left as
// written, since the shared rewriter reads strings by other rules. Without the
// name the provider's engine was split by the shared rules: `@@a` and `b@@)`
// came out as two statements.
func TestSplitSQLStatementsForDialect_HappyPath_AProvidersScanner(t *testing.T) {
	c := qt.New(t)
	release, err := lexerext.Provide(providerscan.New(scantest.Scan), "split-engine", "split-alias")
	c.Assert(err, qt.IsNil)
	c.Cleanup(release)
	want := []string{
		"UPSERT INTO `shop/items` (id, s) VALUES (1, @@a;b@@)",
		"UPSERT INTO `shop/items` (id, s) VALUES (2, \"c;d\")",
	}
	for _, name := range []string{"split-engine", " Split-Alias "} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.SplitSQLStatementsForDialect(providedScript, name), qt.DeepEquals, want)
			c.Assert(sqlutil.StripCommentsForDialect("SELECT @@--x@@; -- note\n", name), qt.Equals, "SELECT @@--x@@; \n")
			c.Assert(sqlutil.SplitSQLStatementsForDialect("-- atlas:delimiter $$\nSELECT 1$$\n", name), qt.DeepEquals,
				[]string{"-- atlas:delimiter $$\nSELECT 1$$"})
		})
	}
}

// A name no provider registered is read by the shared rules, as a blank
// dialect is.
func TestSplitSQLStatementsForDialect_FailurePath_NoProvidersScanner(t *testing.T) {
	c := qt.New(t)
	c.Assert(sqlutil.SplitSQLStatementsForDialect(providedScript, "unscanned-engine"), qt.DeepEquals, sqlutil.SplitSQLStatementsForDialect(providedScript, ""))
	c.Assert(sqlutil.SplitSQLStatementsForDialect(providedScript, "unscanned-engine"), qt.HasLen, 3)
}
