package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
)

// ttlOwners is an owner of a TTL table option on documents of target
// fixture, whose reader records each request.
func ttlOwners(requests *[]sqlext.Request) sqlext.Set {
	return must.Must(sqlext.NewSet(sqlext.Extension{
		Owner: "example.org/ttl", Targets: []string{"fixture"}, Facets: []schemaext.Kind{prefixKind},
		Patterns: []sqlext.Pattern{{Name: "ttl", Site: sqlext.TableOption, Words: []string{"TTL"}, Targeted: true}},
		Parser:   prefixReader{requests: requests},
	}))
}

// TestParse_AClaimStatesWhereItIsWritten hands an owner the place of each
// claim in the document as its author wrote it, and the document's length: a
// byte column, where characters of two bytes come before the claim, a line
// counted after a CRLF, and an offset in the written document where the
// parser read a rewritten one, without a delimiter directive.
func TestParse_AClaimStatesWhereItIsWritten(t *testing.T) {
	for _, test := range []struct {
		name string
		text string
		want sqlext.Position
	}{
		{"after characters of two bytes", "CREATE TABLE заказы (id int) TTL 2 HOURS;\n", sqlext.Position{Offset: 35, Line: 1, Column: 36}},
		{"after a CRLF", "CREATE TABLE a (id int);\r\nCREATE TABLE b (id int)\r\n  TTL 1;\r\n", sqlext.Position{Offset: 53, Line: 3, Column: 3}},
		{"after a lone CR", "CREATE TABLE a (id int);\rCREATE TABLE b (id int) TTL 1;\r", sqlext.Position{Offset: 49, Line: 2, Column: 25}},
		{"below a delimiter directive", "-- atlas:delimiter //\nCREATE TABLE b (id int) TTL 1//\n", sqlext.Position{Offset: 46, Line: 2, Column: 25}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request

			_, err := parser.NewParser(test.text, parser.WithDialect("fixture"), parser.WithSQL(ttlOwners(&requests))).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(requests, qt.HasLen, 1)
			c.Assert(requests[0].Length, qt.Equals, len(test.text))
			claims := requests[0].Claims
			c.Assert(claims, qt.HasLen, 1)
			c.Assert(claims[0].Written, qt.Equals, test.want)
			c.Assert(test.text[test.want.Offset:test.want.Offset+len(claims[0].Text)], qt.Equals, claims[0].Text)
		})
	}
}

// TestParse_AClaimOfAnotherTargetLeavesClickHouseItsTTL parses a ClickHouse
// document beside an owner that claims TTL on another target's documents: the
// owner is not asked, and the clause is ClickHouse's table option.
func TestParse_AClaimOfAnotherTargetLeavesClickHouseItsTTL(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request

	parsed, err := parser.NewParser("CREATE TABLE t (d Date NOT NULL) ENGINE = MergeTree ORDER BY d TTL d + INTERVAL 1 DAY;",
		parser.WithDialect("clickhouse"), parser.WithSQL(ttlOwners(&requests))).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.HasLen, 0)
	table, isTable := parsed.Statements[0].(*ast.CreateTableNode)
	c.Assert(isTable, qt.IsTrue)
	c.Assert(table.Options["TTL"], qt.Equals, "d + INTERVAL 1 DAY")
	c.Assert(table.Facets.Len(), qt.Equals, 0)
}
