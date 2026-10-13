package parser_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
)

const prefixKind schemaext.Kind = "example.org/prefix/setting"

type prefixSetting struct{ Text string }

func (*prefixSetting) Kind() schemaext.Kind     { return prefixKind }
func (p *prefixSetting) Clone() schemaext.Value { return new(*p) }
func (p *prefixSetting) Equal(other schemaext.Value) bool {
	value, ok := other.(*prefixSetting)
	return ok && *value == *p
}

// prefixReader records source spans and returns a facet for each table. It
// refuses a designated claim so a later owner error can test atomic parsing.
type prefixReader struct{ requests *[]sqlext.Request }

func (r prefixReader) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*r.requests = append(*r.requests, request)
	result := sqlext.Result{}
	for _, claim := range request.Claims {
		if strings.Contains(claim.Text, "REFUSED") {
			return sqlext.Result{}, fmt.Errorf("prefix refused for %s", claim.Subject)
		}
		facets, err := schemaext.NewFacets(&prefixSetting{Text: strings.TrimSpace(claim.Text)})
		if err != nil {
			return sqlext.Result{}, err
		}
		result.Values = append(result.Values, sqlext.Value{Facets: facets})
	}
	return result, nil
}

func prefixOwners(requests *[]sqlext.Request) sqlext.Set {
	return must.Must(sqlext.NewSet(prefixExtension(requests)))
}

func prefixExtension(requests *[]sqlext.Request) sqlext.Extension {
	return sqlext.Extension{
		Owner: "example.org/prefix", Targets: []string{"postgres"}, Facets: []schemaext.Kind{prefixKind},
		Patterns: []sqlext.Pattern{{Name: "tagged", Site: sqlext.TablePrefix, Words: []string{"TAGGED"}}},
		Parser:   prefixReader{requests: requests},
	}
}

func TestParse_TablePrefixesRetainTheirTableIdentity(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	document := `CREATE TAGGED ('TABLE', nested(1)) /* prefix */ TABLE IF NOT EXISTS "left"."cache" (id int PRIMARY KEY);
CREATE TAGGED second TABLE "right"."cache" (name text);
CREATE TABLE ordinary (id int);`
	parsed, err := parser.NewParser(document, parser.WithDialect("postgres"), parser.WithSQL(prefixOwners(&requests))).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Statements, qt.HasLen, 3)
	c.Assert(requests, qt.HasLen, 1)
	c.Assert(requests[0].Claims, qt.HasLen, 2)
	c.Assert(requests[0].Claims[0].Subject, qt.Equals, `"left"."cache"`)
	c.Assert(requests[0].Claims[0].Text, qt.Equals, `TAGGED ('TABLE', nested(1)) /* prefix */ `)
	c.Assert(requests[0].Claims[0].Offset, qt.Equals, 7)
	c.Assert(requests[0].Claims[1].Subject, qt.Equals, `"right"."cache"`)
	first := parsed.Statements[0].(*ast.CreateTableNode)
	second := parsed.Statements[1].(*ast.CreateTableNode)
	ordinary := parsed.Statements[2].(*ast.CreateTableNode)
	c.Assert(first.IfNotExists, qt.IsTrue)
	c.Assert(first.Columns, qt.HasLen, 1)
	c.Assert(first.Columns[0].Name, qt.Equals, "id")
	c.Assert(second.Columns[0].Name, qt.Equals, "name")
	firstSetting, _, err := first.Facets.Get(prefixKind)
	c.Assert(err, qt.IsNil)
	c.Assert(firstSetting, qt.DeepEquals, &prefixSetting{Text: `TAGGED ('TABLE', nested(1)) /* prefix */`})
	secondSetting, _, err := second.Facets.Get(prefixKind)
	c.Assert(err, qt.IsNil)
	c.Assert(secondSetting, qt.DeepEquals, &prefixSetting{Text: "TAGGED second"})
	c.Assert(ordinary.Facets.Len(), qt.Equals, 0)
}

func TestParse_TablePrefixStopsAtAnotherOwner(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	var claims []sqlext.Claim
	owners := must.Must(sqlext.NewSet(prefixExtension(&requests), sqlext.Extension{
		Owner: "example.org/other", Targets: []string{"postgres"}, Facets: []schemaext.Kind{"example.org/other/setting"},
		Patterns: []sqlext.Pattern{{Name: "marked", Site: sqlext.TablePrefix, Words: []string{"MARKED"}}},
		Parser:   claimRecorder{claims: &claims},
	}))
	parsed, err := parser.NewParser("CREATE TAGGED first MARKED second TABLE t (id int);", parser.WithDialect("postgres"), parser.WithSQL(owners)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(requests[0].Claims[0].Text, qt.Equals, "TAGGED first")
	c.Assert(claims, qt.DeepEquals, []sqlext.Claim{{Pattern: "marked", Site: sqlext.TablePrefix, Subject: "t", Text: "MARKED second ", Offset: 20}})
	setting, _, err := parsed.Statements[0].(*ast.CreateTableNode).Facets.Get(prefixKind)
	c.Assert(err, qt.IsNil)
	c.Assert(setting, qt.DeepEquals, &prefixSetting{Text: "TAGGED first"})
}

func TestParse_TablePrefixFailureReturnsNoStatements(t *testing.T) {
	for _, test := range []struct{ name, sql, want string }{
		{name: "no table", sql: "CREATE TAGGED option;", want: `.*expected TABLE after a table prefix.*`},
		{name: "wrong object", sql: "CREATE TAGGED VIEW v AS SELECT 1;", want: `.*expected TABLE after a table prefix.*`},
		{name: "malformed table", sql: "CREATE TAGGED TABLE t (", want: `(?s).*expected.*`},
		{name: "duplicate facet", sql: "CREATE TAGGED first TAGGED second TABLE t (id int);", want: `(?s).*duplicate.*`},
		{name: "later owner refusal", sql: "CREATE TAGGED first TABLE a (id int); CREATE TAGGED REFUSED TABLE b (id int);", want: `(?s).*prefix refused for b.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var requests []sqlext.Request
			parsed, err := parser.NewParser(test.sql, parser.WithDialect("postgres"), parser.WithSQL(prefixOwners(&requests))).Parse()
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

func TestParse_TablePrefixRequiresASelectedOwner(t *testing.T) {
	for _, target := range []string{"postgres", "mysql"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := parser.NewParser("CREATE TAGGED TABLE t (id int);", parser.WithDialect(target)).Parse()
			c.Assert(err, qt.ErrorMatches, `.*unsupported CREATE target.*TAGGED.*`)
			c.Assert(parsed, qt.IsNil)
		})
	}
}

func TestParse_TablePrefixDoesNotSelectAnotherTarget(t *testing.T) {
	c := qt.New(t)
	var requests []sqlext.Request
	parsed, err := parser.NewParser("CREATE TAGGED TABLE t (id int);", parser.WithDialect("mysql"), parser.WithSQL(prefixOwners(&requests))).Parse()
	c.Assert(err, qt.ErrorMatches, `.*unsupported CREATE target.*TAGGED.*`)
	c.Assert(parsed, qt.IsNil)
	c.Assert(requests, qt.HasLen, 0)
}
