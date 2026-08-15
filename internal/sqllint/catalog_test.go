package sqllint_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/sqllint"
)

// ruleCodePattern is the shape of a rule identifier: a family prefix and a
// three-digit number.
var ruleCodePattern = regexp.MustCompile(`^[A-Z]+[0-9]{3}$`)

// declaredRuleCodes reads this package's own source for every constant whose
// value looks like a rule identifier.
//
// The SQL linter has no registry to enumerate -- two of its identifiers are
// produced by the parse path, before any rule object exists -- so the source is
// the only place the full set is written down. Reading it here is what stops a
// new identifier from being emitted by a binary and documented nowhere.
func declaredRuleCodes(c *qt.C) []string {
	entries, err := os.ReadDir(".")
	c.Assert(err, qt.IsNil)

	fileSet := token.NewFileSet()
	var codes []string
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fileSet, name, nil, 0)
		c.Assert(parseErr, qt.IsNil)
		scanned++
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, unquoteErr := strconv.Unquote(literal.Value)
			if unquoteErr == nil && ruleCodePattern.MatchString(text) {
				codes = append(codes, text)
			}
			return true
		})
	}
	// A scan that read no files would report an empty set and agree with
	// nothing, which is the failure mode this whole test exists to prevent.
	c.Assert(scanned > 0, qt.IsTrue)

	slices.Sort(codes)
	return slices.Compact(codes)
}

// TestCatalogCoversEveryDeclaredRuleCode fails when a rule identifier is
// declared in this package and left out of the catalog the documentation reads.
func TestCatalogCoversEveryDeclaredRuleCode(t *testing.T) {
	c := qt.New(t)

	catalogued := sqllint.CatalogIDs()
	slices.Sort(catalogued)

	c.Assert(declaredRuleCodes(c), qt.DeepEquals, catalogued)
}

// TestCatalogRowsMatchTheEmittedFindings drives the linter once per identifier
// and compares the finding it produces against the catalog row. The catalog is
// a declaration; without this it would be a claim about the code rather than a
// reading of it.
func TestCatalogRowsMatchTheEmittedFindings(t *testing.T) {
	rows := []struct {
		name    string
		source  sqllint.Source
		options sqllint.Options
		code    string
	}{
		{
			name:    "parse error",
			source:  sqllint.Source{Name: "broken.sql", SQL: "CREATE TABLE ;"},
			options: sqllint.Options{Dialect: platform.Postgres},
			code:    "SQL001",
		},
		{
			name:    "statement the linter does not model",
			source:  sqllint.Source{Name: "query.sql", SQL: "SELECT 1;"},
			options: sqllint.Options{Dialect: platform.Postgres},
			code:    "SQL002",
		},
		{
			name:    "table without a primary key",
			source:  sqllint.Source{Name: "schema.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
			options: sqllint.Options{Dialect: platform.Postgres},
			code:    "DDL001",
		},
		{
			name:   "capability the target lacks",
			source: sqllint.Source{Name: "index.sql", SQL: "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);"},
			options: sqllint.Options{
				Dialect:      platform.CockroachDB,
				Capabilities: capability.CockroachDB23(),
			},
			code: "CAP001",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(row.source, row.options)
			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, row.code)
			c.Assert(findings[0].Title, qt.Equals, sqllint.CatalogTitle(row.code))
			c.Assert(findings[0].Severity, qt.Equals, sqllint.CatalogSeverity(row.code))
		})
	}
}
