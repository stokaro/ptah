package sqllint_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
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

// ruleCodePattern is the shape of a rule identifier: a family prefix, a
// three-digit number, and the optional trailing `P` the repository convention
// puts on a rule of ours inside a family Atlas also uses.
//
// The suffix is not decoration in this pattern. `internal/lintcatalog` refuses
// a Ptah rule in an Atlas-owned family that is spelled without it, so `PG112P`
// is the only spelling such a rule may have -- and a scan blind to that
// spelling would walk past exactly the identifiers the convention mandates,
// leaving an emitted finding documented nowhere.
var ruleCodePattern = regexp.MustCompile(`^[A-Z]+[0-9]{3}P?$`)

// declaredRuleCodes reads the Go source in dir for every constant whose value
// looks like a rule identifier.
//
// The SQL linter has no registry to enumerate -- two of its identifiers are
// produced by the parse path, before any rule object exists -- so the source is
// the only place the full set is written down. Reading it here is what stops a
// new identifier from being emitted by a binary and documented nowhere.
//
// It takes the directory rather than assuming the package's own so the scan
// itself can be driven over a fixture, which is how the identifier shape it
// accepts is pinned rather than asserted.
func declaredRuleCodes(tb testing.TB, dir string) []string {
	c := qt.New(tb)

	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)

	fileSet := token.NewFileSet()
	var codes []string
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fileSet, filepath.Join(dir, name), nil, 0)
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

	c.Assert(declaredRuleCodes(t, "."), qt.DeepEquals, catalogued)
}

// TestDeclaredRuleCodesReadsEveryConventionalSpelling drives the scan over a
// fixture per identifier shape the convention allows, because a scan that
// silently skips a shape reports the same empty disagreement as a package with
// nothing to report. The suffixed row is the one that matters: a rule of ours
// inside an Atlas-owned family may only be spelled with the trailing `P`, so a
// scan that does not read that spelling cannot see the rules the convention
// will produce.
func TestDeclaredRuleCodesReadsEveryConventionalSpelling(t *testing.T) {
	rows := []struct {
		name  string
		code  string
		found []string
	}{
		{name: "Ptah rule inside an Atlas family", code: "PG112P", found: []string{"PG112P"}},
		{name: "rule inside a family of ours", code: "SQL003", found: []string{"SQL003"}},
		{name: "Atlas check under the Atlas identifier", code: "PG101", found: []string{"PG101"}},
		{name: "lowercase is not an identifier", code: "pg112p", found: nil},
		{name: "two digits is not an identifier", code: "PG12", found: nil},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			dir := t.TempDir()
			source := "package fixture\n\nconst RuleFixture = " + strconv.Quote(row.code) + "\n"
			c.Assert(os.WriteFile(filepath.Join(dir, "rules.go"), []byte(source), 0o600), qt.IsNil)

			c.Assert(declaredRuleCodes(t, dir), qt.DeepEquals, row.found)
		})
	}
}

// TestCatalogRowsMatchTheEmittedFindings drives the linter once per identifier
// and compares the finding it produces against the catalog row. The catalog is
// a declaration; without this it would be a claim about the code rather than a
// reading of it.
//
// It also asserts that the rows cover the catalog exactly. The source scan
// above only proves an identifier is written down somewhere in the package: an
// unused constant satisfies it, so a catalog entry plus a dead constant would
// put a rule on the documentation page that no LintSource path can produce.
// Requiring an emission fixture per entry is what makes a phantom rule fail
// instead of publish.
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

	driven := make([]string, 0, len(rows))
	for _, row := range rows {
		driven = append(driven, row.code)
	}
	slices.Sort(driven)

	catalogued := sqllint.CatalogIDs()
	slices.Sort(catalogued)

	c := qt.New(t)
	c.Assert(slices.Compact(driven), qt.DeepEquals, catalogued,
		qt.Commentf("every catalogued identifier needs a fixture that makes the linter emit it"))

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
