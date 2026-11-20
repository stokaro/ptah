package atlascodes_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/internal/atlascodes"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/providerpublished"
	"ptah.run/migration/lint"
)

// dialectFindingCodes lints a one-version migration directory under a dialect
// and returns the identifiers reported, sorted. Unlike findingCodes it gates
// dialect-specific rules, which is what an Atlas row about a MySQL check needs.
func dialectFindingCodes(tb testing.TB, dialect, up, down string, baseline ...lintext.BaselineColumn) []string {
	c := qt.New(tb)

	fsys := fstest.MapFS{
		"0000000001_change.up.sql":   &fstest.MapFile{Data: []byte(up)},
		"0000000001_change.down.sql": &fstest.MapFile{Data: []byte(down)},
	}
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: dialect, Baseline: baseline})
	c.Assert(err, qt.IsNil)

	var codes []string
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// atlasRuleList returns the Ptah rules one Atlas check's row names, sorted.
func atlasRuleList(tb testing.TB, code string) []string {
	c := qt.New(tb)

	for _, check := range atlascodes.Checks() {
		if check.Code == code {
			rules := slices.Clone(check.PtahRules)
			slices.Sort(rules)
			return rules
		}
	}
	c.Fatalf("the Atlas analyzer list carries no check %s", code)
	return nil
}

// statusColumn and flagsColumn are the schema state version 1 starts from in
// the member-list rows above: one column, spelled the way the dev-database read
// reports a MySQL type, with its member list.
// Both columns are NOT NULL already, so the NOT NULL a row's MODIFY carries
// restates what is there and DD103 stays out of the row's finding set.
func statusColumn(columnType string) []lintext.BaselineColumn {
	return []lintext.BaselineColumn{{Version: 1, Table: "orders", Name: "status", ColumnType: columnType, NotNull: true}}
}

func flagsColumn(columnType string) []lintext.BaselineColumn {
	return []lintext.BaselineColumn{{Version: 1, Table: "orders", Name: "flags", ColumnType: columnType, NotNull: true}}
}

// memberLiterals spells n distinct members, for the rows that exercise a
// storage boundary rather than a named value.
func memberLiterals(n int) string {
	values := make([]string, 0, n)
	for i := range n {
		values = append(values, fmt.Sprintf("'m%03d'", i))
	}
	return strings.Join(values, ",")
}

// TestAtlasRowsNameTheRulesThatFire drives the statement an Atlas check
// describes and compares what came back against the rules that check's row
// claims.
//
// Validate only asks that a named rule is registered somewhere, which a row
// naming the wrong registered rule satisfies -- MY110 named DS106, whose scan
// matches the PostgreSQL DROP VALUE and DELETE FROM pg_enum spellings and never
// sees the MySQL MODIFY COLUMN that restates the member list. The row read as
// coverage and the code produced none of it.
//
// These are the rows whose mapping is a claim about which rules fire on a
// specific statement rather than an identity between codes. The rest of the
// analyzer table is still a reading of the Atlas documentation joined to
// registered rules; this test does not extend to it, and does not pretend to.
func TestAtlasRowsNameTheRulesThatFire(t *testing.T) {
	rows := []struct {
		name      string
		atlasCode string
		dialect   string
		up        string
		down      string
		// baseline is the state version 1 starts from, for the rows whose
		// rule compares the statement with what the column is now. A row
		// without one is judged on the statement text alone.
		baseline []lintext.BaselineColumn
	}{
		// The eight member-list rows each carry the column's current list,
		// because the finding is a comparison and the statement holds only
		// one side of it. Each row's fixture is the change its Atlas check
		// describes and nothing else, so the finding set is exactly the
		// rule: the generic DS103 and MY101 are subsumed by it.
		{
			name:      "MY110, an enum value removed by restating the member list",
			atlasCode: "MY110",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid') NOT NULL;\n",
			down:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid','refunded') NOT NULL;\n",
			baseline:  statusColumn("enum('new','paid','refunded')"),
		},
		{
			name:      "MY111, enum values reordered",
			atlasCode: "MY111",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN status ENUM('paid','new','refunded');\n",
			down:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid','refunded');\n",
			baseline:  statusColumn("enum('new','paid','refunded')"),
		},
		{
			name:      "MY112, an enum value inserted before the end",
			atlasCode: "MY112",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN status ENUM('new','held','paid','refunded');\n",
			down:      "ALTER TABLE orders MODIFY COLUMN status ENUM('new','paid','refunded');\n",
			baseline:  statusColumn("enum('new','paid','refunded')"),
		},
		{
			name:      "MY113, an enum grown past 255 values",
			atlasCode: "MY113",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN status ENUM(" + memberLiterals(256) + ");\n",
			down:      "ALTER TABLE orders MODIFY COLUMN status ENUM(" + memberLiterals(255) + ");\n",
			baseline:  statusColumn("enum(" + memberLiterals(255) + ")"),
		},
		{
			name:      "MY120, a set value removed",
			atlasCode: "MY120",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN flags SET('a','b');\n",
			down:      "ALTER TABLE orders MODIFY COLUMN flags SET('a','b','c');\n",
			baseline:  flagsColumn("set('a','b','c')"),
		},
		{
			name:      "MY121, set values reordered",
			atlasCode: "MY121",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN flags SET('a','c','b');\n",
			down:      "ALTER TABLE orders MODIFY COLUMN flags SET('a','b','c');\n",
			baseline:  flagsColumn("set('a','b','c')"),
		},
		{
			name:      "MY122, a set value inserted before the end",
			atlasCode: "MY122",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN flags SET('a','x','b','c');\n",
			down:      "ALTER TABLE orders MODIFY COLUMN flags SET('a','b','c');\n",
			baseline:  flagsColumn("set('a','b','c')"),
		},
		{
			name:      "MY123, a set grown across a storage boundary",
			atlasCode: "MY123",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN flags SET(" + memberLiterals(9) + ");\n",
			down:      "ALTER TABLE orders MODIFY COLUMN flags SET(" + memberLiterals(8) + ");\n",
			baseline:  flagsColumn("set(" + memberLiterals(8) + ")"),
		},
		{
			name:      "MY130, a column type change",
			atlasCode: "MY130",
			dialect:   "mysql",
			up:        "ALTER TABLE orders MODIFY COLUMN total BIGINT NOT NULL;\n",
			down:      "ALTER TABLE orders MODIFY COLUMN total INT NOT NULL;\n",
			baseline:  []lintext.BaselineColumn{{Version: 1, Table: "orders", Name: "total", ColumnType: "int", TableCharset: "utf8mb4", NotNull: true}},
		},
		{
			name:      "MY133, a primary key dropped without a replacement",
			atlasCode: "MY133",
			dialect:   "mysql",
			up:        "ALTER TABLE orders DROP PRIMARY KEY;\n",
			down:      "ALTER TABLE orders ADD PRIMARY KEY (id);\n",
		},
		{
			name:      "MF101, a unique index over an existing table",
			atlasCode: "MF101",
			dialect:   "mysql",
			up:        "CREATE UNIQUE INDEX orders_email ON orders (email);\n",
			down:      "DROP INDEX orders_email ON orders;\n",
		},
		{
			name:      "MF102, an index dropped and rebuilt as unique",
			atlasCode: "MF102",
			dialect:   "mysql",
			up:        "ALTER TABLE orders DROP INDEX orders_email, ADD UNIQUE INDEX orders_email (email);\n",
			down:      "ALTER TABLE orders DROP INDEX orders_email, ADD INDEX orders_email (email);\n",
		},
		{
			name:      "PG301, a column type change PostgreSQL rewrites for",
			atlasCode: "PG301",
			dialect:   "postgres",
			up:        "ALTER TABLE orders ALTER COLUMN total TYPE bigint;\n",
			down:      "ALTER TABLE orders ALTER COLUMN total TYPE integer;\n",
			baseline:  []lintext.BaselineColumn{{Version: 1, Table: "orders", Name: "total", ColumnType: "integer"}},
		},
		{
			name:      "PG304, a primary key over a nullable column",
			atlasCode: "PG304",
			dialect:   "postgres",
			up:        "ALTER TABLE orders ADD PRIMARY KEY (id);\n",
			down:      "ALTER TABLE orders DROP CONSTRAINT orders_pkey;\n",
			baseline:  []lintext.BaselineColumn{{Version: 1, Table: "orders", Name: "id", ColumnType: "integer"}},
		},
		{
			name:      "MY136, a table character set change",
			atlasCode: "MY136",
			dialect:   "mysql",
			up:        "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4;\n",
			down:      "ALTER TABLE orders CONVERT TO CHARACTER SET latin1;\n",
			baseline: []lintext.BaselineColumn{{
				Version: 1, Table: "orders", Name: "note", ColumnType: "varchar(10)", Charset: "latin1", TableCharset: "latin1",
			}},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(dialectFindingCodes(t, row.dialect, row.up, row.down, row.baseline...),
				qt.DeepEquals, atlasRuleList(t, row.atlasCode),
				qt.Commentf("the %s row names rules the statement it describes does not produce", row.atlasCode))
		})
	}
}

// TestAtlasCatalogNamesEveryRowThatIsNotCovered pins the non-covered rows by
// code, so that one cannot appear or disappear without a deliberate edit here.
//
// Asserting that the absent and partial sets are EMPTY and that the covered
// count sits above a floor can be true while missing the defect: the catalog
// agrees with itself while it is nineteen checks behind the page it was built
// from, and a floor on how many rows are covered rises as happily from a wrong
// claim as from a right one (stokaro/ptah#2972). The
// set that decides completeness is compared against the reviewed reference in
// [atlascodes.CompareReference], which Validate runs; this test pins
// what the catalog SAYS about each code it holds.
func TestAtlasCatalogNamesEveryRowThatIsNotCovered(t *testing.T) {
	c := qt.New(t)

	byStatus := make(map[atlascodes.Status][]string)
	for _, check := range atlascodes.Checks() {
		byStatus[check.Status] = append(byStatus[check.Status], check.Code)
		c.Assert(check.Status == atlascodes.StatusCovered || check.Note != "", qt.IsTrue,
			qt.Commentf("%s is %s and gives no reason", check.Code, check.Status))
	}

	// No check is measured absent: MY142, the one that was, reads the server
	// version the run plans against.
	c.Assert(byStatus[atlascodes.StatusAbsent], qt.HasLen, 0)
	// PG108 needs the migration or the dev database to show the parent
	// partitioned; MY148 needs the dev database to prove the copy. Each note says
	// which input is missing.
	c.Assert(byStatus[atlascodes.StatusPartial], qt.DeepEquals, []string{"MY148", "PG108"})
	// The two account-bound rows, and no others. A third waiver would be a way
	// to make the count green without implementing anything.
	c.Assert(byStatus[atlascodes.StatusWaived], qt.DeepEquals, []string{"OW101", "OW102"})
}

// TestAtlasCatalogMatchesTheReviewedReference is the check the comparison was
// missing: the catalog's code set against the committed snapshot, both ways.
func TestAtlasCatalogMatchesTheReviewedReference(t *testing.T) {
	c := qt.New(t)

	drift := atlascodes.CompareReference()

	c.Assert(drift.Empty(), qt.IsTrue, qt.Commentf("%s", drift.Error()))
}

// TestAtlasReference_Parses keeps the embedded snapshot readable, since
// [atlascodes.Reference] panics on a file it cannot parse rather than
// answering "no checks" for one it failed to read.
func TestAtlasReference_Parses(t *testing.T) {
	c := qt.New(t)

	reference := atlascodes.Reference()

	c.Assert(len(reference) > 70, qt.IsTrue,
		qt.Commentf("the reviewed reference holds only %d checks", len(reference)))
	codes := make(map[string]int, len(reference))
	for _, entry := range reference {
		codes[entry.Code]++
		c.Assert(entry.Meaning, qt.Not(qt.Equals), "")
	}
	for code, count := range codes {
		c.Assert(count, qt.Equals, 1, qt.Commentf("%s appears %d times", code, count))
	}
}

// registeredMigrationNames is what migration lint would report a finding
// under, the recorded provider rules included.
func registeredMigrationNames() []string {
	runtime := builtintest.Runtime()
	rules := must.Must(lint.Rules(must.Must(providerpublished.LintRuntime(runtime, runtime))))
	names := make([]string, 0, len(rules))
	for _, rule := range rules {
		names = append(names, rule.Name)
	}
	return names
}

// TestValidate_AcceptsTheShippedTable runs the gate over the rules the
// documented runtime registers: every identity and every check row names a
// registered rule, and each says the same of the other.
func TestValidate_AcceptsTheShippedTable(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlascodes.Validate(registeredMigrationNames()), qt.HasLen, 0)
}

// TestValidate_FailurePath drives the disagreements the gate exists for. A
// gate that only ever ran against a consistent table would report success
// without having established anything.
func TestValidate_FailurePath(t *testing.T) {
	registered := registeredMigrationNames()
	rows := []struct {
		name       string
		registered []string
		want       string
	}{
		{
			name:       "an identity names a rule no linter registers",
			registered: slices.DeleteFunc(slices.Clone(registered), func(name string) bool { return name == "destructive/drop-table" }),
			want:       "rule destructive/drop-table reports Atlas DS102 but no linter registers it",
		},
		{
			name:       "a check row claims a rule no linter registers",
			registered: slices.DeleteFunc(slices.Clone(registered), func(name string) bool { return name == "destructive/drop-object" }),
			want:       "Atlas check DS101 claims coverage by Ptah rule destructive/drop-object, which no linter registers",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(atlascodes.Validate(row.registered), qt.Contains, row.want)
		})
	}
}

// TestIdentities_EveryRuleOfEveryDialectIsFoundByItsCode reads each identity
// forward and backward: the Atlas code a rule reports on a dialect names the
// rule again.
func TestIdentities_EveryRuleOfEveryDialectIsFoundByItsCode(t *testing.T) {
	c := qt.New(t)

	var wrong []string
	for _, identity := range atlascodes.Identities() {
		dialect := ""
		if len(identity.Dialects) > 0 {
			dialect = identity.Dialects[0]
		}
		got := atlascodes.RuleFor(identity.Name, dialect)
		if got.Code != identity.Code || !slices.Contains(atlascodes.NamesFor(identity.Code), identity.Name) {
			wrong = append(wrong, identity.Name)
		}
	}
	c.Assert(wrong, qt.HasLen, 0)
}
