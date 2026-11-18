package lint_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// accountsCode is the starting state of version 2: accounts holds id and
// code, generated or a column that stores what clients wrote.
func accountsCode(generated bool) []lintext.BaselineColumn {
	return []lintext.BaselineColumn{
		{Version: 2, Table: "accounts", Name: "id"},
		{Version: 2, Table: "accounts", Name: "code", Generated: generated},
	}
}

const (
	dropCode         = "ALTER TABLE accounts DROP COLUMN code;\n"
	addCodeGenerated = "ALTER TABLE accounts ADD COLUMN code text GENERATED ALWAYS AS (id::text) STORED;\n"
	rebuildInOne     = "ALTER TABLE accounts DROP COLUMN code, ALTER COLUMN id TYPE bigint, " +
		"ADD COLUMN code text NULL GENERATED ALWAYS AS (id::text) STORED;\n"
)

// regeneratedColumnRows are files that drop the column code of accounts, over
// a starting state. Only a column the state shows generated, added back as
// generated, loses no value a client wrote; every other row is a drop. %s in
// a site is the rule.
var regeneratedColumnRows = []struct {
	name     string
	second   string
	baseline []lintext.BaselineColumn
	want     []string
}{
	{
		name:     "a generated column added back as generated in a later statement",
		second:   dropCode + "ALTER TABLE accounts ALTER COLUMN id TYPE bigint;\n" + addCodeGenerated,
		baseline: accountsCode(true),
	},
	{
		name:     "a generated column added back as generated in the same statement",
		second:   rebuildInOne,
		baseline: accountsCode(true),
	},
	{
		name:     "a stored column added back as generated in a later statement",
		second:   dropCode + addCodeGenerated,
		baseline: accountsCode(false),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a stored column added back as generated in the same statement",
		second:   rebuildInOne,
		baseline: accountsCode(false),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a generated column not added back",
		second:   dropCode,
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a stored column added back as a plain column",
		second:   dropCode + "ALTER TABLE accounts ADD COLUMN code text;\n",
		baseline: accountsCode(false),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a generated column added back as an identity column",
		second:   dropCode + "ALTER TABLE accounts ADD COLUMN code int GENERATED ALWAYS AS IDENTITY;\n",
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a generated column added as generated to another table",
		second:   dropCode + "ALTER TABLE others ADD COLUMN code text GENERATED ALWAYS AS (id::text) STORED;\n",
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:     "a generated column the file changes before the drop",
		second:   "ALTER TABLE accounts RENAME COLUMN id TO code;\n" + dropCode + addCodeGenerated,
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:2:%s"},
	},
	{
		name: "a generated column after a read-only guard",
		second: "SELECT ('refusing to rebuild code' || string_agg(dependent, ', '))::integer FROM (\n" +
			"SELECT pg_describe_object(d.classid, d.objid, d.objsubid) AS dependent FROM pg_depend d\n" +
			"WHERE d.refobjid = 'accounts'::regclass AND d.objid = ANY (ARRAY[to_regclass('accounts_code')]::oid[])) dependents\n" +
			"HAVING count(*) > 0;\n" + dropCode + addCodeGenerated,
		baseline: accountsCode(true),
	},
	{
		name:     "a generated column after a SELECT that calls another function",
		second:   "SELECT change_accounts();\n" + dropCode + addCodeGenerated,
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:2:%s"},
	},
	{
		name:     "a generated column after a read-only call in another schema",
		second:   "SELECT evil.count(id) FROM accounts;\n" + dropCode + addCodeGenerated,
		baseline: accountsCode(true),
		want:     []string{"0000000002_change.up.sql:2:%s"},
	},
	{
		name:     "a generated column after a read-only call in pg_catalog",
		second:   "SELECT pg_catalog.count(id) FROM pg_catalog.pg_class;\n" + dropCode + addCodeGenerated,
		baseline: accountsCode(true),
	},
	{
		name:   "no starting state",
		second: dropCode + addCodeGenerated,
		want:   []string{"0000000002_change.up.sql:1:%s"},
	},
}

// TestDropColumnOfARegeneratedColumn_IsNotADrop pins the data rule, DS102,
// and the deployed-code rule, BC104, on each row of the native surface. A
// stored column turned into a generated one is reported by both: its values
// are gone, and a deployed client that writes it fails, since the server
// refuses a write to a generated column. A run with no starting state cannot
// tell the two apart, so it reports the drop.
func TestDropColumnOfARegeneratedColumn_IsNotADrop(t *testing.T) {
	for _, rule := range []string{"DS102", "BC104"} {
		for _, row := range regeneratedColumnRows {
			t.Run(rule+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				want := make([]string, 0, len(row.want))
				for _, site := range row.want {
					want = append(want, fmt.Sprintf(site, rule))
				}

				got := ruleSites(c, "postgres", row.second, rule, row.baseline...)

				c.Assert(got, qt.DeepEquals, want)
			})
		}
	}
}

// compatColumnDropSites lints second as the compatibility surface does and
// keeps the sites of DS102, which that surface prints as DS103.
func compatColumnDropSites(c *qt.C, second string, baseline []lintext.BaselineColumn) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_base.up.sql":   "CREATE TABLE accounts (id int);\n",
		"0000000002_change.up.sql": second,
	}), lint.Options{Dialect: "postgres", Baseline: baseline, Compatibility: lint.CompatibilityProfileAtlas})
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(findingSites(findings), func(site string) bool {
		return !strings.HasSuffix(site, ":DS102")
	})
}

// The compatibility surface counts an add in the drop's own statement only:
// atlas v1.3.0 reads a drop and an add of one column in one ALTER TABLE as a
// change of the column, and reports the drop where the add is a statement of
// its own.
func TestDropColumnOfARegeneratedColumn_CompatibilityCountsOneStatement(t *testing.T) {
	tests := []struct {
		name   string
		second string
		want   []string
	}{
		{name: "added back in the same statement", second: rebuildInOne, want: make([]string, 0)},
		{
			name:   "added back in a later statement",
			second: dropCode + addCodeGenerated,
			want:   []string{"0000000002_change.up.sql:1:DS102"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := compatColumnDropSites(c, test.second, accountsCode(true))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
