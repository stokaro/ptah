package lint_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
)

// regeneratedColumnRows are files that drop the column code of accounts and
// add a column called code back. A generated column added back holds no value
// of its own and keeps the name, so the drop deletes nothing a reader had; any
// other add, or an add to another table, leaves the drop a drop.
var regeneratedColumnRows = []struct {
	name   string
	second string
	want   []string
}{
	{
		name: "added back as generated in a later statement",
		second: "ALTER TABLE accounts DROP COLUMN code;\nALTER TABLE accounts ALTER COLUMN id TYPE bigint;\n" +
			"ALTER TABLE accounts ADD COLUMN code text GENERATED ALWAYS AS (id::text) STORED;\n",
	},
	{
		name: "added back as generated in the same statement",
		second: "ALTER TABLE accounts DROP COLUMN code, ALTER COLUMN id TYPE bigint, " +
			"ADD COLUMN code text NULL GENERATED ALWAYS AS (id::text) STORED;\n",
	},
	{
		name:   "added back as a plain column",
		second: "ALTER TABLE accounts DROP COLUMN code;\nALTER TABLE accounts ADD COLUMN code text;\n",
		want:   []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name:   "added back as an identity column",
		second: "ALTER TABLE accounts DROP COLUMN code;\nALTER TABLE accounts ADD COLUMN code int GENERATED ALWAYS AS IDENTITY;\n",
		want:   []string{"0000000002_change.up.sql:1:%s"},
	},
	{
		name: "added as generated to another table",
		second: "ALTER TABLE accounts DROP COLUMN code;\n" +
			"ALTER TABLE others ADD COLUMN code text GENERATED ALWAYS AS (id::text) STORED;\n",
		want: []string{"0000000002_change.up.sql:1:%s"},
	},
}

// TestDropColumnOfARegeneratedColumn_IsNotADrop pins the data rule, DS102,
// and the deployed-code rule, BC104, on each row. Neither reports a column the
// file adds back as a generated column, which is how a plan rebuilds one
// around a type change of a column it reads.
func TestDropColumnOfARegeneratedColumn_IsNotADrop(t *testing.T) {
	for _, rule := range []string{"DS102", "BC104"} {
		for _, row := range regeneratedColumnRows {
			t.Run(rule+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)
				want := make([]string, 0, len(row.want))
				for _, site := range row.want {
					want = append(want, fmt.Sprintf(site, rule))
				}

				got := ruleSites(c, "postgres", row.second, rule)

				c.Assert(got, qt.DeepEquals, want)
			})
		}
	}
}
