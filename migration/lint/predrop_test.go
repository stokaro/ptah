package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// ifExistsCase is a directory whose second version holds the statements under
// test, and the starting state the dev database reports for that version.
type ifExistsCase struct {
	name    string
	dialect string
	second  string
	// baseline is the state before the second version, empty for a run
	// without a dev database.
	baseline []lintext.BaselineColumn
}

// usersAt is the column users holds before version 2, in schema; "" for a
// reader that names no schema.
func usersAt(schema string) []lintext.BaselineColumn {
	return []lintext.BaselineColumn{{Version: 2, Schema: schema, Table: "users", Name: "id"}}
}

// tableDropSites lints the directory and keeps the findings of the rules a
// table drop meets: the data it deletes and the name it retires.
func tableDropSites(c *qt.C, test ifExistsCase) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_users.up.sql":  "CREATE TABLE users (id int);\n",
		"0000000002_change.up.sql": test.second,
	}), lint.Options{Dialect: test.dialect, Baseline: test.baseline})
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(findingSites(findings), func(site string) bool {
		return !strings.HasSuffix(site, ":DS101") && !strings.HasSuffix(site, ":BC103")
	})
}

// A DROP TABLE IF EXISTS of a table the starting state shows is absent drops
// nothing and retires no name, so neither DS101 nor BC103 reports it. The
// community binary compares the schema a file starts from with the one it ends
// with and reports nothing there either.
func TestIfExistsDropOfAnAbsentTable_IsNotADrop(t *testing.T) {
	tests := []ifExistsCase{
		{name: "one absent table", dialect: "sqlite", second: "DROP TABLE IF EXISTS missing;\n", baseline: usersAt("")},
		{name: "two absent tables", dialect: "sqlite", second: "DROP TABLE IF EXISTS missing, gone;\n", baseline: usersAt("")},
		{
			name: "qualified by a schema the state holds", dialect: "postgres",
			second: "DROP TABLE IF EXISTS public.missing;\n", baseline: usersAt("public"),
		},
		{
			name: "quoted, in another case", dialect: "sqlite",
			second: "DROP TABLE IF EXISTS \"Missing\";\n", baseline: usersAt(""),
		},
		{
			name: "after a statement whose words show its effect", dialect: "sqlite",
			second: "CREATE TABLE other (id int);\nDROP TABLE IF EXISTS missing;\n", baseline: usersAt(""),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tableDropSites(c, test), qt.HasLen, 0)
		})
	}
}

// Each row is an IF EXISTS drop that may drop a table, so it is reported as
// any drop is: the table is in the starting state, the run read no state, an
// earlier statement of the file may have given the name a table, or the state
// cannot say whether the table's schema holds it.
func TestIfExistsDropThatMayDropATable_IsReported(t *testing.T) {
	tests := []struct {
		ifExistsCase
		want []string
	}{
		{
			ifExistsCase: ifExistsCase{name: "the table is in the starting state", dialect: "sqlite",
				second: "DROP TABLE IF EXISTS users;\n", baseline: usersAt("")},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "no starting state", dialect: "sqlite",
				second: "DROP TABLE IF EXISTS missing;\n"},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "an earlier statement renames a table to the name", dialect: "sqlite",
				second: "ALTER TABLE users RENAME TO missing;\nDROP TABLE IF EXISTS missing;\n", baseline: usersAt("")},
			want: []string{"0000000002_change.up.sql:2:BC103", "0000000002_change.up.sql:2:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "one absent table and one present", dialect: "sqlite",
				second: "DROP TABLE IF EXISTS missing, users;\n", baseline: usersAt("")},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "qualified by a schema the state does not hold", dialect: "postgres",
				second: "DROP TABLE IF EXISTS other.missing;\n", baseline: usersAt("public")},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "qualified, against a state that names no schema", dialect: "postgres",
				second: "DROP TABLE IF EXISTS other.missing;\n", baseline: usersAt("")},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "no IF EXISTS", dialect: "sqlite",
				second: "DROP TABLE missing;\n", baseline: usersAt("")},
			want: []string{"0000000002_change.up.sql:1:BC103", "0000000002_change.up.sql:1:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "a DO block may have renamed a table to the name", dialect: "postgres",
				second: "DO $$ BEGIN ALTER TABLE users RENAME TO x; END $$;\nDROP TABLE IF EXISTS x;\n", baseline: usersAt("public")},
			want: []string{"0000000002_change.up.sql:2:BC103", "0000000002_change.up.sql:2:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "a prepared statement may have renamed a table to the name", dialect: "mysql",
				second:   "SET @s = 'RENAME TABLE users TO x';\nPREPARE p FROM @s;\nEXECUTE p;\nDROP TABLE IF EXISTS x;\n",
				baseline: usersAt("app")},
			want: []string{"0000000002_change.up.sql:4:BC103", "0000000002_change.up.sql:4:DS101"},
		},
		{
			ifExistsCase: ifExistsCase{name: "the search path moves to a schema the state does not hold", dialect: "postgres",
				second: "SET search_path TO other;\nDROP TABLE IF EXISTS missing;\n", baseline: usersAt("public")},
			want: []string{"0000000002_change.up.sql:2:BC103", "0000000002_change.up.sql:2:DS101"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := tableDropSites(c, test.ifExistsCase)
			slices.Sort(got)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// The change count reads the same predicates the rules do. The community binary
// counts what the replay changes on the dev database, so an IF EXISTS drop of an
// absent table and an IF NOT EXISTS create of a present one count nothing
// there, and a statement that may change a table counts one change per table.
func TestIfExistsStatementThatChangesNothing_CountsNoChange(t *testing.T) {
	tests := []struct {
		ifExistsCase
		want []changeProjection
	}{
		{
			ifExistsCase: ifExistsCase{name: "one absent table", dialect: "sqlite",
				second: "DROP TABLE IF EXISTS missing;\n", baseline: usersAt("")},
			want: make([]changeProjection, 0),
		},
		{
			ifExistsCase: ifExistsCase{name: "one absent table and one present", dialect: "postgres",
				second: "DROP TABLE IF EXISTS missing, users;\n", baseline: usersAt("public")},
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users"}},
		},
		{
			ifExistsCase: ifExistsCase{name: "no starting state", dialect: "sqlite",
				second: "DROP TABLE IF EXISTS missing;\n"},
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "missing"}},
		},
		{
			ifExistsCase: ifExistsCase{name: "an earlier statement renames a table to the name", dialect: "sqlite",
				second: "ALTER TABLE users RENAME TO missing;\nDROP TABLE IF EXISTS missing;\n", baseline: usersAt("")},
			want: []changeProjection{
				{Kind: lint.SchemaChangeDrop, Object: "users"},
				{Kind: lint.SchemaChangeAdd, Object: "missing"},
				{Kind: lint.SchemaChangeDrop, Object: "missing"},
			},
		},
		{
			ifExistsCase: ifExistsCase{name: "IF NOT EXISTS create of a present table", dialect: "sqlite",
				second: "CREATE TABLE IF NOT EXISTS users (id int);\nDROP TABLE users;\n", baseline: usersAt("")},
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users"}},
		},
		{
			ifExistsCase: ifExistsCase{name: "IF NOT EXISTS create of a table the file dropped first", dialect: "sqlite",
				second: "DROP TABLE users;\nCREATE TABLE IF NOT EXISTS users (id int);\n", baseline: usersAt("")},
			want: []changeProjection{{Kind: lint.SchemaChangeDrop, Object: "users"}, {Kind: lint.SchemaChangeAdd, Object: "users"}},
		},
		{
			ifExistsCase: ifExistsCase{name: "IF NOT EXISTS create of an absent table", dialect: "sqlite",
				second: "CREATE TABLE IF NOT EXISTS staging (id int);\n", baseline: usersAt("")},
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "staging"}},
		},
		{
			ifExistsCase: ifExistsCase{name: "IF NOT EXISTS create without a starting state", dialect: "sqlite",
				second: "CREATE TABLE IF NOT EXISTS users (id int);\n"},
			want: []changeProjection{{Kind: lint.SchemaChangeAdd, Object: "users"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql":  "CREATE TABLE users (id int);\n",
				"0000000002_change.up.sql": test.second,
			}), lint.Options{Dialect: test.dialect, Baseline: test.baseline})
			c.Assert(err, qt.IsNil)
			c.Assert(projectChanges(fileByName(c, analysis, "0000000002_change.up.sql").Changes), qt.DeepEquals, test.want)
		})
	}
}

// A run without a starting state names the drop rules as thinner only for a
// drop whose answer the state decides. A drop of a table the file created, or
// one after a statement whose effect its words do not show, is reported
// whatever the state says, so the run misses nothing there.
func TestIfExistsDrop_NamesItsInputOnlyWhereTheStateDecides(t *testing.T) {
	tests := []struct {
		name   string
		second string
		want   []string
	}{
		{
			name: "an absent table", second: "DROP TABLE IF EXISTS missing;\n",
			want: []string{"BC103", "DS101"},
		},
		{name: "a table the file created", second: "CREATE TABLE t (a int);\nDROP TABLE IF EXISTS t;\n", want: nil},
		{name: "after a DO block", second: "DO $$ BEGIN NULL; END $$;\nDROP TABLE IF EXISTS missing;\n", want: nil},
		{
			name: "a CREATE TABLE IF NOT EXISTS of a table the file drops", second: "CREATE TABLE IF NOT EXISTS t (a int);\nDROP TABLE t;\n",
			want: []string{"BC103", "DS101"},
		},
		{name: "a CREATE TABLE IF NOT EXISTS nothing after names", second: "CREATE TABLE IF NOT EXISTS t (a int);\n", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_users.up.sql":  "CREATE TABLE users (id int);\n",
				"0000000002_change.up.sql": test.second,
			}), lint.Options{Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			var unmet []string
			for _, entry := range analysis.UnmetInputs() {
				unmet = append(unmet, entry.Rule)
			}
			c.Assert(dropRulesIn(unmet), qt.DeepEquals, test.want)
		})
	}
}

// dropRulesIn keeps the table drop rules among rules, sorted, and is nil when
// none is there.
func dropRulesIn(rules []string) []string {
	var kept []string
	for _, rule := range rules {
		kept = append(kept, slices.DeleteFunc([]string{rule}, func(r string) bool { return r != "BC103" && r != "DS101" })...)
	}
	slices.Sort(kept)
	return kept
}

// A CREATE TABLE IF NOT EXISTS may find the table already there, holding rows,
// so the rules that exempt what a migration created report the table it names
// as they report any other, unless the starting state shows the table is new.
// Each row is the exemption a plain CREATE TABLE earns, which
// TestCreateTable_ExemptsTheTableItCreates holds. Each column of the table
// below is one way the state leaves the table's newness open: no state, a
// state that holds the table, and an earlier rename that may give the name a
// table the state does not show.
func TestCreateTableIfNotExists_ExemptsNothingTheStateDoesNotShowIsNew(t *testing.T) {
	cases := []struct {
		name     string
		before   string
		baseline []lintext.BaselineColumn
		line     string
	}{
		{name: "no starting state", line: "2"},
		{name: "a state that holds the table", baseline: accountsAndUsersAt2(), line: "2"},
		{name: "an earlier rename into the name", before: "ALTER TABLE accounts RENAME TO users;\n", baseline: accountsAt2(), line: "3"},
	}
	for _, test := range createdTableRows {
		for _, state := range cases {
			t.Run(test.name+", "+state.name, func(t *testing.T) {
				c := qt.New(t)
				got := ruleSites(c, "postgres", state.before+"CREATE TABLE IF NOT EXISTS users (id int, email text);\n"+test.then, test.rule, state.baseline...)
				// The rename row also reports its own rename under BC101, so
				// the site under test is asserted present rather than alone.
				c.Assert(got, qt.Contains, "0000000002_change.up.sql:"+state.line+":"+test.rule)
			})
		}
	}
}

// The control for the test above: a plain CREATE TABLE brings the table into
// being, and so does a CREATE TABLE IF NOT EXISTS of a table the starting state
// does not hold, which is what the community binary sees when it replays the
// file. None of the rows reports.
func TestCreateTable_ExemptsTheTableItCreates(t *testing.T) {
	cases := []struct {
		name     string
		create   string
		baseline []lintext.BaselineColumn
	}{
		{name: "a plain CREATE TABLE", create: "CREATE TABLE users (id int, email text);\n"},
		{name: "IF NOT EXISTS, absent from the state", create: "CREATE TABLE IF NOT EXISTS users (id int, email text);\n", baseline: accountsAt2()},
	}
	for _, test := range createdTableRows {
		for _, create := range cases {
			t.Run(test.name+", "+create.name, func(t *testing.T) {
				c := qt.New(t)
				got := ruleSites(c, "postgres", create.create+test.then, test.rule, create.baseline...)
				c.Assert(got, qt.HasLen, 0)
			})
		}
	}
}

// Every rule that exempts the file's own tables reads the starting state for
// a CREATE TABLE IF NOT EXISTS, so a run without one names each of them as
// thinner. Without that request the version is not read at all on a run with
// a dev database, and the rule reports a table the state shows is new.
func TestCreateTableIfNotExists_NamesTheRulesThatReadTheState(t *testing.T) {
	for _, test := range createdTableRows {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0000000001_base.up.sql":   "CREATE TABLE accounts (id int);\n",
				"0000000002_change.up.sql": "CREATE TABLE IF NOT EXISTS users (id int, email text);\n" + test.then,
			}), lint.Options{Dialect: "postgres"})
			c.Assert(err, qt.IsNil)
			var unmet []string
			for _, entry := range analysis.UnmetInputs() {
				unmet = append(unmet, entry.Rule)
			}
			c.Assert(unmet, qt.Contains, test.rule)
		})
	}
}

// accountsAt2 is the state before version 2 of the [ruleSites] directory.
func accountsAt2() []lintext.BaselineColumn {
	return []lintext.BaselineColumn{{Version: 2, Schema: "public", Table: "accounts", Name: "id"}}
}

// accountsAndUsersAt2 is [accountsAt2] with a users table already there.
func accountsAndUsersAt2() []lintext.BaselineColumn {
	return append(accountsAt2(),
		lintext.BaselineColumn{Version: 2, Schema: "public", Table: "users", Name: "id"},
		lintext.BaselineColumn{Version: 2, Schema: "public", Table: "users", Name: "email"},
	)
}

// createdTableRows are the statements that follow a CREATE TABLE of users, and
// the rule that exempts each when the file created the table.
var createdTableRows = []struct {
	name string
	then string
	rule string
}{
	{name: "table dropped, data", then: "DROP TABLE users;\n", rule: "DS101"},
	{name: "table dropped, deployed code", then: "DROP TABLE users;\n", rule: "BC103"},
	{name: "column dropped, data", then: "ALTER TABLE users DROP COLUMN email;\n", rule: "DS102"},
	{name: "column dropped, deployed code", then: "ALTER TABLE users DROP COLUMN email;\n", rule: "BC104"},
	{name: "table renamed", then: "ALTER TABLE users RENAME TO people;\n", rule: "BC101"},
	{name: "NOT NULL column added", then: "ALTER TABLE users ADD COLUMN n int NOT NULL;\n", rule: "DD101"},
	{name: "unique index built", then: "CREATE UNIQUE INDEX users_email ON users (email);\n", rule: "MF101"},
}

// ruleSites lints a directory whose second version is second, against the
// starting state baseline, and keeps the sites of rule.
func ruleSites(c *qt.C, dialect, second, rule string, baseline ...lintext.BaselineColumn) []string {
	c.Helper()
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_base.up.sql":   "CREATE TABLE accounts (id int);\n",
		"0000000002_change.up.sql": second,
	}), lint.Options{Dialect: dialect, Baseline: baseline})
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(findingSites(findings), func(site string) bool {
		return !strings.HasSuffix(site, ":"+rule)
	})
}

// A MySQL CHANGE clause renames a column without the word RENAME, so a CHANGE
// of a column into a name the file added moves the old column's data there, and
// dropping it after loses that data.
func TestDropColumnAfterAChangeIntoTheAddedName_IsReported(t *testing.T) {
	c := qt.New(t)
	got := ruleSites(c, "mysql",
		"ALTER TABLE accounts ADD COLUMN nick text;\nALTER TABLE accounts CHANGE id nick int;\nALTER TABLE accounts DROP COLUMN nick;\n",
		"DS102")
	c.Assert(got, qt.DeepEquals, []string{"0000000002_change.up.sql:3:DS102"})
}

// A drop of one absent table and one present one names only the present one,
// so the finding is about the table the statement may drop.
func TestIfExistsDropOfAnAbsentAndAPresentTable_NamesThePresentOne(t *testing.T) {
	c := qt.New(t)
	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
		"0000000001_users.up.sql":  "CREATE TABLE users (id int);\n",
		"0000000002_change.up.sql": "DROP TABLE IF EXISTS missing, users;\n",
	}), lint.Options{Dialect: "sqlite", Baseline: usersAt("")})
	c.Assert(err, qt.IsNil)
	var messages []string
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(strings.Join(messages, "\n"), qt.Contains, "DS101: DROP TABLE permanently deletes table users ")
	c.Assert(strings.Join(messages, "\n"), qt.Not(qt.Contains), "missing")
}
