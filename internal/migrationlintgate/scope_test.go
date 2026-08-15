package migrationlintgate_test

import (
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationlintgate"
	"go.5x5.cz/ptah/migration/lint"
)

// This file measures how much of the rule registry `ptah migrations up` runs,
// because the reference page tells an operator so and a sentence about scope is
// worth nothing unless something fails when the scope moves.
//
// The gate is deliberately narrower than `ptah migrations lint`: it turns four
// families off outright and drops every finding outside the destructive family
// even when the rule that produced it ran. An operator who read the rule tables
// as a list of what protects an apply would be reading a longer list than the
// gate consults, which is what these tests pin.

// directoryFor builds a one-version migration directory carrying a policy for
// the given dialect. The down file exists so the analysis is about the
// statements rather than about a missing rollback.
func directoryFor(dialect, up, down string) fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName:          {Data: []byte("dialect: " + dialect + "\n")},
		"0000000001_change.up.sql":   {Data: []byte(up)},
		"0000000001_change.down.sql": {Data: []byte(down)},
	}
}

// codesIn reduces findings to the sorted set of identifiers they report under.
func codesIn(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// codesOutsideReportedFamily returns the identifiers that do not belong to the
// family the gate reports. It is what separates "the gate returned destructive
// findings" from "the gate returned only destructive findings"; the first is
// true of the full lint pass as well.
func codesOutsideReportedFamily(findings []lint.Finding) []string {
	var outside []string
	for _, code := range codesIn(findings) {
		if !strings.HasPrefix(code, migrationlintgate.ReportedFamily) {
			outside = append(outside, code)
		}
	}
	return outside
}

// fullPass is what `ptah migrations lint` reports over the same directory: the
// whole registry, nothing disabled and nothing filtered.
func fullPass(tb testing.TB, fsys fstest.MapFS, dialect string) []lint.Finding {
	c := qt.New(tb)

	findings, err := lint.LintFS(fsys, lint.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)
	return findings
}

// gatePass is what `ptah migrations up` would refuse the apply over.
func gatePass(tb testing.TB, fsys fstest.MapFS, dialect string) []lint.Finding {
	c := qt.New(tb)

	findings, err := migrationlintgate.Analyze(fsys, []int64{1}, dialect, "")
	c.Assert(err, qt.IsNil)
	return findings
}

// TestAnalyze_ReportsOnlyTheDestructiveFamily drives one directory that trips
// rules in more than one family and compares the two passes.
//
// The foreign-key drop is the load-bearing statement. CD101 is an enabled rule
// -- its family is not one the gate turns off -- and it reports at error
// severity, so it survives the blocking filter and is dropped for one reason
// only: it is not in the family the gate reports. A fixture whose extra
// findings were advisory would prove nothing here, because the severity filter
// would have removed them anyway.
//
// The full pass is measured in the same test rather than assumed, so a fixture
// that stopped producing the wider findings could not make the narrowing look
// proven.
func TestAnalyze_ReportsOnlyTheDestructiveFamily(t *testing.T) {
	c := qt.New(t)

	fsys := directoryFor("mysql",
		"ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;\nALTER TABLE users DROP COLUMN legacy;\n",
		"ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id);\n"+
			"ALTER TABLE users ADD COLUMN legacy TEXT;\n")

	full := codesIn(fullPass(t, fsys, "mysql"))
	gated := gatePass(t, fsys, "mysql")

	c.Assert(full, qt.Contains, "DS102")
	c.Assert(full, qt.Contains, "CD101")

	c.Assert(codesIn(gated), qt.Contains, "DS102")
	c.Assert(codesIn(gated), qt.Not(qt.Contains), "CD101")
	c.Assert(codesOutsideReportedFamily(gated), qt.HasLen, 0)
}

// TestAnalyze_DropsEveryDisabledFamily walks one statement per family the gate
// turns off. Each row is a finding `ptah migrations lint` reports and the apply
// gate does not, which is the difference the reference page has to state.
//
// What it does not separate: whether the family's disable or the destructive
// filter removed the finding. Both narrowings run, and from outside [Analyze]
// they are indistinguishable -- a disabled rule and a rule whose finding is
// filtered both return nothing. The list of families is pinned separately
// below, which is the only place the disable half is observable.
func TestAnalyze_DropsEveryDisabledFamily(t *testing.T) {
	rows := []struct {
		name    string
		dialect string
		up      string
		down    string
		code    string
	}{
		{
			name:    "MF, a migration carrying no executable statements",
			dialect: "sqlite",
			up:      "-- the DDL never got written\n",
			down:    "-- and neither did its rollback\n",
			code:    "MF102",
		},
		{
			name:    "BC, a rename that retires a deployed name",
			dialect: "sqlite",
			up:      "ALTER TABLE users RENAME TO accounts;\n",
			down:    "ALTER TABLE accounts RENAME TO users;\n",
			code:    "BC101",
		},
		{
			name:    "PG, an index built without CONCURRENTLY",
			dialect: "postgres",
			up:      "CREATE INDEX idx_users_email ON users (email);\n",
			down:    "DROP INDEX CONCURRENTLY idx_users_email;\n",
			code:    "PG101",
		},
		{
			name:    "MY, a foreign key added to a live table",
			dialect: "mysql",
			up:      "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id);\n",
			down:    "ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;\n",
			code:    "MY131",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			fsys := directoryFor(row.dialect, row.up, row.down)

			c.Assert(codesIn(fullPass(t, fsys, row.dialect)), qt.Contains, row.code)
			c.Assert(codesIn(gatePass(t, fsys, row.dialect)), qt.Not(qt.Contains), row.code)
		})
	}
}

// TestDisabledFamilies_MatchesTheFamiliesTheRowsAboveDrive keeps the exported
// list -- the one the reference page renders -- equal to the families the rows
// above measured. A family added to or removed from the gate fails here rather
// than quietly changing a generated sentence nothing drove.
func TestDisabledFamilies_MatchesTheFamiliesTheRowsAboveDrive(t *testing.T) {
	c := qt.New(t)

	c.Assert(migrationlintgate.DisabledFamilies(), qt.DeepEquals, []string{"MF", "BC", "PG", "MY"})
}
