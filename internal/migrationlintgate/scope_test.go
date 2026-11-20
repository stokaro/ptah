package migrationlintgate_test

import (
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
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

// codesOutsideReportedRules returns the identifiers that do not belong to the
// family the gate reports. It is what separates "the gate returned destructive
// findings" from "the gate returned only destructive findings"; the first is
// true of the full lint pass as well.
func codesOutsideReportedRules(findings []lint.Finding) []string {
	var outside []string
	for _, code := range codesIn(findings) {
		if !slices.Contains(migrationlintgate.ReportedRules(), code) {
			outside = append(outside, code)
		}
	}
	return outside
}

// fullPass is what `ptah migrations lint` reports over the same directory: the
// whole registry, nothing disabled and nothing filtered.
func fullPass(tb testing.TB, fsys fstest.MapFS, dialect string) []lint.Finding {
	c := qt.New(tb)

	findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{Dialect: dialect})
	c.Assert(err, qt.IsNil)
	return findings
}

// gatePass is what `ptah migrations up` would refuse the apply over.
func gatePass(tb testing.TB, fsys fstest.MapFS, dialect string) []lint.Finding {
	c := qt.New(tb)

	findings, err := migrationlintgate.Analyze(c.Context(), builtintest.Runtime(), fsys, migrationlintgate.Pending{Versions: []int64{1}}, dialect, "")
	c.Assert(err, qt.IsNil)
	return findings
}

// TestAnalyze_ReportsOnlyTheDestructiveFamily drives one directory that trips
// rules in more than one family and compares the two passes.
//
// The foreign-key drop is the load-bearing statement. integrity/drop-foreign-key is an enabled rule
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

	c.Assert(full, qt.Contains, "destructive/drop-column")
	c.Assert(full, qt.Contains, "integrity/drop-foreign-key")

	c.Assert(codesIn(gated), qt.Contains, "destructive/drop-column")
	c.Assert(codesIn(gated), qt.Not(qt.Contains), "integrity/drop-foreign-key")
	c.Assert(codesOutsideReportedRules(gated), qt.HasLen, 0)
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
			code:    "file-form/empty-migration",
		},
		{
			name:    "BC, a rename that retires a deployed name",
			dialect: "sqlite",
			up:      "ALTER TABLE users RENAME TO accounts;\n",
			down:    "ALTER TABLE accounts RENAME TO users;\n",
			code:    "rollout/rename",
		},
		{
			name:    "PG, an index built without CONCURRENTLY",
			dialect: "postgres",
			up:      "CREATE INDEX idx_users_email ON users (email);\n",
			down:    "DROP INDEX CONCURRENTLY idx_users_email;\n",
			code:    "locking/create-index",
		},
		{
			name:    "MY, a foreign key added to a live table",
			dialect: "mysql",
			up:      "ALTER TABLE orders ADD CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id);\n",
			down:    "ALTER TABLE orders DROP FOREIGN KEY fk_orders_user;\n",
			code:    "locking/add-foreign-key",
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

// TestDisabledRules_AreRulesTheGateDoesNotBlockOn keeps the exported list -- the
// one the reference page counts -- apart from the rules the gate blocks on. A
// rule the gate both disabled and blocked on would never run.
func TestDisabledRules_AreRulesTheGateDoesNotBlockOn(t *testing.T) {
	c := qt.New(t)

	for _, name := range migrationlintgate.DisabledRules() {
		c.Assert(migrationlintgate.ReportedRules(), qt.Not(qt.Contains), name)
	}
	c.Assert(migrationlintgate.DisabledRules(), qt.Contains, "file-form/missing-down")
	c.Assert(migrationlintgate.DisabledRules(), qt.Contains, "rollout/rename")
}
