package lint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// scanRuleCodes lints one up statement for PostgreSQL, under the online mode
// when online is set, and returns the codes of the two scan rules and the
// online mode's in the order reported.
func scanRuleCodes(c *qt.C, sql string, online bool) []string {
	c.Helper()
	findings, err := lint.LintFS(
		fixture(map[string]string{"0000000001_x.up.sql": sql + "\n", "0000000001_x.down.sql": "-- restore\n"}),
		lint.Options{Dialect: "postgres", RequireOnline: online},
	)
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, finding := range findings {
		if slices.Contains([]string{"PG305", "PG306", "ON101"}, finding.Rule) {
			codes = append(codes, finding.Rule)
		}
	}
	return codes
}

// TestLintFS_PostgresScanRulesSkipTheFormTheyRecommend holds PG305 and PG306
// to their own advice: a CHECK or a foreign key added NOT VALID reads no
// existing row, and the VALIDATE CONSTRAINT that completes it takes SHARE
// UPDATE EXCLUSIVE, so neither is reported (stokaro/ptah#3502). The online mode
// proves the same statements online from the same predicate.
func TestLintFS_PostgresScanRulesSkipTheFormTheyRecommend(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"check", "ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0) NOT VALID;"},
		{"unnamed check", "ALTER TABLE t ADD CHECK (id > 0) NOT VALID;"},
		{"check with another attribute after it", "ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0) NOT VALID NO INHERIT;"},
		{"foreign key", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) NOT VALID;"},
		{"foreign key with an action", "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) ON DELETE CASCADE NOT VALID;"},
		{"unnamed foreign key", "ALTER TABLE t ADD FOREIGN KEY (p_id) REFERENCES p (id) NOT VALID;"},
		{"two constraints, each NOT VALID", "ALTER TABLE t ADD CONSTRAINT a CHECK (id > 0) NOT VALID, ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id) NOT VALID;"},
		{"the validation that completes it", "ALTER TABLE t VALIDATE CONSTRAINT ck;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(scanRuleCodes(c, test.sql, false), qt.HasLen, 0)
			c.Assert(scanRuleCodes(c, test.sql, true), qt.HasLen, 0)
		})
	}
}

// TestLintFS_PostgresScanRulesReportAClauseThatValidates keeps the rules
// reporting what they exist for. NOT VALID answers for its own clause only,
// and only outside parentheses: CHECK (NOT valid) tests a boolean column named
// valid and reads every row. The online mode reports each of these too.
func TestLintFS_PostgresScanRulesReportAClauseThatValidates(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		plain  []string
		online []string
	}{
		{
			name:   "check",
			sql:    "ALTER TABLE t ADD CONSTRAINT ck CHECK (id > 0);",
			plain:  []string{"PG305"},
			online: []string{"ON101", "PG305"},
		},
		{
			name:   "foreign key",
			sql:    "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id);",
			plain:  []string{"PG306"},
			online: []string{"ON101", "PG306"},
		},
		{
			name:   "a check that validates beside one added NOT VALID",
			sql:    "ALTER TABLE t ADD CONSTRAINT a CHECK (id > 0), ADD CONSTRAINT b CHECK (id < 9) NOT VALID;",
			plain:  []string{"PG305"},
			online: []string{"ON101", "PG305"},
		},
		{
			name:   "a foreign key that validates beside a check added NOT VALID",
			sql:    "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id), ADD CONSTRAINT b CHECK (id < 9) NOT VALID;",
			plain:  []string{"PG306"},
			online: []string{"ON101", "PG306"},
		},
		{
			name:   "NOT VALID inside the check is its expression",
			sql:    "ALTER TABLE t ADD CONSTRAINT ck CHECK (NOT valid);",
			plain:  []string{"PG305"},
			online: []string{"ON101", "PG305"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(scanRuleCodes(c, test.sql, false), qt.DeepEquals, test.plain)
			c.Assert(scanRuleCodes(c, test.sql, true), qt.DeepEquals, test.online)
		})
	}
}

// TestLintFS_PostgresOnlineGenerationIsLintClean lints the file `ptah
// migrations generate` writes with diff.online_alter for a new CHECK, the
// reproduction in stokaro/ptah#3502. Without the NOT VALID reading, PG305
// reports its first statement.
func TestLintFS_PostgresOnlineGenerationIsLintClean(t *testing.T) {
	c := qt.New(t)
	up := "-- +ptah no_transaction\n" +
		"-- Migration generated from schema differences\n" +
		"-- Generated on: 2026-09-22T22:23:47Z\n" +
		"-- Direction: UP\n" +
		"\n" +
		"-- ALTER statements: --\n" +
		"ALTER TABLE \"accounts\" ADD CONSTRAINT \"accounts_plan_known\" CHECK (plan IN ('monthly', 'annual', 'trial')) NOT VALID;\n" +
		"ALTER TABLE \"accounts\" VALIDATE CONSTRAINT \"accounts_plan_known\";\n"

	findings, err := lint.LintFS(
		fixture(map[string]string{"1790115827_plan_known.up.sql": up, "1790115827_plan_known.down.sql": "-- restore\n"}),
		lint.Options{Dialect: "postgres"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}
