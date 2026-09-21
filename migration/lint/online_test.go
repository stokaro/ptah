package lint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// onlineRuleCodes lints one up statement under the online mode and returns the
// ON codes it reported.
func onlineRuleCodes(c *qt.C, dialect, sql string) []string {
	c.Helper()
	findings, err := lint.LintFS(
		fixture(map[string]string{"0000000001_x.up.sql": sql}),
		lint.Options{Dialect: dialect, RequireOnline: true},
	)
	c.Assert(err, qt.IsNil)
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		if len(finding.Rule) >= 2 && finding.Rule[:2] == lint.OnlineFamily {
			codes = append(codes, finding.Rule)
		}
	}
	return codes
}

// TestOnlineMode_PostgresProvesTheMeasuredSet is the allowlist working: each
// statement here is in the set measured to take no lock conflicting with reads
// and writes, so the mode reports nothing about it.
func TestOnlineMode_PostgresProvesTheMeasuredSet(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "concurrent index build", sql: "CREATE INDEX CONCURRENTLY idx ON users (email);"},
		{name: "concurrent index drop", sql: "DROP INDEX CONCURRENTLY idx;"},
		{name: "plain column addition", sql: "ALTER TABLE users ADD COLUMN nickname text;"},
		{name: "column addition with a default", sql: "ALTER TABLE users ADD COLUMN tier text DEFAULT 'free';"},
		{name: "column drop", sql: "ALTER TABLE users DROP COLUMN nickname;"},
		{name: "default set", sql: "ALTER TABLE users ALTER COLUMN tier SET DEFAULT 'free';"},
		{name: "not null dropped", sql: "ALTER TABLE users ALTER COLUMN tier DROP NOT NULL;"},
		{name: "constraint added without its scan", sql: "ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0) NOT VALID;"},
		{name: "the validation that completes it", sql: "ALTER TABLE users VALIDATE CONSTRAINT ck;"},
		{name: "a new table locks nothing that exists", sql: "CREATE TABLE t (id bigint PRIMARY KEY);"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.HasLen, 0)
		})
	}
}

// TestOnlineMode_PostgresReportsEverythingElse is the half that makes it a
// guarantee. The last row is the one that separates an allowlist from a
// blocklist: a statement no rule in this package describes is reported here
// because it was not proven, not because somebody anticipated it.
func TestOnlineMode_PostgresReportsEverythingElse(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "blocking index build", sql: "CREATE INDEX idx ON users (email);"},
		{name: "blocking index drop", sql: "DROP INDEX idx;"},
		{name: "not null added", sql: "ALTER TABLE users ALTER COLUMN tier SET NOT NULL;"},
		{name: "type change", sql: "ALTER TABLE users ALTER COLUMN id TYPE bigint;"},
		{name: "constraint added with its scan", sql: "ALTER TABLE users ADD CONSTRAINT ck CHECK (id > 0);"},
		{name: "primary key added", sql: "ALTER TABLE users ADD CONSTRAINT pk PRIMARY KEY (id) NOT VALID;"},
		{name: "column added with a constraint", sql: "ALTER TABLE users ADD COLUMN email text NOT NULL;"},
		{name: "a statement no rule here describes", sql: "CLUSTER users USING idx;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "postgres", test.sql), qt.DeepEquals, []string{"ON101"})
		})
	}
}

// TestOnlineMode_MySQLAsksTheServer holds the shorter and stronger rule: a
// statement is proven online when it asked the server to refuse what it cannot
// do online, and not otherwise.
func TestOnlineMode_MySQLAsksTheServer(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "the request the planner writes",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INPLACE, LOCK=NONE;",
			want: make([]string, 0),
		},
		{
			name: "an instant change needs no lock level beside it",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INSTANT;",
			want: make([]string, 0),
		},
		{
			name: "a new table locks nothing that exists",
			sql:  "CREATE TABLE t (id BIGINT PRIMARY KEY);",
			want: make([]string, 0),
		},
		{
			name: "the same change with no request at all",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64);",
			want: []string{"ON102"},
		},
		{
			name: "an algorithm without a lock level says nothing about writes",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=INPLACE;",
			want: []string{"ON102"},
		},
		{
			name: "a copy asked for by name",
			sql:  "ALTER TABLE users ADD COLUMN nickname VARCHAR(64), ALGORITHM=COPY;",
			want: []string{"ON102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(onlineRuleCodes(c, "mysql", test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestOnlineMode_StaysSilentWhenNobodyAskedForIt is the control the whole
// family needs. An allowlist left on by default would report most statements
// of most migrations, so a run that did not select the mode must see none of
// it.
func TestOnlineMode_StaysSilentWhenNobodyAskedForIt(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(
		fixture(map[string]string{"0000000001_x.up.sql": "CREATE INDEX idx ON users (email);"}),
		lint.Options{Dialect: "postgres"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(slices.ContainsFunc(findings, func(finding lint.Finding) bool {
		return len(finding.Rule) >= 2 && finding.Rule[:2] == lint.OnlineFamily
	}), qt.IsFalse)
}

// The mode is refused on an engine nothing here has measured. A run that
// proved nothing and reported nothing is the one answer this family must never
// give, so the refusal happens before any analysis.
func TestOnlineMode_RefusesAnUnmeasuredEngine(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "sqlite rebuilds the table for most changes", dialect: "sqlite"},
		{name: "cockroachdb is online by design and unmeasured here", dialect: "cockroachdb"},
		{name: "sqlserver answers differently by edition", dialect: "sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := lint.ValidateOnlineDialect(test.dialect)

			c.Assert(err, qt.ErrorMatches, `online mode covers postgres, mysql, mariadb; it is refused on ".*" .*`)
		})
	}
}

// The control: the three engines the mode covers are accepted.
func TestOnlineMode_AcceptsTheMeasuredEngines(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: "postgres"},
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lint.ValidateOnlineDialect(test.dialect), qt.IsNil)
		})
	}
}

// A policy that selects the mode refuses the apply on its findings without
// naming the family under `gate` as well: two ways to say one thing would
// leave a mode that reported and ran.
func TestOnlineMode_GateBlocksOnTheFamily(t *testing.T) {
	c := qt.New(t)
	cfg := &lint.Config{Dialect: "postgres", Online: lint.OnlineRequire}

	c.Assert(cfg.GateFamilies(), qt.DeepEquals, []string{"DS", lint.OnlineFamily})
}

// The control: a policy that did not select the mode leaves the gate where it
// was.
func TestOnlineMode_GateIsUnchangedWithoutTheMode(t *testing.T) {
	c := qt.New(t)
	cfg := &lint.Config{Dialect: "postgres"}

	c.Assert(cfg.GateFamilies(), qt.DeepEquals, []string{"DS"})
}
